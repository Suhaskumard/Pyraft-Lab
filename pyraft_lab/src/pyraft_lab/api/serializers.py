"""Backend objects, shaped for the browser.

One rule, inherited from ``cli/main.py``: nothing here invents a number. Where the
front end would like a value this build cannot measure - per-node CPU, for instance,
which is meaningless when every node is a task in one process - the field is absent
rather than estimated, and the UI says so.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from pyraft_lab.cluster.manager import ClusterManager
from pyraft_lab.kv.commands import SESSION_CLIENT, SESSION_SERIAL
from pyraft_lab.observability.events import Event, EventKind
from pyraft_lab.raft.log import RaftLog
from pyraft_lab.raft.node import RaftNode

MS = 1000.0


def _ms(seconds: float | None) -> float | None:
    return None if seconds is None else seconds * MS


def _pct(fraction: float | None) -> float | None:
    return None if fraction is None else fraction * 100


def percentile(values: list[float], fraction: float) -> float | None:
    """Nearest-rank percentile. ``None`` for an empty series, never 0.0 - a latency
    nobody has measured is not a latency of zero."""
    if not values:
        return None
    ordered = sorted(values)
    rank = max(1, min(len(ordered), round(fraction * len(ordered))))
    return ordered[rank - 1]


# --- commands and log entries ---------------------------------------------------------


def describe_command(command: Any) -> str:
    """A log entry's command as one readable line."""
    if command is None:
        return "NO-OP (leader elected)"
    if not isinstance(command, dict):
        return str(command)
    kind = str(command.get("kind", "?")).upper()
    key = command.get("key")
    match command.get("kind"):
        case "put":
            return f"PUT {key}={command.get('value')!r}"
        case "delete":
            return f"DELETE {key}"
        case "cas":
            return f"CAS {key}: {command.get('expected')!r} -> {command.get('new')!r}"
        case "get":
            return f"GET {key}"
        case _:
            return kind


def command_fields(command: Any) -> dict[str, Any]:
    """The command's parts, broken out for a table. Always the same keys - a no-op
    entry has no key or value, and the row still has to have the columns."""
    if not isinstance(command, dict):
        return {"kind": None, "key": None, "value": None, "client": None, "serial": None}
    return {
        "kind": command.get("kind"),
        "key": command.get("key"),
        "value": command.get("value"),
        "client": command.get(SESSION_CLIENT),
        "serial": command.get(SESSION_SERIAL),
    }


def log_entries(node: RaftNode, *, limit: int | None = None) -> list[dict[str, Any]]:
    log: RaftLog = node.state.log
    start = log.base_index + 1
    indexes = range(start, log.last_index + 1)
    if limit is not None:
        indexes = range(max(start, log.last_index - limit + 1), log.last_index + 1)

    rows: list[dict[str, Any]] = []
    for index in indexes:
        entry = log.entry_at(index)
        if entry is None:
            continue
        rows.append(
            {
                "index": entry.index,
                "term": entry.term,
                "command": describe_command(entry.command),
                "committed": entry.index <= node.state.commit_index,
                "applied": entry.index <= node.state.last_applied,
                **command_fields(entry.command),
            }
        )
    return rows


def kv_records(node: RaftNode) -> list[dict[str, Any]]:
    """Every live key, with the log position that last wrote it.

    The version and the modifying index are folded out of the node's own applied log
    prefix rather than tracked by the store: ``KVStore`` is a dict plus an apply
    counter, and giving it per-key provenance would make the state machine carry
    bookkeeping no replica needs to agree on.
    """
    provenance: dict[str, dict[str, Any]] = {}
    log = node.state.log
    for index in range(log.base_index + 1, node.state.last_applied + 1):
        entry = log.entry_at(index)
        if entry is None or not isinstance(entry.command, dict):
            continue
        key = entry.command.get("key")
        if not isinstance(key, str) or entry.command.get("kind") not in ("put", "cas", "delete"):
            continue
        seen = provenance.get(key)
        provenance[key] = {
            "version": (seen["version"] + 1) if seen else 1,
            "modifiedTerm": entry.term,
            "modifiedIndex": entry.index,
        }

    snapshot = node.store.snapshot()
    return [
        {
            "key": key,
            "value": snapshot[key],
            **provenance.get(key, {"version": 1, "modifiedTerm": None, "modifiedIndex": None}),
        }
        for key in sorted(snapshot)
    ]


# --- nodes and the cluster ---------------------------------------------------------------


def node_state(manager: ClusterManager, node_id: str) -> dict[str, Any]:
    member = manager.nodes[node_id]
    node = member.node
    if node is None or not member.running:
        return {
            "id": node_id,
            "alive": False,
            "role": "down",
            "currentTerm": node.current_term if node else None,
            "votedFor": None,
            "leaderId": None,
            "commitIndex": node.state.commit_index if node else 0,
            "lastApplied": node.state.last_applied if node else 0,
            "logLength": node.state.log.last_index if node else 0,
            "storeKeys": len(node.store) if node else 0,
            "peers": list(node.peers) if node else [],
            "walPath": str(member.wal_path) if member.wal_path else None,
            "walBytes": None,
            "snapshotCount": 0,
        }

    wal_bytes = None
    if node.wal is not None:
        try:
            wal_bytes = node.wal.size
        except OSError:
            wal_bytes = None

    return {
        "id": node_id,
        "alive": True,
        "role": node.role.value,
        "currentTerm": node.current_term,
        "votedFor": node.state.voted_for,
        "leaderId": node.leader_id,
        "commitIndex": node.state.commit_index,
        "lastApplied": node.state.last_applied,
        "logLength": node.state.log.last_index,
        "logBaseIndex": node.state.log.base_index,
        "storeKeys": len(node.store),
        "peers": list(node.peers),
        "matchIndex": dict(node.state.match_index),
        "nextIndex": dict(node.state.next_index),
        "walPath": str(member.wal_path) if member.wal_path else None,
        "walBytes": wal_bytes,
        "snapshotCount": len(node.snapshots.metas()) if node.snapshots is not None else 0,
    }


def cluster_status(manager: ClusterManager) -> dict[str, Any]:
    """The header reading: health, term, leader, and the live counters behind them."""
    live = manager.status()
    stats = manager.network.stats

    leader_id = live["leader"]
    leader = manager.nodes.get(leader_id).node if leader_id in manager.nodes else None
    if leader is None or not manager.nodes[leader_id].running:
        leader = None

    latencies = manager.client.metrics.latencies if manager.client else []
    client_metrics = manager.client.metrics.as_dict() if manager.client else None

    return {
        "nodeCount": len(manager.nodes),
        "term": live["term"],
        "leader": leader_id,
        "status": live["status"],
        "partitioned": live["partitioned"],
        "partitionGroups": live["partition_groups"],
        "down": live["down"],
        "electionsStarted": live["elections_started"],
        "leadersElected": live["leaders_elected"],
        "termChanges": live["term_changes"],
        "appendEntriesSent": live["append_entries_sent"],
        "appendEntriesRejected": live["append_entries_rejected"],
        "commitIndex": leader.state.commit_index if leader else live["max_commit_index"],
        "appliedIndex": leader.state.last_applied if leader else 0,
        "logEntriesCount": leader.state.log.last_index if leader else 0,
        "electionTimeMs": _ms(live["last_election_seconds"]),
        "meanElectionTimeMs": _ms(live["mean_election_seconds"]),
        "p50LatencyMs": _ms(percentile(latencies, 0.50)),
        "p99LatencyMs": _ms(percentile(latencies, 0.99)),
        "availabilityPct": _pct(client_metrics["availability"]) if client_metrics else None,
        "client": client_metrics,
        "messagesSent": stats.sent,
        "messagesDelivered": stats.delivered,
        "droppedPackets": stats.dropped,
        "droppedLoss": stats.dropped_loss,
        "droppedPartition": stats.dropped_partition,
        "droppedUnknown": stats.dropped_unknown_dst,
        "delayed": stats.delayed,
        "eventCount": len(manager.tracer),
    }


def faults(manager: ClusterManager) -> dict[str, Any]:
    """What is currently wrong with the network, on purpose."""
    link = manager.network._default_link
    live = manager.status()
    return {
        "minLatencyMs": link.min_latency * MS,
        "maxLatencyMs": link.max_latency * MS,
        "lossProbability": link.loss_probability,
        "partitioned": live["partitioned"],
        "partitionGroups": live["partition_groups"],
        "downNodes": live["down"],
    }


# --- the trace ----------------------------------------------------------------------------

#: Trace events that describe one message crossing the wire, and the label the
#: consensus view draws them with. The other kinds are node-local facts, not traffic.
RPC_KINDS = {
    EventKind.VOTE_REQUESTED: "RequestVote",
    EventKind.VOTE_GRANTED: "RequestVoteResponse",
    EventKind.APPEND_ENTRIES_SENT: "AppendEntries",
    EventKind.APPEND_ENTRIES_REJECTED: "AppendEntriesResponse",
}


def event_row(event: Event) -> dict[str, Any]:
    return {
        "seq": event.seq,
        "timestamp": event.timestamp,
        "node": event.node,
        "term": event.term,
        "kind": str(event.kind),
        "details": event.details,
        "text": str(event),
    }


def rpc_row(event: Event) -> dict[str, Any] | None:
    """One wire-crossing event as a from/to message, or ``None`` if it is not one.

    The direction is read off the event's own details rather than guessed: a
    ``VOTE_GRANTED`` is emitted by the voter and names the candidate it answers, and an
    ``APPEND_ENTRIES_REJECTED`` is emitted by the follower and names the leader it
    rejected (docs/architecture.md P4), so both of those flow *from* ``event.node``.
    """
    label = RPC_KINDS.get(event.kind)
    if label is None:
        return None

    peer = (
        event.details.get("peer") or event.details.get("candidate") or event.details.get("leader")
    )
    return {
        "id": f"rpc-{event.seq}",
        "from": event.node,
        "to": peer,
        "type": label,
        "term": event.term,
        "timestamp": event.timestamp,
        "details": event.details,
    }


# --- persisted artefacts --------------------------------------------------------------------


def wal_report(path: Path, records: list[dict[str, Any]], recovery: Any) -> dict[str, Any]:
    return {
        "path": str(path),
        "exists": True,
        "nodeId": recovery.node_id,
        "recordsRead": recovery.records_read,
        "recordsValid": recovery.records_valid,
        "validBytes": recovery.valid_bytes,
        "truncatedTail": recovery.truncated_tail,
        "droppedRecords": recovery.dropped_records,
        "term": recovery.term,
        "votedFor": recovery.voted_for,
        "commitIndex": recovery.commit_index,
        "baseIndex": recovery.base_index,
        "lastIndex": recovery.last_index,
        "reportLines": recovery.report_lines(),
        "records": [
            {
                "position": position,
                "kind": record.get("kind"),
                "index": record.get("index"),
                "term": record.get("term"),
                "detail": " ".join(f"{k}={v}" for k, v in sorted(record.items()) if k != "kind"),
                # A record listed here verified its own checksum; the listing stops at
                # the first one that did not (see cli/main.py's _wal_records).
                "status": "VALID",
            }
            for position, record in enumerate(records, start=1)
        ],
    }


def snapshot_meta(meta: Any) -> dict[str, Any]:
    return {
        "snapshotId": meta.filename,
        "nodeId": meta.node_id or None,
        "lastIncludedIndex": meta.last_included_index,
        "lastIncludedTerm": meta.last_included_term,
        "keys": meta.keys,
        "createdAt": meta.created_at,
        "checksum": meta.checksum,
    }
