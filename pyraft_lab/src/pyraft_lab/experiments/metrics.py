"""Turning a finished run into the numbers, in the shape the specification asks for.

The post-hoc half of the metrics split (docs/architecture.md P4): everything here reads
a completed :class:`~pyraft_lab.experiments.runner.RunResult` and computes distributions
over it. ``observability/metrics.py`` is the other half - running counters a dashboard
reads while a run is still going. Neither is a substitute for the other: percentiles
need the whole sample, and a dashboard cannot wait for the end.

Every number is derived from what the run recorded - the event trace, the client
history, the network's own counters. Nothing is estimated, and where a number cannot be
computed from what happened it is reported as ``None`` rather than as zero. A zero that
means "no data" is a lie that survives into a graph.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from pyraft_lab import NodeId
from pyraft_lab.consistency import linearizability
from pyraft_lab.consistency.history import History, OpKind
from pyraft_lab.experiments.runner import RunResult
from pyraft_lab.observability.events import Event, EventKind

MS = 1000.0


def percentile(values: list[float], fraction: float) -> float | None:
    """The ``fraction`` percentile by nearest-rank, or ``None`` for no sample.

    Nearest-rank rather than interpolating: an interpolated p99 reports a latency no
    request actually experienced, which is exactly the wrong thing to hand somebody
    asking how bad the tail was.
    """
    if not values:
        return None
    ordered = sorted(values)
    rank = max(1, min(len(ordered), round(fraction * len(ordered) + 0.5)))
    return ordered[rank - 1]


def _distribution(values: list[float], *, scale: float = MS) -> dict[str, Any]:
    scaled = [v * scale for v in values]
    return {
        "count": len(scaled),
        "mean": (sum(scaled) / len(scaled)) if scaled else None,
        "p50": percentile(scaled, 0.50),
        "p95": percentile(scaled, 0.95),
        "p99": percentile(scaled, 0.99),
        "max": max(scaled, default=None),
    }


# --- leadership ----------------------------------------------------------------------


@dataclass(frozen=True)
class Reign:
    """One leader's time in office, as the trace recorded it."""

    term: int
    leader: NodeId
    start: float
    end: float

    def to_dict(self) -> dict[str, Any]:
        return {
            "term": self.term,
            "leader": self.leader,
            "start_ms": self.start * MS,
            "end_ms": self.end * MS,
        }


def leadership_timeline(events: list[Event], until: float) -> list[Reign]:
    """Who led, in which term, between when and when.

    A reign ends when the next one begins - not when the leader crashed. The trace
    cannot say when a node stopped believing it led, only when somebody else was
    elected, and inventing an earlier end would be inventing a gap nobody observed.
    """
    elections = [e for e in events if e.kind is EventKind.LEADER_ELECTED and e.node]
    reigns: list[Reign] = []

    for index, event in enumerate(elections):
        end = elections[index + 1].timestamp if index + 1 < len(elections) else until
        reigns.append(
            Reign(
                term=event.term or 0,
                leader=event.node or "",
                start=event.timestamp,
                end=max(end, event.timestamp),
            )
        )
    return reigns


def recovery_intervals(events: list[Event]) -> list[float]:
    """How long the cluster went without a leader after losing one.

    Measured from the moment the cluster lost the node that had most recently been
    elected - a crash, or a partition that cut it off - to the next election that
    produced a leader. A crash of a follower starts no interval: nothing was lost that
    had to be recovered.
    """
    intervals: list[float] = []
    leader: NodeId | None = None
    lost_at: float | None = None

    for event in events:
        if event.kind is EventKind.LEADER_ELECTED:
            if lost_at is not None:
                intervals.append(event.timestamp - lost_at)
                lost_at = None
            leader = event.node
        elif event.kind is EventKind.NODE_CRASHED and event.node == leader:
            lost_at = event.timestamp
        elif event.kind is EventKind.PARTITION_CREATED and leader is not None:
            # A partition does not always cost the cluster its leader; if it did not,
            # the next election simply never comes and this opens no interval that
            # closes. Recorded as a candidate, discarded if nothing follows.
            lost_at = lost_at if lost_at is not None else event.timestamp

    return intervals


def election_durations(events: list[Event]) -> list[float]:
    """Time from the first candidate of a term to that term's leader being elected."""
    started: dict[int, float] = {}
    durations: list[float] = []

    for event in events:
        if event.term is None:
            continue
        if event.kind is EventKind.ELECTION_STARTED:
            started.setdefault(event.term, event.timestamp)
        elif event.kind is EventKind.LEADER_ELECTED:
            begun = started.pop(event.term, None)
            if begun is not None:
                durations.append(event.timestamp - begun)
    return durations


def replication_lag(events: list[Event], nodes: list[NodeId]) -> dict[str, Any]:
    """How far behind the trailing replica ran, in entries.

    Replayed from the commit events rather than sampled: the lag is the spread between
    the furthest-ahead and furthest-behind node's commit index at each moment one of
    them moved, which is every moment the answer could have changed.
    """
    commit: dict[NodeId, int] = dict.fromkeys(nodes, 0)
    peak = 0
    samples: list[int] = []

    for event in events:
        if event.kind is not EventKind.LOG_COMMITTED or event.node is None:
            continue
        commit[event.node] = max(commit.get(event.node, 0), int(event.details.get("to", 0)))
        spread = max(commit.values()) - min(commit.values())
        peak = max(peak, spread)
        samples.append(spread)

    return {
        "max_entries": peak,
        "mean_entries": (sum(samples) / len(samples)) if samples else None,
        "final_entries": (max(commit.values()) - min(commit.values())) if commit else 0,
    }


# --- safety -----------------------------------------------------------------------------


def lost_writes(history: History, surviving: list[Any]) -> list[dict[str, Any]]:
    """Acknowledged writes that are missing from the cluster's surviving log.

    The invariant the whole project exists to defend: a write the client was *told*
    succeeded had committed and applied before that answer went out, so Raft guarantees
    it survives on a majority. If it is not in the longest surviving committed prefix,
    it is gone, and that is data loss in the only sense that matters.

    Writes the client never got an answer for are excluded - the cluster promised
    nothing about those, and losing one is not a broken promise.
    """
    committed = {json.dumps(command, sort_keys=True) for command in surviving}
    lost = []

    for op in history.operations:
        if op.kind is not OpKind.PUT or not op.completed or not op.ok:
            continue
        wire = json.dumps({"kind": "put", "key": op.key, "value": op.value}, sort_keys=True)
        if wire not in committed:
            lost.append({"key": op.key, "value": op.value, "completed_at": op.completed_at})
    return lost


def divergent_nodes(result: RunResult) -> list[dict[str, Any]]:
    """Nodes whose committed prefixes disagree at a shared index.

    Log Matching says this cannot happen; checking it anyway is what makes the run
    evidence rather than an assumption.
    """
    problems: list[dict[str, Any]] = []
    snapshots = [n for n in result.nodes if n.alive]

    for i, first in enumerate(snapshots):
        for second in snapshots[i + 1 :]:
            shared = min(len(first.committed), len(second.committed))
            for index in range(shared):
                if first.committed[index] != second.committed[index]:
                    problems.append(
                        {
                            "nodes": [first.node_id, second.node_id],
                            "index": index + 1,
                            "left": first.committed[index],
                            "right": second.committed[index],
                        }
                    )
                    break
    return problems


# --- the report ---------------------------------------------------------------------------


def report(result: RunResult, *, check_consistency: bool = True) -> dict[str, Any]:
    """The full result document for one run, in the specification's schema."""
    history = result.history
    duration = result.duration or result.scenario.duration

    completed = history.completed
    writes = [op for op in completed if op.kind is OpKind.PUT]
    reads = [op for op in completed if op.kind is OpKind.GET]

    redirects = sum(m.redirects for m in result.clients.values())
    timeouts = sum(m.timeouts for m in result.clients.values())
    lost = lost_writes(history, result.surviving_log)
    divergence = divergent_nodes(result)

    elections = election_durations(result.events)
    recoveries = recovery_intervals(result.events)
    reigns = leadership_timeline(result.events, duration)

    document: dict[str, Any] = {
        "scenario": result.scenario.name,
        "source": str(result.scenario.source) if result.scenario.source else None,
        "run_id": result.run_id,
        "seed": result.seed,
        "duration_sec": duration,
        "summary": {
            "total_requests": len(history),
            "successful": len(completed),
            "incomplete": len(history.incomplete),
            "failed_redirects": redirects,
            "timeouts": timeouts,
            "availability": (len(completed) / len(history)) if len(history) else 0.0,
            "throughput_per_sec": (len(completed) / duration) if duration else 0.0,
            "data_loss": bool(lost),
            "lost_writes": lost,
            "divergent_logs": divergence,
        },
        "timing": {
            "avg_commit_latency_ms": _distribution([op.duration or 0.0 for op in writes])["mean"],
            "p50_commit_latency_ms": percentile([(op.duration or 0.0) * MS for op in writes], 0.50),
            "p95_commit_latency_ms": percentile([(op.duration or 0.0) * MS for op in writes], 0.95),
            "p99_commit_latency_ms": percentile([(op.duration or 0.0) * MS for op in writes], 0.99),
            "commit_latency": _distribution([op.duration or 0.0 for op in writes]),
            "read_latency": _distribution([op.duration or 0.0 for op in reads]),
        },
        "leadership": {
            "total_elections": sum(
                1 for e in result.events if e.kind is EventKind.ELECTION_STARTED
            ),
            "leader_changes": len(reigns),
            "election_time_ms": _distribution(elections)["mean"],
            "election_time": _distribution(elections),
            "recovery_time_ms": max((r * MS for r in recoveries), default=None),
            "recovery_time": _distribution(recoveries),
            "final_leader": result.live.leader,
            "final_term": result.live.current_term,
            "leadership_timeline": [reign.to_dict() for reign in reigns],
        },
        "replication": replication_lag(result.events, [n.node_id for n in result.nodes]),
        "fault_impact": {
            "messages_sent": result.network.sent,
            "messages_delivered": result.network.delivered,
            "messages_dropped": result.network.dropped,
            "messages_delayed": result.network.delayed,
            "dropped_by_partition": result.network.dropped_partition,
            "dropped_by_loss": result.network.dropped_loss,
            "faults_applied": [fault.to_dict() for fault in result.faults],
        },
        "nodes": [snapshot.to_dict() for snapshot in result.nodes],
    }

    if check_consistency:
        outcome = linearizability.check(history, events=result.events)
        document["consistency"] = {
            **outcome.to_dict(),
            "explanation": outcome.explain(),
        }

    document["expectations"] = [
        item.to_dict() for item in evaluate_expectations(result.scenario.expected, document)
    ]
    document["passed"] = (
        all(item["met"] for item in document["expectations"] if item["checked"])
        and not document["summary"]["data_loss"]
    )
    return document


def write_report(document: dict[str, Any], path: str | Path) -> Path:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(document, indent=2, sort_keys=False), encoding="utf-8")
    return path


# --- expectations -----------------------------------------------------------------------


#: Where each ``expected:`` key lives in the report above. Anything not named here is
#: reported as unchecked rather than quietly ignored - a scenario that expects something
#: this build cannot measure should say so out loud.
EXPECTATION_PATHS = {
    "availability": ("summary", "availability"),
    "throughput_per_sec": ("summary", "throughput_per_sec"),
    "data_loss": ("summary", "data_loss"),
    "total_requests": ("summary", "total_requests"),
    "avg_commit_latency_ms": ("timing", "avg_commit_latency_ms"),
    "p50_commit_latency_ms": ("timing", "p50_commit_latency_ms"),
    "p95_commit_latency_ms": ("timing", "p95_commit_latency_ms"),
    "p99_commit_latency_ms": ("timing", "p99_commit_latency_ms"),
    "total_elections": ("leadership", "total_elections"),
    "leader_changes": ("leadership", "leader_changes"),
    "election_time_ms": ("leadership", "election_time_ms"),
    "recovery_time_ms": ("leadership", "recovery_time_ms"),
    "max_replication_lag": ("replication", "max_entries"),
    "linearizable": ("consistency", "linearizable"),
}

_COMPARISON = re.compile(r"^\s*(?P<op><=|>=|<|>|==|=)\s*(?P<value>-?\d+(?:\.\d+)?)\s*$")


@dataclass(frozen=True)
class ExpectationResult:
    key: str
    expected: Any
    actual: Any
    met: bool
    checked: bool
    note: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "key": self.key,
            "expected": self.expected,
            "actual": self.actual,
            "met": self.met,
            "checked": self.checked,
            "note": self.note,
        }


def evaluate_expectations(
    expected: dict[str, Any], document: dict[str, Any]
) -> list[ExpectationResult]:
    """Score a scenario's own ``expected:`` block against what the run produced.

    The specification writes these as ``<50`` and ``1.0`` and ``false``, so all three
    forms are read: a comparison string, an exact number, or a boolean.
    """
    results: list[ExpectationResult] = []

    for key, want in expected.items():
        path = EXPECTATION_PATHS.get(_canonical(key))
        if path is None:
            results.append(
                ExpectationResult(key, want, None, False, False, "this build does not measure it")
            )
            continue

        actual = document
        for part in path:
            actual = actual.get(part) if isinstance(actual, dict) else None

        if key.startswith("no_"):
            want = not _as_bool(want)

        if actual is None:
            results.append(
                ExpectationResult(key, want, None, False, False, "no measurement in this run")
            )
            continue

        met, note = _satisfied(want, actual)
        results.append(ExpectationResult(key, want, actual, met, True, note))

    return results


def _canonical(key: str) -> str:
    """``no_data_loss`` asks about ``data_loss``; the sense is flipped by the caller."""
    return key[3:] if key.startswith("no_") else key


def _as_bool(value: Any) -> bool:
    if isinstance(value, str):
        return value.strip().lower() in {"true", "yes", "1"}
    return bool(value)


def _satisfied(want: Any, actual: Any) -> tuple[bool, str]:
    if isinstance(want, bool):
        return _as_bool(actual) is want, ""

    if isinstance(want, str):
        match = _COMPARISON.match(want)
        if match is None:
            return str(actual) == want, ""
        threshold = float(match["value"])
        match match["op"]:
            case "<":
                return actual < threshold, ""
            case "<=":
                return actual <= threshold, ""
            case ">":
                return actual > threshold, ""
            case ">=":
                return actual >= threshold, ""
            case _:
                return actual == threshold, ""

    if isinstance(want, int | float):
        return abs(float(actual) - float(want)) < 1e-9, ""

    return False, f"cannot compare {want!r}"


def summarize(document: dict[str, Any]) -> str:
    """A few lines for a terminal - the run at a glance."""
    summary = document["summary"]
    leadership = document["leadership"]
    timing = document["timing"]

    lines = [
        f"{document['scenario']}  ({document['duration_sec']:.1f}s, run {document['run_id']})",
        f"  requests      {summary['total_requests']}"
        f"  served {summary['successful']}"
        f"  availability {summary['availability']:.3f}",
        f"  commit p50/p99  {_ms(timing['p50_commit_latency_ms'])} / "
        f"{_ms(timing['p99_commit_latency_ms'])}",
        f"  elections     {leadership['total_elections']}"
        f"  leader changes {leadership['leader_changes']}"
        f"  recovery {_ms(leadership['recovery_time_ms'])}",
        f"  replication   max lag {document['replication']['max_entries']} entries",
        f"  data loss     {'YES' if summary['data_loss'] else 'no'}",
    ]
    if "consistency" in document:
        lines.append(f"  linearizable  {document['consistency']['verdict']}")
    for item in document["expectations"]:
        if not item["checked"]:
            lines.append(f"  expected {item['key']}: not checked ({item['note']})")
        else:
            mark = "ok" if item["met"] else "FAILED"
            lines.append(f"  expected {item['key']} {item['expected']}: {mark} ({item['actual']})")
    return "\n".join(lines)


def _ms(value: float | None) -> str:
    return "n/a" if value is None else f"{value:.1f}ms"
