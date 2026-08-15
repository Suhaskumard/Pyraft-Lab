"""Live counters: what is true about the cluster *right now*, folded from the trace.

Deliberately not the same thing as ``experiments/metrics.py``, which does not exist yet
and which computes p50/p95/p99, replication lag and election counts *after* a run from
its finished trace. This module is the running total a dashboard can render at 10 Hz
without re-reading anything: every value here is maintained incrementally as events
arrive, and none of it requires the run to be over. See docs/architecture.md P4 for why
the split is worth keeping.

Everything is derived from the event stream alone. Nothing here reaches into a
``RaftNode`` to read its state - a metrics object that can see the cluster directly
would go on working when the trace is wrong, which is exactly when it should not.
"""

from __future__ import annotations

from statistics import mean
from typing import Any

from pyraft_lab import NodeId
from pyraft_lab.observability.events import Event, EventKind
from pyraft_lab.observability.tracer import Tracer


class LiveMetrics:
    """A running fold of a trace. Attach to a tracer, read whenever."""

    def __init__(self, tracer: Tracer | None = None) -> None:
        self.current_term = 0
        self.leader: NodeId | None = None
        self.leader_term = 0

        self.elections_started = 0
        self.leaders_elected = 0
        self.votes_requested = 0
        self.votes_granted = 0
        self.term_changes = 0

        self.append_entries_sent = 0
        self.append_entries_rejected = 0

        self.commit_index: dict[NodeId, int] = {}
        self.known_nodes: set[NodeId] = set()
        self.down: set[NodeId] = set()

        self.partitioned = False
        self.partitions_created = 0
        self.partition_groups: list[list[NodeId]] = []

        self.election_durations: list[float] = []
        # term -> when the first candidate in it started campaigning. Several nodes can
        # start an election in the same term; the earliest is when the cluster actually
        # began being leaderless, which is the number worth reporting.
        self._campaign_started: dict[int, float] = {}

        if tracer is not None:
            tracer.subscribe(self.record)

    # --- folding --------------------------------------------------------------------

    def record(self, event: Event) -> None:
        """Fold one event in. Safe to call directly; usually a tracer subscription."""
        if event.node is not None:
            self.known_nodes.add(event.node)
        if event.term is not None and event.term > self.current_term:
            self.current_term = event.term

        match event.kind:
            case EventKind.ELECTION_STARTED:
                self.elections_started += 1
                if event.term is not None:
                    self._campaign_started.setdefault(event.term, event.timestamp)

            case EventKind.VOTE_REQUESTED:
                self.votes_requested += 1

            case EventKind.VOTE_GRANTED:
                self.votes_granted += 1

            case EventKind.LEADER_ELECTED:
                self.leaders_elected += 1
                if event.term is not None and event.term >= self.leader_term:
                    self.leader = event.node
                    self.leader_term = event.term
                    started = self._campaign_started.pop(event.term, None)
                    if started is not None:
                        self.election_durations.append(event.timestamp - started)

            case EventKind.TERM_CHANGED:
                self.term_changes += 1
                # A new term means whoever led the old one no longer does. Reporting a
                # stale leader is worse than reporting none: a dashboard that keeps
                # naming a deposed node is how a split brain gets mistaken for health.
                if event.term is not None and event.term > self.leader_term:
                    self.leader = None

            case EventKind.APPEND_ENTRIES_SENT:
                self.append_entries_sent += 1

            case EventKind.APPEND_ENTRIES_REJECTED:
                self.append_entries_rejected += 1

            case EventKind.LOG_COMMITTED:
                if event.node is not None:
                    to = int(event.details.get("to", 0))
                    self.commit_index[event.node] = max(self.commit_index.get(event.node, 0), to)

            case EventKind.NODE_CRASHED:
                if event.node is not None:
                    self.down.add(event.node)

            case EventKind.NODE_RECOVERED:
                if event.node is not None:
                    self.down.discard(event.node)

            case EventKind.PARTITION_CREATED:
                self.partitioned = True
                self.partitions_created += 1
                self.partition_groups = [list(g) for g in event.details.get("groups", [])]

            case EventKind.PARTITION_HEALED:
                self.partitioned = False
                self.partition_groups = []

    # --- derived readings -------------------------------------------------------------

    @property
    def up(self) -> set[NodeId]:
        return self.known_nodes - self.down

    @property
    def max_commit_index(self) -> int:
        return max(self.commit_index.values(), default=0)

    @property
    def last_election_seconds(self) -> float | None:
        return self.election_durations[-1] if self.election_durations else None

    @property
    def mean_election_seconds(self) -> float | None:
        return mean(self.election_durations) if self.election_durations else None

    @property
    def has_leader(self) -> bool:
        return self.leader is not None

    @property
    def status(self) -> str:
        """One word for the cluster, for a dashboard header.

        Deliberately coarse, and deliberately pessimistic about a partition: a cluster
        that still has a leader but is split is not healthy, whatever the majority side
        currently thinks.
        """
        if not self.has_leader:
            return "NO LEADER"
        if self.partitioned:
            return "PARTITIONED"
        if self.down:
            return "DEGRADED"
        return "HEALTHY"

    def snapshot(self) -> dict[str, Any]:
        """A flat, JSON-safe reading of everything above - what a dashboard renders and
        what a `cluster status` command prints."""
        return {
            "term": self.current_term,
            "leader": self.leader,
            "status": self.status,
            "nodes": sorted(self.known_nodes),
            "down": sorted(self.down),
            "partitioned": self.partitioned,
            "partition_groups": self.partition_groups,
            "commit_index": dict(sorted(self.commit_index.items())),
            "max_commit_index": self.max_commit_index,
            "elections_started": self.elections_started,
            "leaders_elected": self.leaders_elected,
            "votes_requested": self.votes_requested,
            "votes_granted": self.votes_granted,
            "term_changes": self.term_changes,
            "append_entries_sent": self.append_entries_sent,
            "append_entries_rejected": self.append_entries_rejected,
            "last_election_seconds": self.last_election_seconds,
            "mean_election_seconds": self.mean_election_seconds,
        }
