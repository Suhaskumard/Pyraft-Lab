"""Phase 4: the running fold of a trace that a dashboard reads.

Everything here is driven by emitting events rather than by touching a cluster - which
is the point of the design: ``LiveMetrics`` knows only what the trace says, so a metric
that looks right while the trace is wrong is not a thing that can happen.
"""

from __future__ import annotations

import pytest

from pyraft_lab.network.clock import VirtualClock
from pyraft_lab.observability.events import EventKind
from pyraft_lab.observability.metrics import LiveMetrics
from pyraft_lab.observability.tracer import Tracer


@pytest.fixture
def clock() -> VirtualClock:
    return VirtualClock()


@pytest.fixture
def tracer(clock: VirtualClock) -> Tracer:
    return Tracer(clock)


@pytest.fixture
def metrics(tracer: Tracer) -> LiveMetrics:
    return LiveMetrics(tracer)


# --- leadership ---------------------------------------------------------------------


def test_it_starts_knowing_nothing(metrics: LiveMetrics) -> None:
    assert metrics.leader is None
    assert metrics.current_term == 0
    assert metrics.status == "NO LEADER"


def test_it_follows_the_term(tracer: Tracer, metrics: LiveMetrics) -> None:
    tracer.emit(EventKind.TERM_CHANGED, node="n1", term=4, previous_term=3)

    assert metrics.current_term == 4
    assert metrics.term_changes == 1


def test_it_names_the_elected_leader(tracer: Tracer, metrics: LiveMetrics) -> None:
    tracer.emit(EventKind.LEADER_ELECTED, node="n2", term=3, votes=2, cluster_size=3)

    assert metrics.leader == "n2"
    assert metrics.leader_term == 3
    assert metrics.has_leader
    assert metrics.status == "HEALTHY"


def test_a_new_term_unseats_the_leader_it_did_not_elect(
    tracer: Tracer, metrics: LiveMetrics
) -> None:
    """Reporting a deposed leader is worse than reporting none: it is how a split brain
    gets mistaken for a healthy cluster."""
    tracer.emit(EventKind.LEADER_ELECTED, node="n1", term=1, votes=2)
    tracer.emit(EventKind.TERM_CHANGED, node="n2", term=2, previous_term=1)

    assert metrics.leader is None
    assert metrics.status == "NO LEADER"


def test_a_stale_election_does_not_replace_the_current_leader(
    tracer: Tracer, metrics: LiveMetrics
) -> None:
    """A late-arriving event from an older term must not overwrite what a later one
    already established."""
    tracer.emit(EventKind.LEADER_ELECTED, node="n2", term=5, votes=3)
    tracer.emit(EventKind.LEADER_ELECTED, node="n1", term=2, votes=3)

    assert metrics.leader == "n2"
    assert metrics.leader_term == 5


# --- election timing ----------------------------------------------------------------


async def test_it_times_an_election(
    tracer: Tracer, metrics: LiveMetrics, clock: VirtualClock
) -> None:
    tracer.emit(EventKind.ELECTION_STARTED, node="n1", term=1)
    await clock.advance(0.182)
    tracer.emit(EventKind.LEADER_ELECTED, node="n1", term=1, votes=2)

    assert metrics.last_election_seconds == pytest.approx(0.182)
    assert metrics.mean_election_seconds == pytest.approx(0.182)


async def test_an_election_is_timed_from_the_first_candidate(
    tracer: Tracer, metrics: LiveMetrics, clock: VirtualClock
) -> None:
    """Two nodes can campaign in the same term. The cluster became leaderless when the
    first of them started, not the second."""
    tracer.emit(EventKind.ELECTION_STARTED, node="n1", term=7)
    await clock.advance(0.05)
    tracer.emit(EventKind.ELECTION_STARTED, node="n2", term=7)
    await clock.advance(0.05)
    tracer.emit(EventKind.LEADER_ELECTED, node="n2", term=7, votes=2)

    assert metrics.last_election_seconds == pytest.approx(0.1)


async def test_it_averages_across_elections(
    tracer: Tracer, metrics: LiveMetrics, clock: VirtualClock
) -> None:
    for term, duration in ((1, 0.1), (2, 0.3)):
        tracer.emit(EventKind.ELECTION_STARTED, node="n1", term=term)
        await clock.advance(duration)
        tracer.emit(EventKind.LEADER_ELECTED, node="n1", term=term, votes=2)

    assert metrics.mean_election_seconds == pytest.approx(0.2)
    assert metrics.last_election_seconds == pytest.approx(0.3)


def test_an_election_that_never_finished_is_not_timed(tracer: Tracer, metrics: LiveMetrics) -> None:
    tracer.emit(EventKind.ELECTION_STARTED, node="n1", term=1)

    assert metrics.last_election_seconds is None
    assert metrics.mean_election_seconds is None
    assert metrics.elections_started == 1


# --- replication --------------------------------------------------------------------


def test_it_tracks_each_node_s_commit_index(tracer: Tracer, metrics: LiveMetrics) -> None:
    tracer.emit(EventKind.LOG_COMMITTED, node="n1", term=1, **{"from": 0}, to=9, entries=9)
    tracer.emit(EventKind.LOG_COMMITTED, node="n2", term=1, **{"from": 0}, to=4, entries=4)

    assert metrics.commit_index == {"n1": 9, "n2": 4}
    assert metrics.max_commit_index == 9


def test_a_commit_index_never_goes_backward(tracer: Tracer, metrics: LiveMetrics) -> None:
    """Out-of-order events must not make a node look like it lost committed entries -
    a committed entry is committed forever."""
    tracer.emit(EventKind.LOG_COMMITTED, node="n1", term=2, **{"from": 0}, to=9)
    tracer.emit(EventKind.LOG_COMMITTED, node="n1", term=1, **{"from": 0}, to=3)

    assert metrics.commit_index["n1"] == 9


def test_it_counts_replication_traffic(tracer: Tracer, metrics: LiveMetrics) -> None:
    tracer.emit(EventKind.APPEND_ENTRIES_SENT, node="n1", term=1, peer="n2", entries=0)
    tracer.emit(EventKind.APPEND_ENTRIES_SENT, node="n1", term=1, peer="n3", entries=2)
    tracer.emit(EventKind.APPEND_ENTRIES_REJECTED, node="n3", term=1, reason="log_mismatch")

    assert metrics.append_entries_sent == 2
    assert metrics.append_entries_rejected == 1


def test_it_counts_votes(tracer: Tracer, metrics: LiveMetrics) -> None:
    tracer.emit(EventKind.VOTE_REQUESTED, node="n1", term=1, peer="n2")
    tracer.emit(EventKind.VOTE_GRANTED, node="n2", term=1, candidate="n1")

    assert metrics.votes_requested == 1
    assert metrics.votes_granted == 1


# --- health -------------------------------------------------------------------------


def test_it_tracks_who_is_down(tracer: Tracer, metrics: LiveMetrics) -> None:
    tracer.emit(EventKind.LEADER_ELECTED, node="n1", term=1, votes=2)
    tracer.emit(EventKind.APPEND_ENTRIES_SENT, node="n1", term=1, peer="n2")
    tracer.emit(EventKind.LOG_COMMITTED, node="n2", term=1, **{"from": 0}, to=1)
    tracer.emit(EventKind.NODE_CRASHED, node="n2")

    assert metrics.down == {"n2"}
    assert metrics.up == {"n1"}
    assert metrics.status == "DEGRADED"


def test_a_recovered_node_is_no_longer_down(tracer: Tracer, metrics: LiveMetrics) -> None:
    tracer.emit(EventKind.LEADER_ELECTED, node="n1", term=1, votes=2)
    tracer.emit(EventKind.NODE_CRASHED, node="n2")
    tracer.emit(EventKind.NODE_RECOVERED, node="n2")

    assert metrics.down == set()
    assert metrics.status == "HEALTHY"


def test_a_partition_is_not_health(tracer: Tracer, metrics: LiveMetrics) -> None:
    """A majority side with a leader still reports PARTITIONED: whatever that side
    believes, part of the cluster is unreachable."""
    tracer.emit(EventKind.LEADER_ELECTED, node="n1", term=1, votes=3)
    tracer.emit(EventKind.PARTITION_CREATED, groups=[["n1", "n2", "n3"], ["n4", "n5"]])

    assert metrics.partitioned
    assert metrics.partitions_created == 1
    assert metrics.partition_groups == [["n1", "n2", "n3"], ["n4", "n5"]]
    assert metrics.status == "PARTITIONED"


def test_healing_clears_the_partition(tracer: Tracer, metrics: LiveMetrics) -> None:
    tracer.emit(EventKind.LEADER_ELECTED, node="n1", term=1, votes=3)
    tracer.emit(EventKind.PARTITION_CREATED, groups=[["n1"], ["n2"]])
    tracer.emit(EventKind.PARTITION_HEALED)

    assert not metrics.partitioned
    assert metrics.partition_groups == []
    assert metrics.status == "HEALTHY"


# --- the snapshot -------------------------------------------------------------------


def test_the_snapshot_is_flat_and_json_safe(tracer: Tracer, metrics: LiveMetrics) -> None:
    import json

    tracer.emit(EventKind.ELECTION_STARTED, node="n1", term=1)
    tracer.emit(EventKind.LEADER_ELECTED, node="n1", term=1, votes=2, cluster_size=3)
    tracer.emit(EventKind.LOG_COMMITTED, node="n1", term=1, **{"from": 0}, to=3)

    snapshot = metrics.snapshot()

    assert json.loads(json.dumps(snapshot)) == snapshot
    assert snapshot["term"] == 1
    assert snapshot["leader"] == "n1"
    assert snapshot["status"] == "HEALTHY"
    assert snapshot["max_commit_index"] == 3
    assert snapshot["nodes"] == ["n1"]


def test_it_can_be_folded_without_a_tracer() -> None:
    """``record`` is an ordinary method - a trace read back from disk can be replayed
    through it just as a live run is, which is what Phase 7 will do."""
    metrics = LiveMetrics()
    tracer = Tracer(VirtualClock())
    tracer.emit(EventKind.LEADER_ELECTED, node="n1", term=2, votes=2)

    for event in tracer.events:
        metrics.record(event)

    assert metrics.leader == "n1"
