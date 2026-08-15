"""Phase 4 acceptance: a real run tells its own story.

The point of the event system is that after a run you can answer "why did that happen?"
from the trace alone. So these tests run actual clusters and then interrogate nothing
but the recorded events - if the story is not in the trace, it is not in these
assertions either.
"""

from __future__ import annotations

import random
from pathlib import Path

import pytest
from tests.test_node_election import FAST_ELECTION, FAST_HEARTBEAT, leaders, wait_until

from pyraft_lab.kv.commands import Put
from pyraft_lab.network import InMemoryBus
from pyraft_lab.network.simulator import SimulatedNetwork
from pyraft_lab.observability.events import EventKind, read_jsonl
from pyraft_lab.observability.metrics import LiveMetrics
from pyraft_lab.observability.tracer import NULL_TRACER, Tracer
from pyraft_lab.raft.node import RaftNode
from pyraft_lab.raft.state import Role


def put(key: str, value: object) -> dict[str, object]:
    return Put(key=key, value=value).to_wire()


@pytest.fixture
def tracer() -> Tracer:
    """On the wall clock, like the cluster it watches: these are polling tests, and
    ``test_tracer.py`` already asserts exact timing against a virtual clock."""
    return Tracer()


@pytest.fixture
def bus() -> InMemoryBus:
    return InMemoryBus()


@pytest.fixture
def net(tracer: Tracer) -> SimulatedNetwork:
    return SimulatedNetwork(rng=random.Random(0), tracer=tracer)


def build_cluster(transport, size: int, tracer: Tracer | None = None, seed: int = 0):
    ids = [f"n{i}" for i in range(1, size + 1)]
    return [
        RaftNode(
            node_id=node_id,
            peers=ids,
            transport=transport,
            rng=random.Random(seed + i),
            tracer=tracer,
            election_timeout_range=FAST_ELECTION,
            heartbeat_interval=FAST_HEARTBEAT,
        )
        for i, node_id in enumerate(ids)
    ]


async def run_cluster(nodes: list[RaftNode]) -> None:
    for node in nodes:
        node.start()
    assert await wait_until(lambda: len(leaders(nodes)) == 1)


async def shutdown(nodes: list[RaftNode]) -> None:
    for node in nodes:
        await node.stop()


def kinds(tracer: Tracer) -> set[EventKind]:
    return {event.kind for event in tracer}


# --- instrumentation is opt-in ------------------------------------------------------


async def test_a_cluster_given_no_tracer_records_nothing(bus: InMemoryBus) -> None:
    """The default is silence, and silence has to be free: a node without a tracer must
    not be quietly filling one in somewhere."""
    nodes = build_cluster(bus, 3)
    await run_cluster(nodes)

    try:
        leaders(nodes)[0].submit(put("k", 1))
        assert len(NULL_TRACER) == 0
    finally:
        await shutdown(nodes)


# --- an election, narrated -----------------------------------------------------------


async def test_an_election_is_fully_narrated(bus: InMemoryBus, tracer: Tracer) -> None:
    nodes = build_cluster(bus, 3, tracer)
    await run_cluster(nodes)

    try:
        assert {
            EventKind.TERM_CHANGED,
            EventKind.ELECTION_STARTED,
            EventKind.VOTE_REQUESTED,
            EventKind.VOTE_GRANTED,
            EventKind.LEADER_ELECTED,
            EventKind.APPEND_ENTRIES_SENT,
        } <= kinds(tracer)
    finally:
        await shutdown(nodes)


async def test_the_trace_names_the_node_that_actually_leads(
    bus: InMemoryBus, tracer: Tracer
) -> None:
    nodes = build_cluster(bus, 3, tracer)
    await run_cluster(nodes)

    try:
        leader = leaders(nodes)[0]
        elected = tracer.last(EventKind.LEADER_ELECTED)

        assert elected is not None
        assert elected.node == leader.node_id
        assert elected.term == leader.current_term
        assert elected.details["votes"] >= 2
        assert elected.details["cluster_size"] == 3
    finally:
        await shutdown(nodes)


async def test_a_candidate_solicits_every_peer(bus: InMemoryBus, tracer: Tracer) -> None:
    nodes = build_cluster(bus, 3, tracer)
    await run_cluster(nodes)

    try:
        started = tracer.first(EventKind.ELECTION_STARTED)
        assert started is not None
        candidate = started.node

        asked = {
            e.details["peer"]
            for e in tracer.of_kind(EventKind.VOTE_REQUESTED)
            if e.node == candidate and e.term == started.term
        }
        assert asked == {n.node_id for n in nodes if n.node_id != candidate}
    finally:
        await shutdown(nodes)


async def test_the_term_change_is_recorded_before_the_election_it_belongs_to(
    bus: InMemoryBus, tracer: Tracer
) -> None:
    """Ordering is the whole value of a trace. An election recorded before the term
    change that produced it would read as a node campaigning in a term it had not
    reached yet."""
    nodes = build_cluster(bus, 3, tracer)
    await run_cluster(nodes)

    try:
        started = tracer.first(EventKind.ELECTION_STARTED)
        assert started is not None

        change = next(
            e
            for e in tracer.of_kind(EventKind.TERM_CHANGED)
            if e.node == started.node and e.term == started.term
        )
        assert change.order < started.order
        assert change.details["previous_term"] == started.term - 1
    finally:
        await shutdown(nodes)


async def test_a_voter_records_who_it_voted_for(bus: InMemoryBus, tracer: Tracer) -> None:
    nodes = build_cluster(bus, 3, tracer)
    await run_cluster(nodes)

    try:
        leader = leaders(nodes)[0]
        granted = [
            e
            for e in tracer.of_kind(EventKind.VOTE_GRANTED)
            if e.details["candidate"] == leader.node_id
        ]

        assert granted, "the leader must have been voted for by somebody"
        assert all(e.node != leader.node_id for e in granted), "a self-vote is not an RPC"
    finally:
        await shutdown(nodes)


# --- replication, narrated -----------------------------------------------------------


async def test_committing_is_narrated_with_the_index_it_reached(
    bus: InMemoryBus, tracer: Tracer
) -> None:
    nodes = build_cluster(bus, 3, tracer)
    await run_cluster(nodes)

    try:
        leader = leaders(nodes)[0]
        for i in range(5):
            leader.submit(put(f"k{i}", i))
        assert await wait_until(lambda: leader.state.commit_index >= 5)

        committed = [e for e in tracer.of_kind(EventKind.LOG_COMMITTED) if e.node == leader.node_id]
        assert committed
        assert committed[-1].details["to"] == leader.state.commit_index
        # Each event's window starts where the previous one ended: a commit index that
        # skipped a range would mean entries nobody can account for.
        for previous, current in zip(committed, committed[1:], strict=False):
            assert current.details["from"] == previous.details["to"]
    finally:
        await shutdown(nodes)


async def test_a_heartbeat_is_recorded_as_an_empty_append(bus: InMemoryBus, tracer: Tracer) -> None:
    """Replication and heartbeats are one code path (D4), so the trace distinguishes
    them the same way the wire does: by whether there were any entries."""
    nodes = build_cluster(bus, 3, tracer)
    await run_cluster(nodes)

    try:
        leader = leaders(nodes)[0]
        # Wait for one rather than assuming: an idle leader only produces an empty
        # append once a follower has caught up *and* the next heartbeat tick arrives,
        # which is not guaranteed to have happened by the time the election settles.
        assert await wait_until(
            lambda: any(
                e.details["entries"] == 0 for e in tracer.of_kind(EventKind.APPEND_ENTRIES_SENT)
            )
        ), "no heartbeats recorded"

        for i in range(3):
            leader.submit(put(f"k{i}", i))
        assert await wait_until(lambda: leader.state.commit_index >= 3)

        sent = tracer.of_kind(EventKind.APPEND_ENTRIES_SENT)
        assert any(e.details["entries"] > 0 for e in sent), "no replication recorded"
    finally:
        await shutdown(nodes)


async def test_a_rejoining_empty_node_records_why_it_rejected(
    bus: InMemoryBus, tracer: Tracer
) -> None:
    """The reason a follower rejects exists nowhere on the wire - Figure 2's reply
    carries only term and success - so the follower is the only place it can be
    recorded. This is that reason surviving to the trace."""
    nodes = build_cluster(bus, 3, tracer)
    await run_cluster(nodes)
    ids = [n.node_id for n in nodes]

    try:
        leader = leaders(nodes)[0]
        follower = next(n for n in nodes if n is not leader)
        await follower.stop()

        for i in range(5):
            leader.submit(put(f"k{i}", i))
        assert await wait_until(lambda: leader.state.commit_index >= 5)

        # A hard crash: same id, no log, no term, no vote.
        replacement = RaftNode(
            node_id=follower.node_id,
            peers=ids,
            transport=bus,
            rng=random.Random(hash(follower.node_id) & 0xFFFF),
            tracer=tracer,
            election_timeout_range=FAST_ELECTION,
            heartbeat_interval=FAST_HEARTBEAT,
        )
        nodes = [n if n is not follower else replacement for n in nodes]
        replacement.start()

        assert await wait_until(lambda: replacement.state.commit_index == leader.state.commit_index)

        rejections = [
            e
            for e in tracer.of_kind(EventKind.APPEND_ENTRIES_REJECTED)
            if e.node == replacement.node_id
        ]
        assert rejections, "an empty log cannot have accepted the leader's first append"
        assert rejections[0].details["reason"] == "log_mismatch"
        assert rejections[0].details["leader"] == leader.node_id

        # And the leader's reaction - nextIndex walking back - is visible as the next
        # append to that peer starting from further behind. That is the half of the
        # exchange the follower's own event cannot show.
        rejected_at = rejections[0].details["prev_log_index"]
        assert any(
            e.details["peer"] == replacement.node_id
            and e.details["prev_log_index"] < rejected_at
            and e.order > rejections[0].order
            for e in tracer.of_kind(EventKind.APPEND_ENTRIES_SENT)
        ), "the leader never retried from further back"
    finally:
        await shutdown(nodes)


# --- the network narrates what a node cannot ------------------------------------------


async def test_a_partition_and_its_healing_are_recorded(
    net: SimulatedNetwork, tracer: Tracer
) -> None:
    net.register("n1")
    net.register("n2")
    net.register("n3")

    net.partition(["n1", "n2"], ["n3"])
    net.heal()

    created = tracer.first(EventKind.PARTITION_CREATED)
    healed = tracer.first(EventKind.PARTITION_HEALED)
    assert created is not None and healed is not None
    assert created.details["groups"] == [["n1", "n2"], ["n3"]]
    assert created.node is None, "a partition belongs to no single node"
    assert created.order < healed.order


async def test_healing_an_unsplit_network_is_not_an_event(
    net: SimulatedNetwork, tracer: Tracer
) -> None:
    net.register("n1")

    net.heal()
    net.reset_faults()

    assert tracer.count(EventKind.PARTITION_HEALED) == 0


async def test_resetting_faults_heals_and_says_so(net: SimulatedNetwork, tracer: Tracer) -> None:
    net.register("n1")
    net.register("n2")
    net.partition(["n1"], ["n2"])

    net.reset_faults()

    assert tracer.count(EventKind.PARTITION_HEALED) == 1


async def test_joining_is_not_recovering(net: SimulatedNetwork, tracer: Tracer) -> None:
    """A node's first appearance is a join. Recording it as a recovery would put a
    recovery in every run before anything had gone wrong."""
    net.register("n1")

    assert tracer.count(EventKind.NODE_RECOVERED) == 0


async def test_a_crash_and_a_rejoin_are_recorded_by_the_network(
    net: SimulatedNetwork, tracer: Tracer
) -> None:
    nodes = build_cluster(net, 3, tracer)
    await run_cluster(nodes)

    try:
        victim = next(n for n in nodes if n.role is not Role.LEADER)
        await victim.stop()

        crashed = tracer.last(EventKind.NODE_CRASHED)
        assert crashed is not None
        assert crashed.node == victim.node_id
        assert crashed.term is None, "the network cannot know what term a node was in"

        victim.start()
        recovered = tracer.last(EventKind.NODE_RECOVERED)
        assert recovered is not None
        assert recovered.node == victim.node_id
        assert crashed.order < recovered.order
    finally:
        await shutdown(nodes)


# --- live metrics over a real run -----------------------------------------------------


async def test_live_metrics_track_a_real_cluster(bus: InMemoryBus, tracer: Tracer) -> None:
    metrics = LiveMetrics(tracer)
    nodes = build_cluster(bus, 3, tracer)
    await run_cluster(nodes)

    try:
        leader = leaders(nodes)[0]
        for i in range(3):
            leader.submit(put(f"k{i}", i))
        assert await wait_until(lambda: leader.state.commit_index >= 3)

        assert metrics.leader == leader.node_id
        assert metrics.current_term == leader.current_term
        assert metrics.status == "HEALTHY"
        assert metrics.max_commit_index == leader.state.commit_index
        assert metrics.leaders_elected >= 1
        assert metrics.last_election_seconds is not None
        assert metrics.last_election_seconds >= 0
    finally:
        await shutdown(nodes)


async def test_live_metrics_notice_a_node_going_down(net: SimulatedNetwork, tracer: Tracer) -> None:
    metrics = LiveMetrics(tracer)
    nodes = build_cluster(net, 3, tracer)
    await run_cluster(nodes)

    try:
        victim = next(n for n in nodes if n.role is not Role.LEADER)
        await victim.stop()

        assert metrics.down == {victim.node_id}
        assert metrics.status == "DEGRADED"
    finally:
        await shutdown([n for n in nodes if n is not victim])


# --- the trace outlives the run --------------------------------------------------------


async def test_a_run_s_trace_survives_a_round_trip_to_disk(
    bus: InMemoryBus, tracer: Tracer, tmp_path: Path
) -> None:
    """What Phase 5's metrics and Phase 7's replay will read: a finished run, from a
    file, with nothing of the cluster left in memory."""
    nodes = build_cluster(bus, 3, tracer)
    await run_cluster(nodes)
    try:
        leaders(nodes)[0].submit(put("k", 1))
        assert await wait_until(lambda: leaders(nodes)[0].state.commit_index >= 1)
    finally:
        await shutdown(nodes)

    path = tmp_path / "run.jsonl"
    tracer.write_jsonl(path)

    replayed = read_jsonl(path)
    assert replayed == tracer.events

    metrics = LiveMetrics()
    for event in replayed:
        metrics.record(event)
    assert metrics.leader is not None
    assert metrics.leaders_elected >= 1
