"""Phase 1 acceptance: a real cluster on a real bus elects exactly one leader.

These drive ``RaftNode`` end to end over the ``InMemoryBus``. Timeouts are compressed
far below the production 150-300 ms so the suite stays fast; the ratio between the
heartbeat and the election timeout is preserved, which is the part that actually
matters for correctness.
"""

from __future__ import annotations

import asyncio
import random

import pytest

from pyraft_lab.network import InMemoryBus
from pyraft_lab.raft.node import RaftNode
from pyraft_lab.raft.state import Role

FAST_ELECTION = (0.050, 0.100)
FAST_HEARTBEAT = 0.012
SETTLE = 0.6  # an upper bound to wait *up to*, not a duration to sleep for


async def wait_until(predicate, timeout: float = SETTLE, interval: float = 0.005) -> bool:
    """Poll ``predicate`` until it holds or ``timeout`` elapses.

    Preferred over sleeping a fixed duration. These nodes still run on the event loop's
    wall clock - the deterministic virtual clock arrives in Phase 3 - so under a loaded
    machine a fixed sleep is a race: too short and the suite flakes, too long and every
    run pays for the worst case. Polling is fast when the cluster converges quickly and
    patient when it does not.
    """
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while loop.time() < deadline:
        if predicate():
            return True
        await asyncio.sleep(interval)
    return predicate()


def build_cluster(bus: InMemoryBus, size: int, seed: int = 0) -> list[RaftNode]:
    ids = [f"n{i}" for i in range(1, size + 1)]
    return [
        RaftNode(
            node_id=node_id,
            peers=ids,
            transport=bus,
            rng=random.Random(seed + i),
            election_timeout_range=FAST_ELECTION,
            heartbeat_interval=FAST_HEARTBEAT,
        )
        for i, node_id in enumerate(ids)
    ]


async def run_cluster(nodes: list[RaftNode], settle: float = SETTLE) -> None:
    """Start every node and wait for an election to settle on exactly one leader."""
    for node in nodes:
        node.start()
    await wait_until(lambda: len(leaders(nodes)) == 1, timeout=settle)


async def shutdown(nodes: list[RaftNode]) -> None:
    for node in nodes:
        await node.stop()


def leaders(nodes: list[RaftNode]) -> list[RaftNode]:
    return [n for n in nodes if n.role is Role.LEADER]


@pytest.fixture
def bus() -> InMemoryBus:
    return InMemoryBus()


async def test_a_three_node_cluster_elects_exactly_one_leader(bus: InMemoryBus) -> None:
    nodes = build_cluster(bus, 3)
    await run_cluster(nodes)

    try:
        assert len(leaders(nodes)) == 1
    finally:
        await shutdown(nodes)


async def test_a_five_node_cluster_elects_exactly_one_leader(bus: InMemoryBus) -> None:
    nodes = build_cluster(bus, 5)
    await run_cluster(nodes)

    try:
        assert len(leaders(nodes)) == 1
    finally:
        await shutdown(nodes)


async def test_every_node_agrees_on_the_term_and_the_leader(bus: InMemoryBus) -> None:
    nodes = build_cluster(bus, 3)
    await run_cluster(nodes)

    try:
        leader = leaders(nodes)[0]
        assert {n.current_term for n in nodes} == {leader.current_term}
        assert {n.leader_id for n in nodes} == {leader.node_id}
    finally:
        await shutdown(nodes)


async def test_at_most_one_leader_per_term(bus: InMemoryBus) -> None:
    """Election safety, the invariant the whole phase exists to protect."""
    nodes = build_cluster(bus, 5)
    await run_cluster(nodes)

    try:
        elected = leaders(nodes)
        assert len(elected) == 1
        leader = elected[0]
        # Nobody else believes they lead this term.
        for node in nodes:
            if node is not leader:
                assert node.role is not Role.LEADER
    finally:
        await shutdown(nodes)


async def test_followers_stay_followers_while_the_leader_heartbeats(bus: InMemoryBus) -> None:
    """Heartbeats must keep election timers from ever expiring."""
    nodes = build_cluster(bus, 3)
    await run_cluster(nodes)

    try:
        leader = leaders(nodes)[0]
        term_after_election = leader.current_term

        # Let many election timeouts' worth of time pass with the leader alive.
        await asyncio.sleep(SETTLE)

        assert leaders(nodes) == [leader]
        assert leader.current_term == term_after_election  # no new elections churned
    finally:
        await shutdown(nodes)


async def test_a_new_leader_is_elected_after_the_leader_crashes(bus: InMemoryBus) -> None:
    """The Phase 1 acceptance criterion: leader crash results in a new election."""
    nodes = build_cluster(bus, 3)
    await run_cluster(nodes)
    alive = list(nodes)

    try:
        original = leaders(nodes)[0]
        original_term = original.current_term

        await original.stop()  # crash: it leaves the bus and stops heartbeating
        alive = [n for n in nodes if n is not original]

        await asyncio.sleep(SETTLE)

        assert len(leaders(alive)) == 1
        assert leaders(alive)[0].current_term > original_term
    finally:
        await shutdown(alive)


async def test_a_single_node_cluster_elects_itself(bus: InMemoryBus) -> None:
    """A lone node is its own majority and needs no votes to win."""
    node = RaftNode(
        node_id="solo",
        peers=["solo"],
        transport=bus,
        rng=random.Random(0),
        election_timeout_range=FAST_ELECTION,
        heartbeat_interval=FAST_HEARTBEAT,
    )
    node.start()
    await asyncio.sleep(0.15)

    try:
        assert node.role is Role.LEADER
    finally:
        await node.stop()


async def test_a_minority_cannot_elect_a_leader(bus: InMemoryBus) -> None:
    """Two nodes of a five-node cluster can never reach a majority of three."""
    ids = [f"n{i}" for i in range(1, 6)]
    minority = [
        RaftNode(
            node_id=node_id,
            peers=ids,
            transport=bus,
            rng=random.Random(i),
            election_timeout_range=FAST_ELECTION,
            heartbeat_interval=FAST_HEARTBEAT,
        )
        for i, node_id in enumerate(ids[:2])
    ]

    await run_cluster(minority)

    try:
        assert leaders(minority) == []
        # They keep campaigning, which is correct: terms climb without a winner.
        assert all(n.current_term > 1 for n in minority)
    finally:
        await shutdown(minority)


async def test_the_cluster_recovers_repeatedly_across_successive_crashes(bus: InMemoryBus) -> None:
    """Churn: crash the leader twice, and a five-node cluster still re-elects."""
    nodes = build_cluster(bus, 5)
    await run_cluster(nodes)
    alive = list(nodes)

    try:
        for _ in range(2):
            leader = leaders(alive)[0]
            await leader.stop()
            alive = [n for n in alive if n is not leader]
            await asyncio.sleep(SETTLE)

            assert len(leaders(alive)) == 1  # 4 then 3 nodes: still a quorum
    finally:
        await shutdown(alive)
