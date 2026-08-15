"""Day 9: real clusters under real partitions, driven over ``SimulatedNetwork``.

The plan's acceptance criteria for the day: a minority side makes no progress, the
majority side does, and healing reconciles the two without losing anything the
majority committed. This is also the first file where nodes run over the simulator
rather than ``InMemoryBus`` - the point being that nothing in ``raft/`` changed to
make that work.
"""

from __future__ import annotations

import random

import pytest
from tests.test_node_election import (
    FAST_ELECTION,
    FAST_HEARTBEAT,
    SETTLE,
    leaders,
    wait_until,
)

from pyraft_lab.kv.commands import Put
from pyraft_lab.network.simulator import SimulatedNetwork
from pyraft_lab.raft.node import RaftNode
from pyraft_lab.raft.state import Role


def put(key: str, value: object) -> dict[str, object]:
    return Put(key=key, value=value).to_wire()


@pytest.fixture
def net() -> SimulatedNetwork:
    """A simulated network on the *wall* clock, so the same polling helpers the
    Phase 1 suites use still apply. Fault injection is what is under test here; the
    virtual clock's exact-timing assertions are ``test_simulator.py``'s job.
    """
    return SimulatedNetwork(rng=random.Random(0))


def build_cluster(net: SimulatedNetwork, size: int, seed: int = 0) -> list[RaftNode]:
    ids = [f"n{i}" for i in range(1, size + 1)]
    return [
        RaftNode(
            node_id=node_id,
            peers=ids,
            transport=net,
            rng=random.Random(seed + i),
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


def committed_commands(node: RaftNode) -> list[object]:
    return [
        node.state.log.entry_at(i).command  # type: ignore[union-attr]
        for i in range(1, node.state.commit_index + 1)
    ]


# --- the cluster still works over the simulator at all ------------------------------


async def test_a_cluster_elects_a_leader_over_the_simulated_network(
    net: SimulatedNetwork,
) -> None:
    """No node knows it is on a fault-injecting transport rather than the honest bus."""
    nodes = build_cluster(net, 3)
    await run_cluster(nodes)
    try:
        assert len(leaders(nodes)) == 1
    finally:
        await shutdown(nodes)


async def test_replication_survives_latency_and_loss(net: SimulatedNetwork) -> None:
    """Day 8's faults, without a partition: slow and lossy, but still correct."""
    nodes = build_cluster(net, 3)
    await run_cluster(nodes)

    try:
        net.set_latency(0.001, 0.005)
        net.set_loss(0.1)

        leader = leaders(nodes)[0]
        base = leader.state.log.last_index
        for i in range(20):
            leader.submit(put(f"k{i}", i))

        assert await wait_until(
            lambda: all(n.state.commit_index >= base + 20 for n in nodes), timeout=3.0
        )
        for node in nodes:
            assert committed_commands(node) == committed_commands(leader)
    finally:
        net.reset_faults()
        await shutdown(nodes)


# --- minority / majority ------------------------------------------------------------


async def test_a_minority_partition_makes_no_progress(net: SimulatedNetwork) -> None:
    """1 node cut off from 4 can never reach a majority, however long it campaigns."""
    nodes = build_cluster(net, 5)
    await run_cluster(nodes)

    try:
        isolated = next(n for n in nodes if n.role is not Role.LEADER)
        majority_side = [n for n in nodes if n is not isolated]
        net.partition({isolated.node_id}, {n.node_id for n in majority_side})

        assert not await wait_until(lambda: isolated.role is Role.LEADER, timeout=SETTLE)
        assert isolated.role is not Role.LEADER
        # It campaigns anyway, which is correct: terms climb with no winner.
        assert isolated.current_term > 1
    finally:
        net.heal()
        await shutdown(nodes)


async def test_the_majority_side_keeps_committing_during_a_partition(
    net: SimulatedNetwork,
) -> None:
    nodes = build_cluster(net, 5)
    await run_cluster(nodes)

    try:
        leader = leaders(nodes)[0]
        isolated = next(n for n in nodes if n is not leader)
        majority_side = [n for n in nodes if n is not isolated]
        net.partition({isolated.node_id}, {n.node_id for n in majority_side})

        base = leader.state.log.last_index
        for i in range(10):
            leader.submit(put(f"k{i}", i))

        assert await wait_until(
            lambda: all(n.state.commit_index >= base + 10 for n in majority_side)
        )
        # The cut-off node saw none of it.
        assert isolated.state.commit_index < base + 10
    finally:
        net.heal()
        await shutdown(nodes)


async def test_a_leader_in_the_minority_cannot_commit(net: SimulatedNetwork) -> None:
    """The safety half of the partition story: the old leader still *believes* it leads
    (nothing forces it to self-demote without hearing a higher term), and it may still
    append to its own log - but with no quorum reachable, none of it may commit.
    """
    nodes = build_cluster(net, 5)
    await run_cluster(nodes)

    try:
        leader = leaders(nodes)[0]
        with_leader = [leader, next(n for n in nodes if n is not leader)]
        majority_side = [n for n in nodes if n not in with_leader]
        net.partition({n.node_id for n in with_leader}, {n.node_id for n in majority_side})

        commit_before = leader.state.commit_index
        for i in range(5):
            leader.submit(put(f"doomed{i}", i))

        assert not await wait_until(
            lambda: leader.state.commit_index > commit_before, timeout=SETTLE
        )
        assert leader.state.commit_index == commit_before  # appended, never committed
        assert leader.state.log.last_index > commit_before
    finally:
        net.heal()
        await shutdown(nodes)


async def test_the_majority_side_elects_a_new_leader(net: SimulatedNetwork) -> None:
    nodes = build_cluster(net, 5)
    await run_cluster(nodes)

    try:
        old_leader = leaders(nodes)[0]
        minority = [old_leader, next(n for n in nodes if n is not old_leader)]
        majority_side = [n for n in nodes if n not in minority]
        net.partition({n.node_id for n in minority}, {n.node_id for n in majority_side})

        assert await wait_until(lambda: len(leaders(majority_side)) == 1)
        new_leader = leaders(majority_side)[0]
        assert new_leader.current_term > old_leader.current_term
    finally:
        net.heal()
        await shutdown(nodes)


# --- healing --------------------------------------------------------------------------


async def test_healing_reconciles_the_minority_with_the_majority(
    net: SimulatedNetwork,
) -> None:
    """The day's headline criterion: after the split heals, the side that could not
    commit adopts the side that could, and nothing the majority committed is lost.
    """
    nodes = build_cluster(net, 5)
    await run_cluster(nodes)

    try:
        old_leader = leaders(nodes)[0]
        minority = [old_leader, next(n for n in nodes if n is not old_leader)]
        majority_side = [n for n in nodes if n not in minority]
        net.partition({n.node_id for n in minority}, {n.node_id for n in majority_side})

        # Both sides write. Only the majority's writes can commit.
        for i in range(5):
            old_leader.submit(put(f"orphan{i}", i))
        assert await wait_until(lambda: len(leaders(majority_side)) == 1)
        new_leader = leaders(majority_side)[0]
        for i in range(5):
            new_leader.submit(put(f"real{i}", i))
        assert await wait_until(
            lambda: all(n.state.commit_index >= 5 for n in majority_side)
        )
        committed_by_majority = committed_commands(new_leader)

        net.heal()

        assert await wait_until(
            lambda: all(
                n.state.commit_index == new_leader.state.commit_index for n in nodes
            ),
            timeout=3.0,
        )
        for node in nodes:
            assert committed_commands(node) == committed_commands(new_leader)
        # Everything the majority committed is still there, in the same order.
        assert committed_commands(new_leader)[: len(committed_by_majority)] == (
            committed_by_majority
        )
        # The minority's uncommitted writes were discarded, never applied.
        assert all(put(f"orphan{i}", i) not in committed_commands(new_leader) for i in range(5))
    finally:
        net.heal()
        await shutdown(nodes)


async def test_the_old_leader_steps_down_after_healing(net: SimulatedNetwork) -> None:
    """Two nodes believe they lead while the split lasts - in different terms, which is
    why that is not a safety violation. Healing has to collapse it back to one.
    """
    nodes = build_cluster(net, 5)
    await run_cluster(nodes)

    try:
        old_leader = leaders(nodes)[0]
        minority = [old_leader, next(n for n in nodes if n is not old_leader)]
        majority_side = [n for n in nodes if n not in minority]
        net.partition({n.node_id for n in minority}, {n.node_id for n in majority_side})

        assert await wait_until(lambda: len(leaders(majority_side)) == 1)
        new_leader = leaders(majority_side)[0]
        deposed_term = old_leader.current_term
        assert new_leader.current_term > deposed_term  # a strictly newer term won

        net.heal()

        # Seeing the majority's higher term is what deposes it (paper §5.1).
        assert await wait_until(lambda: old_leader.role is not Role.LEADER, timeout=3.0)
        assert old_leader.current_term > deposed_term

        # One leader *and* one term, as a single predicate: checked separately, the
        # pair can both hold at moments that never coexist - every node agreeing on a
        # term mid-election is exactly when there is no leader at all.
        assert await wait_until(
            lambda: len(leaders(nodes)) == 1 and len({n.current_term for n in nodes}) == 1,
            timeout=3.0,
        )
    finally:
        net.heal()
        await shutdown(nodes)
