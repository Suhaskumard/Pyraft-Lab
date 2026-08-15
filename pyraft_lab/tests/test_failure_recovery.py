"""Day 7: the failure detection/recovery machinery - heartbeats and stale-leader
step-down, already built into Day 3's ``node.py`` - exercised under real crash/rejoin
scenarios. Phase 1's ``test_node_election.py`` already covers a *clean* stop/restart
(the same ``RaftNode`` object, state intact); what's new here is a *hard* crash - a
fresh ``RaftNode`` under the old node id, with no memory of anything - and cluster-wide
unavailability when a majority is down at once.
"""

from __future__ import annotations

import random

import pytest
from tests.test_node_election import (
    FAST_ELECTION,
    FAST_HEARTBEAT,
    SETTLE,
    build_cluster,
    leaders,
    run_cluster,
    shutdown,
    wait_until,
)

from pyraft_lab.kv.commands import Put
from pyraft_lab.network import InMemoryBus
from pyraft_lab.raft.node import RaftNode
from pyraft_lab.raft.state import Role


def put(key: str, value: object) -> dict[str, object]:
    return Put(key=key, value=value).to_wire()


@pytest.fixture
def bus() -> InMemoryBus:
    return InMemoryBus()


def committed_commands(node: RaftNode) -> list[object]:
    return [
        node.state.log.entry_at(i).command  # type: ignore[union-attr]
        for i in range(1, node.state.commit_index + 1)
    ]


def hard_crash_and_replace(bus: InMemoryBus, dead: RaftNode, all_ids: list[str]) -> RaftNode:
    """Simulate losing the process, not just the connection: a brand new ``RaftNode``
    under the same id, starting from nothing - no log, no term, no vote. Only the
    cluster's membership list is assumed to survive a real restart.
    """
    return RaftNode(
        node_id=dead.node_id,
        peers=all_ids,
        transport=bus,
        rng=random.Random(hash(dead.node_id) & 0xFFFF),
        election_timeout_range=FAST_ELECTION,
        heartbeat_interval=FAST_HEARTBEAT,
    )


# --- a follower that loses everything still rejoins safely -------------------------


async def test_a_follower_that_loses_all_state_still_catches_up(bus: InMemoryBus) -> None:
    """nextIndex backoff has to walk all the way to the sentinel for a log that isn't
    just behind, but entirely empty - proving log repair doesn't assume partial state.
    """
    nodes = build_cluster(bus, 3)
    await run_cluster(nodes)
    ids = [n.node_id for n in nodes]

    try:
        leader = leaders(nodes)[0]
        follower = next(n for n in nodes if n is not leader)

        await follower.stop()  # the crash: this object is discarded entirely
        for i in range(15):
            leader.submit(put(f"k{i}", i))
        assert await wait_until(lambda: leader.state.commit_index >= 15)

        replacement = hard_crash_and_replace(bus, follower, ids)
        nodes = [n if n is not follower else replacement for n in nodes]
        replacement.start()

        assert await wait_until(
            lambda: (
                replacement.state.log.last_index == leader.state.log.last_index
                and replacement.state.commit_index == leader.state.commit_index
            )
        )
        assert committed_commands(replacement) == committed_commands(leader)
        assert replacement.role is not Role.LEADER  # it never had grounds to campaign
    finally:
        await shutdown(nodes)


async def test_a_leader_that_hard_crashes_rejoins_as_a_caught_up_follower(
    bus: InMemoryBus,
) -> None:
    nodes = build_cluster(bus, 3)
    await run_cluster(nodes)
    ids = [n.node_id for n in nodes]
    alive = list(nodes)

    try:
        original = leaders(nodes)[0]
        for i in range(10):
            original.submit(put(f"before{i}", i))
        assert await wait_until(lambda: original.state.commit_index >= 10)

        await original.stop()
        alive = [n for n in nodes if n is not original]
        assert await wait_until(lambda: len(leaders(alive)) == 1)

        new_leader = leaders(alive)[0]
        for i in range(10):
            new_leader.submit(put(f"after{i}", i))
        assert await wait_until(lambda: new_leader.state.commit_index >= 20)

        replacement = hard_crash_and_replace(bus, original, ids)
        alive.append(replacement)
        replacement.start()

        assert await wait_until(
            lambda: replacement.state.commit_index == new_leader.state.commit_index
        )
        assert committed_commands(replacement) == committed_commands(new_leader)
        # Nothing committed under the term it used to lead was lost.
        for i in range(10):
            assert put(f"before{i}", i) in committed_commands(replacement)
    finally:
        await shutdown(alive)


# --- majority failure: no leader, no progress, until enough nodes are back ---------


async def test_losing_a_majority_makes_the_cluster_unavailable(bus: InMemoryBus) -> None:
    """Safety over liveness: with only 2 of 5 nodes reachable - and the leader not one
    of them - nobody may lead. (A leader that is itself among the survivors would keep
    believing it leads, per §5.2: nothing here makes it self-demote just for lacking a
    quorum to commit to - that would need a lease of its own, out of this test's scope.)
    """
    nodes = build_cluster(bus, 5)
    await run_cluster(nodes)
    alive = list(nodes)

    try:
        leader = leaders(nodes)[0]
        for i in range(5):
            leader.submit(put(f"k{i}", i))
        # Wait for the *followers* to learn the commit index too, not just the leader:
        # they learn it one AppendEntries after receiving the entries, and it is their
        # committed prefix this test is about to assert survives the outage. The target
        # is the leader's last *appended* index - its commit index is still behind that
        # at this point, so waiting on it would be trivially satisfied at once.
        target = leader.state.log.last_index
        assert await wait_until(lambda: all(n.state.commit_index >= target for n in nodes))
        committed_before = committed_commands(leader)

        survivors = [n for n in nodes if n is not leader][:2]  # 2 followers, no leader
        crashed = [n for n in nodes if n not in survivors]  # the leader + 2 followers
        for node in crashed:
            await node.stop()
        alive = survivors

        assert len(alive) == 2
        # Poll the full settle window: if a leader were ever going to emerge from a
        # minority, it would have by now, and wait_until would have returned True.
        assert not await wait_until(lambda: len(leaders(alive)) > 0, timeout=SETTLE)
        assert leaders(alive) == []
        # Nothing committed before the majority vanished was lost or changed.
        for node in alive:
            assert committed_commands(node)[: len(committed_before)] == committed_before
    finally:
        await shutdown(alive)


async def test_the_cluster_recovers_once_a_majority_is_reachable_again(bus: InMemoryBus) -> None:
    nodes = build_cluster(bus, 5)
    await run_cluster(nodes)
    alive = list(nodes)

    try:
        leader = leaders(nodes)[0]
        followers = [n for n in nodes if n is not leader]
        crashed = [leader, *followers[:2]]  # 3 of 5 down, including the leader

        for node in crashed:
            await node.stop()
        alive = [n for n in nodes if n not in crashed]
        assert len(alive) == 2
        assert not await wait_until(lambda: len(leaders(alive)) > 0, timeout=SETTLE)

        # One of the crashed followers rejoins: 3 of 5 reachable again, a majority.
        # (Not the old leader - a clean restart of that one would resume believing it
        # still leads, which is a different scenario from the one this test is after.)
        rejoined = crashed[1]
        rejoined.start()
        alive.append(rejoined)

        assert await wait_until(lambda: len(leaders(alive)) == 1)
        new_leader = leaders(alive)[0]
        new_leader.submit(put("recovered", True))
        assert await wait_until(lambda: new_leader.state.commit_index >= 1)
    finally:
        # The old leader and the other crashed follower were never restarted.
        await shutdown(alive)


async def test_the_cluster_recovers_repeatedly_across_hard_crashes(bus: InMemoryBus) -> None:
    """Churn (Day 7), but with real state loss each time rather than a clean stop."""
    nodes = build_cluster(bus, 5)
    await run_cluster(nodes)
    ids = [n.node_id for n in nodes]
    alive = list(nodes)

    try:
        for _ in range(2):
            leader = leaders(alive)[0]
            await leader.stop()
            replacement = hard_crash_and_replace(bus, leader, ids)
            # Mutated in place rather than rebound: the predicate below closes over
            # this list, and a fresh list each round would leave it reading a stale one.
            alive.remove(leader)
            alive.append(replacement)
            replacement.start()

            assert await wait_until(lambda: len(leaders(alive)) == 1)  # 5 then 5: still quorum
    finally:
        await shutdown(alive)
