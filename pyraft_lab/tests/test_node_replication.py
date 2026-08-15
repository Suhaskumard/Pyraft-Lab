"""Phase 1 acceptance, replication half: real clusters replicating real commands.

Covers the plan's remaining Phase 1 criteria - 100 commands replicate consistently,
conflicting follower logs are repaired, and no committed entry is ever replaced at the
same index.
"""

from __future__ import annotations

from typing import Any

import pytest
from tests.test_node_election import (
    build_cluster,
    leaders,
    run_cluster,
    shutdown,
    wait_until,
)

from pyraft_lab.kv.commands import Put
from pyraft_lab.network import InMemoryBus
from pyraft_lab.raft.log import LogEntry
from pyraft_lab.raft.node import RaftNode
from pyraft_lab.raft.replication import NotLeader
from pyraft_lab.raft.state import Role

REPLICATE = 0.25  # long enough for several heartbeat rounds to push and confirm


def put(key: str, value: Any) -> dict[str, Any]:
    """A PUT in the wire form the log carries."""
    return Put(key=key, value=value).to_wire()


@pytest.fixture
def bus() -> InMemoryBus:
    return InMemoryBus()


def committed_commands(node: RaftNode) -> list[object]:
    """Every command the node considers committed, in log order."""
    return [
        node.state.log.entry_at(i).command  # type: ignore[union-attr]
        for i in range(1, node.state.commit_index + 1)
    ]


async def test_a_command_replicates_to_every_follower(bus: InMemoryBus) -> None:
    nodes = build_cluster(bus, 3)
    await run_cluster(nodes)

    try:
        leader = leaders(nodes)[0]
        leader.submit(put("a", 1))
        assert await wait_until(lambda: all(n.state.log.last_index >= 1 for n in nodes))

        for node in nodes:
            assert node.state.log.last_index >= 1
            assert node.state.log.entry_at(1).command == put("a", 1)
    finally:
        await shutdown(nodes)


async def test_a_command_commits_once_a_majority_holds_it(bus: InMemoryBus) -> None:
    nodes = build_cluster(bus, 3)
    await run_cluster(nodes)

    try:
        leader = leaders(nodes)[0]
        index = leader.submit(put("x", 1))
        assert await wait_until(lambda: all(n.state.commit_index >= index for n in nodes))

        assert leader.state.commit_index >= index
        # Followers learn the commit index on the next AppendEntries.
        for node in nodes:
            assert node.state.commit_index >= index
    finally:
        await shutdown(nodes)


async def test_one_hundred_commands_replicate_consistently(bus: InMemoryBus) -> None:
    """The plan's Phase 1 criterion, verbatim."""
    nodes = build_cluster(bus, 3)
    await run_cluster(nodes)

    try:
        leader = leaders(nodes)[0]
        for i in range(100):
            leader.submit(put(f"k{i}", i))
        # Followers learn the commit index one AppendEntries after receiving the
        # entries, so waiting on log length alone would race that second round.
        assert await wait_until(lambda: all(n.state.commit_index == 100 for n in nodes))

        assert leader.state.commit_index == 100
        expected = [put(f"k{i}", i) for i in range(100)]

        for node in nodes:
            assert node.state.log.last_index == 100
            assert committed_commands(node) == expected
    finally:
        await shutdown(nodes)


async def test_every_node_agrees_entry_for_entry(bus: InMemoryBus) -> None:
    """Log matching: same index and term implies the same command everywhere."""
    nodes = build_cluster(bus, 5)
    await run_cluster(nodes)

    try:
        leader = leaders(nodes)[0]
        for i in range(30):
            leader.submit(put(f"k{i}", i))
        assert await wait_until(lambda: all(n.state.log.last_index == 30 for n in nodes))

        reference = leader.state.log
        for node in nodes:
            for index in range(1, reference.last_index + 1):
                mine = node.state.log.entry_at(index)
                theirs = reference.entry_at(index)
                if mine is not None:
                    assert (mine.term, mine.command) == (theirs.term, theirs.command)
    finally:
        await shutdown(nodes)


async def test_a_conflicting_follower_log_is_repaired(bus: InMemoryBus) -> None:
    """A follower that starts with divergent entries has them truncated and refilled.

    The divergence is seeded at an *older term* deliberately. Raft's Log Matching
    Property means two logs can never hold different commands at the same index *and*
    term - one term has at most one leader, so that entry has exactly one author. Real
    divergence is therefore always a term difference: entries a previous leader
    appended but never got committed before losing its term.
    """
    nodes = build_cluster(bus, 3)

    rogue = nodes[2]
    rogue.state.log.append(LogEntry(term=1, index=1, command=put("stale", 1)))
    rogue.state.log.append(LogEntry(term=1, index=2, command=put("stale", 2)))

    # Start everyone well past that term, so whoever wins appends at a higher term and
    # the mismatch is visible at index 1.
    for node in nodes:
        node.state.current_term = 5

    await run_cluster(nodes)

    try:
        leader = leaders(nodes)[0]
        if leader is rogue:
            pytest.skip("the rogue node won this election; its log is not the stale one")

        for i in range(5):
            leader.submit(put(f"good{i}", i))
        assert await wait_until(lambda: rogue.state.log.last_index == leader.state.log.last_index)

        # The rogue's fabricated entries are gone, replaced by the leader's.
        assert rogue.state.log.last_index == leader.state.log.last_index
        for index in range(1, leader.state.log.last_index + 1):
            assert rogue.state.log.entry_at(index).command == (
                leader.state.log.entry_at(index).command
            )
    finally:
        await shutdown(nodes)


async def test_a_committed_entry_is_never_replaced_at_the_same_index(bus: InMemoryBus) -> None:
    """State-machine safety, checked across a leader crash and re-election."""
    nodes = build_cluster(bus, 5)
    await run_cluster(nodes)
    alive = list(nodes)

    try:
        leader = leaders(nodes)[0]
        for i in range(10):
            leader.submit(put(f"before{i}", i))
        assert await wait_until(lambda: leader.state.commit_index >= 10)

        # Snapshot what was committed before the crash.
        committed_before = {
            i: leader.state.log.entry_at(i).command for i in range(1, leader.state.commit_index + 1)
        }
        assert committed_before, "expected entries to commit before the crash"

        await leader.stop()
        alive = [n for n in nodes if n is not leader]
        assert await wait_until(lambda: len(leaders(alive)) == 1)

        new_leader = leaders(alive)[0]
        for i in range(10):
            new_leader.submit(put(f"after{i}", i))
        assert await wait_until(lambda: new_leader.state.commit_index >= 20)

        # Nothing committed under the old leader changed under the new one.
        for index, command in committed_before.items():
            assert new_leader.state.log.entry_at(index).command == command
    finally:
        await shutdown(alive)


async def test_a_follower_refuses_to_accept_client_commands(bus: InMemoryBus) -> None:
    nodes = build_cluster(bus, 3)
    await run_cluster(nodes)

    try:
        follower = next(n for n in nodes if n.role is not Role.LEADER)
        with pytest.raises(NotLeader):
            follower.submit(put("nope", 1))
    finally:
        await shutdown(nodes)


async def test_a_restarted_follower_catches_up_from_scratch(bus: InMemoryBus) -> None:
    """nextIndex backoff has to walk a far-behind follower all the way back."""
    nodes = build_cluster(bus, 3)
    await run_cluster(nodes)

    try:
        leader = leaders(nodes)[0]
        laggard = next(n for n in nodes if n is not leader)

        await laggard.stop()  # miss everything that follows
        for i in range(20):
            leader.submit(put(f"missed{i}", i))
        assert await wait_until(lambda: leader.state.commit_index >= 20)

        laggard.start()  # rejoin
        assert await wait_until(
            lambda: (
                laggard.state.log.last_index == leader.state.log.last_index
                and laggard.state.commit_index == leader.state.commit_index
            )
        )

        assert laggard.state.log.last_index == leader.state.log.last_index
        assert committed_commands(laggard) == committed_commands(leader)
    finally:
        await shutdown(nodes)
