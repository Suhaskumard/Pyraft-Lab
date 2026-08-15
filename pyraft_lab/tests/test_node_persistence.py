"""Phase 6 at the node level: a restart that remembers, and one that snapshots.

Phase 3's ``test_failure_recovery.py`` crashes nodes and brings them back with *nothing* -
which is what a node without a WAL genuinely has. These tests are the other half: the
same crashes, with the WAL in place, asserting that what comes back is what went down.

The two properties that matter are safety properties, not conveniences:

- a node that voted in term T and then crashed must come back having voted in term T,
  or it can vote twice in one term and elect two leaders;
- nothing this node ever told a peer or a client can be forgotten, which is why the
  assertions read the file from disk *while the node is still running* rather than
  after a tidy shutdown.
"""

from __future__ import annotations

import random
from pathlib import Path

import pytest
from tests.test_node_election import (
    FAST_ELECTION,
    FAST_HEARTBEAT,
    build_cluster,
    leaders,
    run_cluster,
    shutdown,
    wait_until,
)

from pyraft_lab.kv.commands import Put
from pyraft_lab.network import InMemoryBus
from pyraft_lab.raft.node import RaftNode
from pyraft_lab.raft.persistence import WalError, WriteAheadLog, read_wal
from pyraft_lab.raft.rpc import RequestVote, handle_request_vote
from pyraft_lab.raft.snapshot import SnapshotError, SnapshotPolicy, SnapshotStore
from pyraft_lab.raft.state import Role


@pytest.fixture
def bus() -> InMemoryBus:
    return InMemoryBus()


def put(key: str, value: object) -> dict[str, object]:
    return Put(key=key, value=value).to_wire()


def wal_for(tmp_path: Path, node_id: str) -> WriteAheadLog:
    return WriteAheadLog(tmp_path / f"{node_id}.wal", node_id=node_id, sync=False)


def persistent_node(
    bus: InMemoryBus,
    tmp_path: Path,
    node_id: str,
    ids: list[str],
    *,
    snapshots: SnapshotStore | None = None,
    snapshot_policy: SnapshotPolicy | None = None,
) -> RaftNode:
    """A node built on the WAL at ``tmp_path/<id>.wal`` - a fresh one, or the one a
    previous incarnation left behind. A restart is exactly this call again.
    """
    return RaftNode(
        node_id=node_id,
        peers=ids,
        transport=bus,
        rng=random.Random(hash(node_id) & 0xFFFF),
        wal=wal_for(tmp_path, node_id),
        snapshots=snapshots,
        snapshot_policy=snapshot_policy,
        election_timeout_range=FAST_ELECTION,
        heartbeat_interval=FAST_HEARTBEAT,
    )


def committed_commands(node: RaftNode) -> list[object]:
    log = node.state.log
    return [
        entry.command
        for index in range(log.base_index + 1, node.state.commit_index + 1)
        if (entry := log.entry_at(index)) is not None and entry.command is not None
    ]


async def caught_up(node: RaftNode) -> bool:
    return await wait_until(lambda: node.state.commit_index == node.state.log.last_index)


# --- a single node, so the assertions are about persistence and nothing else -------


async def test_a_node_comes_back_with_its_term_vote_log_and_state(
    bus: InMemoryBus, tmp_path: Path
) -> None:
    node = persistent_node(bus, tmp_path, "n1", ["n1"])
    node.start()
    assert await wait_until(lambda: node.role is Role.LEADER)
    for i in range(5):
        node.submit(put(f"k{i}", i))
    assert await caught_up(node)

    term, commit, applied = node.current_term, node.state.commit_index, node.state.last_applied
    await node.stop()

    revived = persistent_node(bus, tmp_path, "n1", ["n1"])
    try:
        assert revived.current_term == term
        assert revived.state.voted_for == "n1"  # it voted for itself to win that term
        assert revived.state.commit_index == commit
        assert revived.state.last_applied == applied
        assert revived.store.read("k3") == 3
        assert len(revived.store) == 5
        assert committed_commands(revived) == [put(f"k{i}", i) for i in range(5)]
    finally:
        await revived.stop()


async def test_what_a_client_was_told_is_already_on_disk(bus: InMemoryBus, tmp_path: Path) -> None:
    """Read from the file while the node is still running: durability before disclosure
    is a claim about the moment of the reply, not about shutdown.
    """
    node = persistent_node(bus, tmp_path, "n1", ["n1"])
    node.start()
    assert await wait_until(lambda: node.role is Role.LEADER)

    try:
        node.submit(put("durable", True))
        assert await caught_up(node)

        assert node.wal is not None
        assert node.wal.syncs > 0
        on_disk = read_wal(node.wal.path)
        assert put("durable", True) in [e.command for e in on_disk.entries]
        assert on_disk.commit_index >= node.state.commit_index
        assert on_disk.term == node.current_term
    finally:
        await node.stop()


async def test_a_restart_recovers_the_prefix_a_torn_write_left_behind(
    bus: InMemoryBus, tmp_path: Path
) -> None:
    """2.0 item 9's explicit edge case, at the node level: the process died partway
    through an append, and what comes back is everything up to that point.
    """
    node = persistent_node(bus, tmp_path, "n1", ["n1"])
    node.start()
    assert await wait_until(lambda: node.role is Role.LEADER)
    for i in range(4):
        node.submit(put(f"k{i}", i))
    assert await caught_up(node)
    await node.stop()

    path = tmp_path / "n1.wal"
    with path.open("a", encoding="utf-8", newline="") as handle:
        handle.write('a1b2c3d4 {"kind":"entry","term":1,"in')  # a crash mid-append

    revived = persistent_node(bus, tmp_path, "n1", ["n1"])
    try:
        assert revived.recovery is not None
        assert revived.recovery.truncated_tail
        assert len(revived.store) == 4
        assert revived.store.read("k2") == 2

        # And it is a working node afterwards, not just a readable one.
        revived.start()
        assert await wait_until(lambda: revived.role is Role.LEADER)
        revived.submit(put("after", "recovery"))
        assert await caught_up(revived)
        assert revived.store.read("after") == "recovery"
    finally:
        await revived.stop()


async def test_a_node_without_a_wal_is_exactly_what_it_was_before_this_phase(
    bus: InMemoryBus,
) -> None:
    node = RaftNode("n1", ["n1"], bus, election_timeout_range=FAST_ELECTION)

    assert node.wal is None
    assert node.recovery is None
    assert node.state.log.journal.record_append(None) is None  # type: ignore[arg-type]
    assert node.state.log.base_index == 0


# --- the safety property: a recovered vote is still binding ------------------------


def test_a_recovered_node_will_not_vote_twice_in_the_same_term(
    bus: InMemoryBus, tmp_path: Path
) -> None:
    """The reason ``votedFor`` is persistent state at all. Without the WAL this node
    would wake up with no vote recorded and grant a second one in the same term.
    """
    wal = wal_for(tmp_path, "n1")
    wal.recover()
    wal.record_term(5, "n2")
    wal.sync()
    wal.close()

    revived = persistent_node(bus, tmp_path, "n1", ["n1", "n2", "n3"])

    assert (revived.current_term, revived.state.voted_for) == (5, "n2")
    reply = handle_request_vote(
        revived.state,
        RequestVote(term=5, candidate_id="n3", last_log_index=0, last_log_term=0),
    )
    assert not reply.vote_granted


def test_a_wal_from_another_node_is_refused_rather_than_adopted(
    bus: InMemoryBus, tmp_path: Path
) -> None:
    wal = wal_for(tmp_path, "n1")
    wal.recover()
    wal.close()

    with pytest.raises(WalError, match="belongs to n1"):
        RaftNode("n2", ["n1", "n2"], bus, wal=WriteAheadLog(tmp_path / "n1.wal", sync=False))


def test_a_compacted_wal_without_its_snapshot_store_is_refused(
    bus: InMemoryBus, tmp_path: Path
) -> None:
    """Rather than starting from an empty state machine, which would look like a working
    node that has silently lost every key the snapshot held.
    """
    from pyraft_lab.raft.snapshot import Snapshot

    wal = wal_for(tmp_path, "n1")
    wal.recover()
    snapshot = Snapshot.create(
        last_included_index=4, last_included_term=1, data={"a": 1}, node_id="n1"
    )
    wal.compact(snapshot=snapshot.meta, term=1, voted_for="n1", entries=[], commit_index=4)
    wal.close()

    with pytest.raises(SnapshotError, match="no snapshot store"):
        persistent_node(bus, tmp_path, "n1", ["n1"])


# --- a real cluster ---------------------------------------------------------------


async def test_a_hard_crash_with_a_wal_rejoins_with_its_log_intact(
    bus: InMemoryBus, tmp_path: Path
) -> None:
    """The same crash as Phase 3's ``hard_crash_and_replace``, except that this node kept
    its WAL - so the replacement starts from its own committed prefix rather than from
    nothing, and needs no repair from the leader to hold it.
    """
    ids = ["n1", "n2", "n3"]
    nodes = [persistent_node(bus, tmp_path, node_id, ids) for node_id in ids]
    await run_cluster(nodes)
    alive = list(nodes)

    try:
        leader = leaders(nodes)[0]
        for i in range(12):
            leader.submit(put(f"k{i}", i))
        target = leader.state.log.last_index
        assert await wait_until(lambda: all(n.state.commit_index >= target for n in nodes))

        victim = next(n for n in nodes if n is not leader)
        expected = committed_commands(victim)
        await victim.stop()
        alive.remove(victim)

        replacement = persistent_node(bus, tmp_path, victim.node_id, ids)
        # Before it is even started: the log came off the disk, not off the leader.
        assert committed_commands(replacement) == expected
        assert replacement.state.log.last_index >= target

        alive.append(replacement)
        replacement.start()
        assert await wait_until(lambda: replacement.state.commit_index >= target)
        assert replacement.store.read("k11") == 11
    finally:
        await shutdown(alive)


async def test_a_cluster_of_persistent_nodes_still_elects_and_replicates(
    bus: InMemoryBus, tmp_path: Path
) -> None:
    """Persistence must not change the algorithm's behaviour - only its memory."""
    ids = ["n1", "n2", "n3"]
    nodes = [persistent_node(bus, tmp_path, node_id, ids) for node_id in ids]
    await run_cluster(nodes)

    try:
        assert len(leaders(nodes)) == 1
        leader = leaders(nodes)[0]
        for i in range(20):
            leader.submit(put(f"k{i}", i))
        target = leader.state.log.last_index
        assert await wait_until(lambda: all(n.state.commit_index >= target for n in nodes))

        first = committed_commands(nodes[0])
        assert all(committed_commands(node) == first for node in nodes)
        # Every node's WAL agrees with the log it is holding in memory.
        for node in nodes:
            assert node.wal is not None
            recovery = read_wal(node.wal.path)
            assert recovery.last_index == node.state.log.last_index
    finally:
        await shutdown(nodes)


async def test_a_node_without_persistence_is_unaffected_by_one_that_has_it(
    bus: InMemoryBus,
) -> None:
    """The mixed cluster, because ``wal=None`` is the default everywhere else in the
    suite and Phase 6 must not have made it a special case.
    """
    nodes = build_cluster(bus, 3)
    await run_cluster(nodes)

    try:
        leader = leaders(nodes)[0]
        leader.submit(put("k", "v"))
        assert await wait_until(lambda: leader.state.commit_index == leader.state.log.last_index)
        assert all(node.wal is None for node in nodes)
    finally:
        await shutdown(nodes)


# --- snapshots, end to end -------------------------------------------------------


async def test_the_log_growth_threshold_takes_a_snapshot_and_compacts_the_wal(
    bus: InMemoryBus, tmp_path: Path
) -> None:
    store = SnapshotStore(tmp_path / "snapshots", sync=False)
    node = persistent_node(
        bus,
        tmp_path,
        "n1",
        ["n1"],
        snapshots=store,
        snapshot_policy=SnapshotPolicy(entry_threshold=5),
    )
    node.start()
    assert await wait_until(lambda: node.role is Role.LEADER)

    try:
        for i in range(12):
            node.submit(put(f"k{i}", i))
        assert await caught_up(node)

        assert node.wal is not None
        assert node.wal.compactions > 0
        latest = store.load_latest()
        assert latest is not None
        # A snapshot captures the state as of the index it names, which is behind the
        # live state by however much has been applied since - so it is a prefix of it,
        # never a copy of it. Entry 1 is the no-op a new leader commits, hence the -1.
        assert 0 < latest.meta.last_included_index <= node.state.last_applied
        assert latest.meta.keys == latest.meta.last_included_index - 1
        assert latest.data.items() <= node.store.snapshot().items()

        # The WAL now starts at the snapshot rather than at the beginning of time.
        recovery = read_wal(node.wal.path)
        assert recovery.snapshot is not None
        assert recovery.base_index == latest.meta.last_included_index
        assert recovery.last_index == node.state.log.last_index
    finally:
        await node.stop()


async def test_a_restart_from_a_compacted_wal_rebuilds_the_state_machine(
    bus: InMemoryBus, tmp_path: Path
) -> None:
    store = SnapshotStore(tmp_path / "snapshots", sync=False)
    policy = SnapshotPolicy(entry_threshold=4)
    node = persistent_node(bus, tmp_path, "n1", ["n1"], snapshots=store, snapshot_policy=policy)
    node.start()
    assert await wait_until(lambda: node.role is Role.LEADER)
    for i in range(15):
        node.submit(put(f"k{i}", i))
    assert await caught_up(node)
    expected_state = node.store.snapshot()
    commit = node.state.commit_index
    await node.stop()

    revived = persistent_node(bus, tmp_path, "n1", ["n1"], snapshots=store, snapshot_policy=policy)
    try:
        assert revived.recovery is not None
        assert revived.recovery.snapshot is not None
        assert revived.store.snapshot() == expected_state
        assert revived.state.commit_index == commit
        # The recovered log starts at the snapshot: the entries below it are genuinely
        # gone, and the base stands in for them in the consistency check.
        assert revived.state.log.base_index == revived.recovery.snapshot.last_included_index
        assert revived.state.log.last_index == commit

        revived.start()
        assert await wait_until(lambda: revived.role is Role.LEADER)
        revived.submit(put("after", "restart"))
        assert await caught_up(revived)
        assert revived.store.read("after") == "restart"
        assert revived.store.read("k14") == 14
    finally:
        await revived.stop()


async def test_a_leader_recovered_from_a_snapshot_can_still_catch_a_follower_up(
    bus: InMemoryBus, tmp_path: Path
) -> None:
    """The case the missing InstallSnapshot would otherwise have taken with it. The
    leader's log starts at a base; the follower is behind but holds the committed prefix,
    so the base is enough to re-establish agreement.
    """
    ids = ["n1", "n2", "n3"]
    policy = SnapshotPolicy(entry_threshold=4)
    # A store per node, because in a real deployment each node's snapshots are its own.
    nodes = {
        node_id: persistent_node(
            bus,
            tmp_path,
            node_id,
            ids,
            snapshots=SnapshotStore(tmp_path / f"snapshots-{node_id}", sync=False),
            snapshot_policy=policy,
        )
        for node_id in ids
    }
    alive = list(nodes.values())
    await run_cluster(alive)

    try:
        leader = leaders(alive)[0]
        for i in range(20):
            leader.submit(put(f"k{i}", i))
        target = leader.state.log.last_index
        assert await wait_until(lambda: all(n.state.commit_index >= target for n in alive))

        # Take the leader down and bring it back from its own compacted WAL.
        assert leader.wal is not None
        assert leader.wal.compactions > 0
        await leader.stop()
        alive.remove(leader)

        revived = persistent_node(
            bus,
            tmp_path,
            leader.node_id,
            ids,
            snapshots=SnapshotStore(tmp_path / f"snapshots-{leader.node_id}", sync=False),
            snapshot_policy=policy,
        )
        assert revived.state.log.base_index > 0
        alive.append(revived)
        revived.start()

        # Somebody leads again, and every survivor agrees on the whole committed prefix.
        assert await wait_until(lambda: len(leaders(alive)) == 1)
        current = leaders(alive)[0]
        current.submit(put("post-recovery", True))
        final = current.state.log.last_index
        assert await wait_until(lambda: all(n.state.commit_index >= final for n in alive))
        assert all(node.store.read("k19") == 19 for node in alive)
        assert all(node.store.read("post-recovery") is True for node in alive)
    finally:
        await shutdown(alive)


async def test_taking_a_snapshot_by_hand_is_a_no_op_when_nothing_is_new(
    bus: InMemoryBus, tmp_path: Path
) -> None:
    store = SnapshotStore(tmp_path / "snapshots", sync=False)
    node = persistent_node(
        bus,
        tmp_path,
        "n1",
        ["n1"],
        snapshots=store,
        snapshot_policy=SnapshotPolicy(entry_threshold=0),
    )
    node.start()
    assert await wait_until(lambda: node.role is Role.LEADER)

    try:
        node.submit(put("a", 1))
        assert await caught_up(node)

        first = node.take_snapshot()
        assert first is not None
        assert node.take_snapshot() is None  # nothing applied since
        assert [m.last_included_index for m in store.metas()] == [first.last_included_index]
    finally:
        await node.stop()


async def test_a_node_with_no_snapshot_store_never_snapshots(
    bus: InMemoryBus, tmp_path: Path
) -> None:
    node = persistent_node(bus, tmp_path, "n1", ["n1"], snapshot_policy=SnapshotPolicy(1))
    node.start()
    assert await wait_until(lambda: node.role is Role.LEADER)

    try:
        for i in range(6):
            node.submit(put(f"k{i}", i))
        assert await caught_up(node)

        assert node.take_snapshot() is None
        assert node.wal is not None
        assert node.wal.compactions == 0
        assert read_wal(node.wal.path).snapshot is None
    finally:
        await node.stop()
