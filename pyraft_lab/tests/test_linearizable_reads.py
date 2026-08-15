"""Day 10: reads that cannot serve stale data.

A ``Get`` never enters the log. ``RaftNode`` answers it from the local ``KVStore``
only once two things hold: a majority has acknowledged an AppendEntries from this
leader recently enough that none of them can yet have voted for a challenger (the
lease), and the state machine has applied everything that was committed when the read
arrived (the read index). These tests are about what happens when either half is
missing.
"""

from __future__ import annotations

import random

import pytest
from tests.test_node_election import leaders, wait_until
from tests.test_partitions import build_cluster, run_cluster, shutdown

from pyraft_lab.client.client import BusChannel, KVClient, RequestFailed, RetryPolicy
from pyraft_lab.kv.commands import Put
from pyraft_lab.network.simulator import SimulatedNetwork
from pyraft_lab.raft.state import Role

FAST_RETRY = RetryPolicy(max_attempts=8, initial_backoff=0.002, max_backoff=0.02)


def put(key: str, value: object) -> dict[str, object]:
    return Put(key=key, value=value).to_wire()


@pytest.fixture
def net() -> SimulatedNetwork:
    return SimulatedNetwork(rng=random.Random(0))


def connect(net: SimulatedNetwork, nodes: list, name: str = "client") -> KVClient:
    channel = BusChannel(net, name, timeout=0.15)
    return KVClient(name, [n.node_id for n in nodes], channel, policy=FAST_RETRY)


def disconnect(client: KVClient) -> None:
    """Leave the transport, so the next test's ``register`` of the same id is clean."""
    client._submit.close()


# --- the happy path -----------------------------------------------------------------


async def test_a_read_returns_the_latest_committed_write(net: SimulatedNetwork) -> None:
    nodes = build_cluster(net, 3)
    await run_cluster(nodes)
    client = connect(net, nodes)

    try:
        await client.put("k", "v1")
        assert await client.get("k") == "v1"
        await client.put("k", "v2")
        assert await client.get("k") == "v2"
    finally:
        await shutdown(nodes)


async def test_a_read_never_appends_to_the_log(net: SimulatedNetwork) -> None:
    """The whole point of the lease: a read costs no log entry and no replication."""
    nodes = build_cluster(net, 3)
    await run_cluster(nodes)
    client = connect(net, nodes)

    try:
        await client.put("k", 1)
        leader = leaders(nodes)[0]
        length_before = leader.state.log.last_index

        for _ in range(10):
            assert await client.get("k") == 1

        assert leader.state.log.last_index == length_before
    finally:
        await shutdown(nodes)


async def test_reading_an_absent_key_returns_none(net: SimulatedNetwork) -> None:
    nodes = build_cluster(net, 3)
    await run_cluster(nodes)
    client = connect(net, nodes)

    try:
        assert await client.get("never-written") is None
    finally:
        await shutdown(nodes)


async def test_a_follower_redirects_a_read_rather_than_serving_it(
    net: SimulatedNetwork,
) -> None:
    """A follower's store can be arbitrarily stale, so it must never answer a read."""
    nodes = build_cluster(net, 3)
    await run_cluster(nodes)
    client = connect(net, nodes)

    try:
        await client.put("k", "v")
        leader = leaders(nodes)[0]
        follower = next(n for n in nodes if n is not leader)

        client.leader_hint = follower.node_id  # aim the next read at the wrong node
        assert await client.get("k") == "v"  # served only after the redirect
        assert client.metrics.redirects >= 1
        assert client.leader_hint == leader.node_id
    finally:
        await shutdown(nodes)


# --- the lease half: a leader that cannot prove it still leads must not answer -------


async def test_a_partitioned_leader_refuses_to_serve_reads(net: SimulatedNetwork) -> None:
    """The anomaly the lease exists to prevent: a leader cut off from its cluster still
    *believes* it leads and still holds a plausible-looking value in its store. Without
    a lease it would happily serve that value while a new leader elsewhere has already
    moved on - a stale read, and a linearizability violation.

    The client is deliberately left on the *stale leader's* side of the split, so it
    can still reach the one node that would give a wrong answer, and only that node.
    Failing is the correct outcome here; answering would be the bug.
    """
    nodes = build_cluster(net, 5)
    await run_cluster(nodes)
    client = connect(net, nodes)

    try:
        await client.put("k", "before")
        leader = leaders(nodes)[0]
        others = [n for n in nodes if n is not leader]

        net.partition({leader.node_id, "client"}, {n.node_id for n in others})
        client.leader_hint = leader.node_id  # keep asking the node that cannot answer

        # Wait for the majority side to elect someone. A read served *before* this
        # point would still be correct - the old leader's lease had not expired, and
        # the lease's whole guarantee is that no new leader can appear while it holds.
        # Once a new leader exists, that guarantee is spent and the old one must refuse.
        assert await wait_until(lambda: len(leaders(others)) == 1)

        with pytest.raises(RequestFailed):
            await client.get("k")

        assert leader.role is Role.LEADER  # it never learned it had been deposed
    finally:
        net.heal()
        await shutdown(nodes)
        disconnect(client)


async def test_a_read_is_answered_by_the_new_leader_after_a_partition(
    net: SimulatedNetwork,
) -> None:
    """The liveness other half: the majority side elects someone who *can* prove it
    leads, and the client's read is served there rather than failing outright.
    """
    nodes = build_cluster(net, 5)
    await run_cluster(nodes)
    client = connect(net, nodes)

    try:
        await client.put("k", "before")
        old_leader = leaders(nodes)[0]
        others = [n for n in nodes if n is not old_leader]

        net.partition({old_leader.node_id}, {n.node_id for n in others} | {"client"})
        assert await wait_until(lambda: len(leaders(others)) == 1)

        assert await client.get("k") == "before"
        assert client.leader_hint == leaders(others)[0].node_id
    finally:
        net.heal()
        await shutdown(nodes)
        disconnect(client)


async def test_a_read_sees_a_write_that_committed_under_a_previous_leader(
    net: SimulatedNetwork,
) -> None:
    """The read-index half, and what ``_win_election``'s no-op is for: a brand new
    leader's commit index starts frozen on an older-term entry that §5.4.2 forbids it
    from counting a majority on. Without the no-op, an idle cluster that only ever
    serves reads after a failover could sit behind its own committed state forever.
    """
    nodes = build_cluster(net, 5)
    await run_cluster(nodes)
    client = connect(net, nodes)
    alive = list(nodes)

    try:
        await client.put("k", "committed-before-the-crash")
        leader = leaders(nodes)[0]

        await leader.stop()
        alive = [n for n in nodes if n is not leader]
        assert await wait_until(lambda: len(leaders(alive)) == 1)

        # No writes at all after the failover - only a read.
        assert await client.get("k") == "committed-before-the-crash"
    finally:
        await shutdown(alive)
        disconnect(client)


async def test_a_new_leader_applies_everything_before_answering(
    net: SimulatedNetwork,
) -> None:
    nodes = build_cluster(net, 5)
    await run_cluster(nodes)
    client = connect(net, nodes)
    alive = list(nodes)

    try:
        for i in range(20):
            await client.put(f"k{i}", i)
        leader = leaders(nodes)[0]

        await leader.stop()
        alive = [n for n in nodes if n is not leader]
        assert await wait_until(lambda: len(leaders(alive)) == 1)

        new_leader = leaders(alive)[0]
        for i in range(20):
            assert await client.get(f"k{i}") == i
        # Everything committed under the old term is applied under the new one.
        assert new_leader.state.last_applied >= 20
    finally:
        await shutdown(alive)
        disconnect(client)


# --- reads under a real workload -----------------------------------------------------


async def test_a_read_never_goes_backward_in_time(net: SimulatedNetwork) -> None:
    """Monotonicity, the property a stale read would break: once a client has observed
    a value, no later read of that key may return an earlier one.
    """
    nodes = build_cluster(net, 5)
    await run_cluster(nodes)
    client = connect(net, nodes)

    try:
        highest_seen = -1
        for i in range(15):
            await client.put("counter", i)
            observed = await client.get("counter")
            assert observed >= highest_seen
            highest_seen = observed
        assert highest_seen == 14
    finally:
        await shutdown(nodes)
        disconnect(client)


async def test_reads_stay_correct_under_latency_and_loss(net: SimulatedNetwork) -> None:
    nodes = build_cluster(net, 3)
    await run_cluster(nodes)
    client = connect(net, nodes)

    try:
        net.set_latency(0.001, 0.004)
        net.set_loss(0.05)

        for i in range(10):
            await client.put("k", i)
            assert await client.get("k") == i
    finally:
        net.reset_faults()
        await shutdown(nodes)
        disconnect(client)


async def test_a_read_against_a_fully_unavailable_cluster_fails_rather_than_lying(
    net: SimulatedNetwork,
) -> None:
    """Refusing to answer is the correct behaviour; inventing an answer is not."""
    nodes = build_cluster(net, 3)
    await run_cluster(nodes)
    client = connect(net, nodes)

    try:
        await client.put("k", "v")
        for node in nodes:
            await node.stop()

        with pytest.raises(RequestFailed):
            await client.get("k")
    finally:
        disconnect(client)
