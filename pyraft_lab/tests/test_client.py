"""The client: retry policy and metrics in isolation, then against a real cluster."""

from __future__ import annotations

import pytest
from tests.test_node_election import (
    build_cluster,
    leaders,
    run_cluster,
    shutdown,
    wait_until,
)

from pyraft_lab.client.client import (
    BusChannel,
    ClientMetrics,
    ClientReply,
    ClientRequest,
    KVClient,
    RequestFailed,
    RetryPolicy,
)
from pyraft_lab.network import InMemoryBus
from pyraft_lab.raft.node import RaftNode

FAST_RETRY = RetryPolicy(max_attempts=8, initial_backoff=0.002, max_backoff=0.02)


@pytest.fixture
def bus() -> InMemoryBus:
    return InMemoryBus()


# --- retry policy -----------------------------------------------------------------


def test_backoff_grows_exponentially() -> None:
    policy = RetryPolicy(initial_backoff=0.01, multiplier=2.0, max_backoff=10.0)

    assert [policy.backoff_for(i) for i in range(4)] == [0.01, 0.02, 0.04, 0.08]


def test_backoff_is_capped() -> None:
    policy = RetryPolicy(initial_backoff=0.1, multiplier=10.0, max_backoff=0.5)

    assert policy.backoff_for(5) == 0.5


# --- metrics ----------------------------------------------------------------------


def test_availability_counts_requests_not_attempts() -> None:
    """A request retried into success is still a request the system served."""
    metrics = ClientMetrics(attempts=9, successes=3, failures=0, redirects=6)

    assert metrics.availability == 1.0


def test_availability_is_unmeasured_before_any_request() -> None:
    """Not 0.0: nothing has been asked of the cluster, so nothing has been denied.

    Reporting zero here is what put a red 0.00% on the overview of an idle but
    perfectly healthy cluster, and it is the same distinction ``percentile`` draws
    when it refuses to call an unmeasured latency zero.
    """
    assert ClientMetrics().availability is None


def test_availability_reflects_outright_failures() -> None:
    metrics = ClientMetrics(successes=3, failures=1)

    assert metrics.availability == 0.75


# --- the retry loop, against scripted replies -------------------------------------


class ScriptedCluster:
    """Answers requests from a script, recording who was asked."""

    def __init__(self, *replies: object) -> None:
        self.script = list(replies)
        self.asked: list[str] = []

    async def __call__(self, target: str, request: ClientRequest) -> ClientReply | None:
        self.asked.append(target)
        answer = self.script.pop(0) if self.script else None
        if answer is None:
            return None
        if isinstance(answer, ClientReply):
            return answer.model_copy(update={"request_id": request.request_id})
        raise AssertionError(f"bad script entry {answer!r}")


def ok(value: object = None) -> ClientReply:
    return ClientReply(request_id="", ok=True, value=value)


def not_leader(hint: str | None) -> ClientReply:
    return ClientReply(request_id="", ok=False, not_leader=True, leader_hint=hint)


async def test_a_redirect_is_followed_and_counted_separately_from_a_failure() -> None:
    cluster = ScriptedCluster(not_leader("n2"), ok("done"))
    client = KVClient("c1", ["n1", "n2", "n3"], cluster, policy=FAST_RETRY)

    result = await client.put("k", "v")

    assert result == "done"
    assert cluster.asked == ["n1", "n2"]  # the hint was believed
    assert client.metrics.redirects == 1
    assert client.metrics.failures == 0
    assert client.metrics.successes == 1


async def test_the_leader_is_remembered_across_requests() -> None:
    """Leader discovery is worth nothing if it has to be repeated every time."""
    cluster = ScriptedCluster(not_leader("n3"), ok(1), ok(2))
    client = KVClient("c1", ["n1", "n2", "n3"], cluster, policy=FAST_RETRY)

    await client.put("a", 1)
    await client.put("b", 2)

    assert cluster.asked == ["n1", "n3", "n3"]


async def test_a_silent_node_is_abandoned_and_another_is_tried() -> None:
    cluster = ScriptedCluster(None, ok("served"))
    client = KVClient("c1", ["n1", "n2", "n3"], cluster, policy=FAST_RETRY)

    assert await client.put("k", "v") == "served"
    assert cluster.asked == ["n1", "n2"]  # rotated rather than re-asking the dead node
    assert client.metrics.timeouts == 1


async def test_a_stale_leader_hint_does_not_trap_the_client() -> None:
    """If the remembered leader goes silent, the client must go looking again."""
    cluster = ScriptedCluster(ok("first"), None, ok("second"))
    client = KVClient("c1", ["n1", "n2", "n3"], cluster, policy=FAST_RETRY)

    await client.put("a", 1)
    assert client.leader_hint == "n1"

    await client.put("b", 2)

    assert cluster.asked == ["n1", "n1", "n2"]


async def test_giving_up_raises_and_counts_one_failure() -> None:
    cluster = ScriptedCluster(*[not_leader(None)] * 3)
    client = KVClient("c1", ["n1", "n2", "n3"], cluster, policy=RetryPolicy(max_attempts=3))

    with pytest.raises(RequestFailed, match="gave up after 3 attempts"):
        await client.put("k", "v")

    assert client.metrics.failures == 1
    assert client.metrics.attempts == 3


async def test_each_request_gets_a_distinct_id() -> None:
    """Reply matching depends on it; duplicates would cross wires between requests."""
    seen: list[str] = []

    async def capture(target: str, request: ClientRequest) -> ClientReply:
        seen.append(request.request_id)
        return ClientReply(request_id=request.request_id, ok=True)

    client = KVClient("c1", ["n1"], capture, policy=FAST_RETRY)
    await client.put("a", 1)
    await client.get("a")

    assert len(set(seen)) == 2


# --- against a real cluster --------------------------------------------------------


async def connect(bus: InMemoryBus, nodes: list[RaftNode], name: str = "client") -> KVClient:
    channel = BusChannel(bus, name, timeout=0.15)
    return KVClient(name, [n.node_id for n in nodes], channel, policy=FAST_RETRY)


async def test_put_then_get_through_the_cluster(bus: InMemoryBus) -> None:
    nodes = build_cluster(bus, 3)
    await run_cluster(nodes)
    client = await connect(bus, nodes)

    try:
        await client.put("colour", "blue")
        assert await client.get("colour") == "blue"
    finally:
        await shutdown(nodes)


async def test_the_client_finds_the_leader_whoever_it_is(bus: InMemoryBus) -> None:
    """The client is told nothing about who leads; it discovers it by being redirected."""
    nodes = build_cluster(bus, 5)
    await run_cluster(nodes)
    client = await connect(bus, nodes)

    try:
        await client.put("k", 1)

        assert client.leader_hint == leaders(nodes)[0].node_id
    finally:
        await shutdown(nodes)


async def test_writes_land_on_every_replica(bus: InMemoryBus) -> None:
    nodes = build_cluster(bus, 3)
    await run_cluster(nodes)
    client = await connect(bus, nodes)

    try:
        for i in range(10):
            await client.put(f"k{i}", i)
        assert await wait_until(lambda: all(len(n.store) == 10 for n in nodes))

        for node in nodes:
            assert node.store.read("k9") == 9
            assert len(node.store) == 10
    finally:
        await shutdown(nodes)


async def test_delete_and_cas_through_the_cluster(bus: InMemoryBus) -> None:
    nodes = build_cluster(bus, 3)
    await run_cluster(nodes)
    client = await connect(bus, nodes)

    try:
        await client.put("n", 1)

        assert await client.cas("n", expected=1, new=2) is True
        assert await client.cas("n", expected=1, new=3) is False  # already 2
        assert await client.get("n") == 2

        assert await client.delete("n") is True
        assert await client.delete("n") is False  # already gone
        assert await client.get("n") is None
    finally:
        await shutdown(nodes)


async def test_the_client_survives_a_leader_crash(bus: InMemoryBus) -> None:
    """Availability under failover: the write goes through against the new leader."""
    nodes = build_cluster(bus, 5)
    await run_cluster(nodes)
    client = await connect(bus, nodes)
    alive = list(nodes)

    try:
        await client.put("before", 1)
        leader = leaders(nodes)[0]

        await leader.stop()
        alive = [n for n in nodes if n is not leader]
        assert await wait_until(lambda: len(leaders(alive)) == 1)

        await client.put("after", 2)

        assert await client.get("before") == 1  # nothing committed was lost
        assert await client.get("after") == 2
        assert client.metrics.availability == 1.0
    finally:
        await shutdown(alive)
