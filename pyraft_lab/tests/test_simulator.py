"""Day 8/9: ``SimulatedNetwork`` - the same ``Transport`` as ``InMemoryBus``, with
latency, loss and partitions layered on top of a ``VirtualClock`` so delivery timing
is asserted exactly rather than polled for.
"""

from __future__ import annotations

import random

import pytest

from pyraft_lab.network import DuplicateNode, Ping, Transport, UnknownNode
from pyraft_lab.network.clock import VirtualClock
from pyraft_lab.network.link import LinkConfig
from pyraft_lab.network.simulator import SimulatedNetwork


@pytest.fixture
def clock() -> VirtualClock:
    return VirtualClock()


@pytest.fixture
def net(clock: VirtualClock) -> SimulatedNetwork:
    return SimulatedNetwork(clock=clock, rng=random.Random(0))


# --- membership, shared with InMemoryBus's contract --------------------------------


def test_satisfies_the_transport_protocol(net: SimulatedNetwork) -> None:
    assert isinstance(net, Transport)


def test_duplicate_registration_is_rejected(net: SimulatedNetwork) -> None:
    net.register("a")
    with pytest.raises(DuplicateNode):
        net.register("a")


def test_unregister_removes_the_node(net: SimulatedNetwork) -> None:
    net.register("a")
    net.register("b")
    net.unregister("b")
    assert net.nodes == {"a"}
    with pytest.raises(UnknownNode):
        net.unregister("b")


def test_sending_from_an_unregistered_node_is_a_bug(net: SimulatedNetwork) -> None:
    with pytest.raises(UnknownNode):
        net.send("nobody", "somebody", Ping(nonce=1))


async def test_send_to_unknown_node_is_dropped_not_raised(net: SimulatedNetwork) -> None:
    net.register("a")
    net.send("a", "ghost", Ping(nonce=1))
    assert net.stats.dropped_unknown_dst == 1
    assert net.stats.sent == 1


# --- an ideal network behaves like InMemoryBus --------------------------------------


async def test_zero_latency_delivers_immediately_without_advancing(
    net: SimulatedNetwork, clock: VirtualClock
) -> None:
    """No delay means ``call_at``'s deadline has already passed, so delivery happens
    inline, in ``send`` itself - no ``advance`` required, unlike every latency case."""
    net.register("a")
    inbox = net.register("b")

    net.send("a", "b", Ping(nonce=1))

    assert inbox.pending() == 1
    src, message = await inbox.recv()
    assert (src, message) == ("a", Ping(nonce=1))
    assert net.stats.delivered == 1


# --- latency ------------------------------------------------------------------------


async def test_a_fixed_latency_delays_delivery_until_the_clock_reaches_it(
    net: SimulatedNetwork, clock: VirtualClock
) -> None:
    net.register("a")
    inbox = net.register("b")
    net.set_latency(0.1, 0.1)

    net.send("a", "b", Ping(nonce=1))

    await clock.advance(0.05)
    assert inbox.pending() == 0  # not due yet

    await clock.advance(0.05)
    assert inbox.pending() == 1
    _, message = await inbox.recv()
    assert message == Ping(nonce=1)


async def test_latency_is_sampled_within_the_configured_range(
    net: SimulatedNetwork, clock: VirtualClock
) -> None:
    net.register("a")
    inbox = net.register("b")
    net.set_latency(0.01, 0.05)

    for n in range(20):
        net.send("a", "b", Ping(nonce=n))

    await clock.advance(0.05)
    received = [(await inbox.recv())[1].nonce for _ in range(20)]
    assert sorted(received) == list(range(20))


async def test_a_per_link_override_wins_over_the_network_default(
    net: SimulatedNetwork, clock: VirtualClock
) -> None:
    net.register("a")
    inbox_b = net.register("b")
    inbox_c = net.register("c")
    net.set_latency(0.1, 0.1)
    net.set_link("a", "c", LinkConfig(min_latency=0.5, max_latency=0.5))

    net.send("a", "b", Ping(nonce=1))
    net.send("a", "c", Ping(nonce=2))

    await clock.advance(0.1)
    assert inbox_b.pending() == 1
    assert inbox_c.pending() == 0  # still waiting on its 0.5s override

    await clock.advance(0.4)
    assert inbox_c.pending() == 1


# --- loss -----------------------------------------------------------------------------


async def test_total_loss_drops_every_message(net: SimulatedNetwork, clock: VirtualClock) -> None:
    net.register("a")
    inbox = net.register("b")
    net.set_loss(1.0)

    for n in range(10):
        net.send("a", "b", Ping(nonce=n))
    await clock.advance(1.0)

    assert inbox.pending() == 0
    assert net.stats.dropped_loss == 10
    assert net.stats.delivered == 0


async def test_zero_loss_drops_nothing(net: SimulatedNetwork, clock: VirtualClock) -> None:
    net.register("a")
    inbox = net.register("b")
    net.set_loss(0.0)

    for n in range(10):
        net.send("a", "b", Ping(nonce=n))
    await clock.advance(1.0)

    assert inbox.pending() == 10
    assert net.stats.dropped_loss == 0


async def test_partial_loss_drops_some_and_delivers_the_rest(
    net: SimulatedNetwork, clock: VirtualClock
) -> None:
    net.register("a")
    inbox = net.register("b")
    net.set_loss(0.5)

    for n in range(200):
        net.send("a", "b", Ping(nonce=n))
    await clock.advance(1.0)

    assert 0 < net.stats.dropped_loss < 200
    assert net.stats.dropped_loss + inbox.pending() == 200


# --- partitions (Day 9) -----------------------------------------------------------------


async def test_a_partition_blocks_traffic_across_groups(
    net: SimulatedNetwork, clock: VirtualClock
) -> None:
    inbox_a = net.register("a")
    inbox_b = net.register("b")
    inbox_c = net.register("c")
    net.partition({"a"}, {"b", "c"})

    net.send("a", "b", Ping(nonce=1))
    net.send("b", "c", Ping(nonce=2))
    await clock.advance(0.0)

    assert inbox_a.pending() == 0
    assert inbox_b.pending() == 0
    assert net.stats.dropped_partition == 1

    _, message = await inbox_c.recv()  # same side of the split: unaffected
    assert message == Ping(nonce=2)


async def test_a_node_named_in_no_group_reaches_everyone(
    net: SimulatedNetwork, clock: VirtualClock
) -> None:
    net.register("a")
    net.register("b")
    inbox_observer = net.register("observer")
    net.partition({"a"}, {"b"})  # "observer" is in neither group

    net.send("a", "observer", Ping(nonce=1))
    net.send("observer", "b", Ping(nonce=2))
    await clock.advance(0.0)

    assert inbox_observer.pending() == 1
    assert net.stats.dropped_partition == 0


async def test_heal_restores_full_connectivity(net: SimulatedNetwork, clock: VirtualClock) -> None:
    inbox_a = net.register("a")
    net.register("b")
    net.partition({"a"}, {"b"})

    net.send("b", "a", Ping(nonce=1))
    await clock.advance(0.0)
    assert inbox_a.pending() == 0

    net.heal()
    net.send("b", "a", Ping(nonce=2))
    await clock.advance(0.0)
    assert inbox_a.pending() == 1


async def test_reset_faults_clears_latency_loss_and_partition(
    net: SimulatedNetwork, clock: VirtualClock
) -> None:
    inbox = net.register("a")
    net.register("b")
    net.set_latency(1.0, 1.0)
    net.set_loss(1.0)
    net.partition({"a"}, {"b"})

    net.reset_faults()
    net.send("b", "a", Ping(nonce=1))
    await clock.advance(0.0)

    assert inbox.pending() == 1
