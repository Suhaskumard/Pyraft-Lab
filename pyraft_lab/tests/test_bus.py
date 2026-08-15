"""Phase 0 acceptance: two nodes exchange arbitrary messages reliably."""

from __future__ import annotations

import asyncio

import pytest

from pyraft_lab.network import (
    Blob,
    DuplicateNode,
    InMemoryBus,
    Ping,
    Pong,
    Transport,
    UnknownNode,
)


@pytest.fixture
def bus() -> InMemoryBus:
    return InMemoryBus()


async def test_two_nodes_exchange_a_message(bus: InMemoryBus) -> None:
    """The Phase 0 acceptance gate, in its smallest form."""
    inbox1 = bus.register("node1")
    inbox2 = bus.register("node2")

    bus.send("node1", "node2", Ping(nonce=42))
    src, message = await inbox2.recv()

    assert src == "node1"
    assert message == Ping(nonce=42)

    bus.send("node2", "node1", Pong(nonce=42))
    src, message = await inbox1.recv()

    assert src == "node2"
    assert message == Pong(nonce=42)


async def test_arbitrary_payloads_survive_the_trip(bus: InMemoryBus) -> None:
    bus.register("a")
    inbox = bus.register("b")
    payload = {"op": "put", "key": "foo", "value": "bar", "meta": {"term": 3, "tags": ["x"]}}

    bus.send("a", "b", Blob(data=payload))
    _, message = await inbox.recv()

    assert isinstance(message, Blob)
    assert message.data == payload


async def test_delivery_is_fifo_per_sender(bus: InMemoryBus) -> None:
    bus.register("a")
    inbox = bus.register("b")

    for n in range(100):
        bus.send("a", "b", Ping(nonce=n))

    received = [(await inbox.recv())[1].nonce for _ in range(100)]
    assert received == list(range(100))


async def test_receiver_gets_a_copy_not_a_reference(bus: InMemoryBus) -> None:
    """A sender must not be able to mutate what a peer already holds."""
    bus.register("a")
    inbox = bus.register("b")

    sent = Blob(data={"counter": 1})
    bus.send("a", "b", sent)

    # Frozen models still have mutable interiors: the sender can scribble inside its
    # own payload after handing the message over. The receiver must not see it.
    sent.data["counter"] = 999

    _, received = await inbox.recv()

    assert isinstance(received, Blob)
    assert received.data == {"counter": 1}
    assert received.data is not sent.data


async def test_send_to_unknown_node_is_dropped_not_raised(bus: InMemoryBus) -> None:
    """A sender cannot distinguish a crashed peer from a slow one."""
    bus.register("a")

    bus.send("a", "ghost", Ping(nonce=1))

    assert bus.stats.sent == 1
    assert bus.stats.delivered == 0
    assert bus.stats.dropped_unknown_dst == 1


def test_sending_from_an_unregistered_node_is_a_bug(bus: InMemoryBus) -> None:
    """Unlike an unknown destination, an unknown sender is a programming error."""
    with pytest.raises(UnknownNode):
        bus.send("nobody", "somebody", Ping(nonce=1))


def test_duplicate_registration_is_rejected(bus: InMemoryBus) -> None:
    bus.register("node1")
    with pytest.raises(DuplicateNode):
        bus.register("node1")


def test_unregister_removes_the_node(bus: InMemoryBus) -> None:
    bus.register("a")
    bus.register("b")
    assert bus.nodes == {"a", "b"}

    bus.unregister("b")
    assert bus.nodes == {"a"}

    with pytest.raises(UnknownNode):
        bus.unregister("b")


async def test_broadcast_reaches_every_peer_but_not_the_sender(bus: InMemoryBus) -> None:
    inboxes = {node: bus.register(node) for node in ("n1", "n2", "n3")}

    bus.broadcast("n1", Ping(nonce=7))

    assert inboxes["n1"].pending() == 0
    for node in ("n2", "n3"):
        src, message = await inboxes[node].recv()
        assert (src, message) == ("n1", Ping(nonce=7))


async def test_try_recv_does_not_block(bus: InMemoryBus) -> None:
    bus.register("a")
    inbox = bus.register("b")

    assert inbox.try_recv() is None

    bus.send("a", "b", Ping(nonce=1))
    assert inbox.try_recv() == ("a", Ping(nonce=1))
    assert inbox.try_recv() is None


async def test_stats_and_pending_count_traffic(bus: InMemoryBus) -> None:
    bus.register("a")
    inbox = bus.register("b")

    for n in range(5):
        bus.send("a", "b", Ping(nonce=n))

    assert bus.stats.as_dict() == {"sent": 5, "delivered": 5, "dropped_unknown_dst": 0}
    assert bus.pending() == 5

    await inbox.recv()
    assert bus.pending() == 4


async def test_envelopes_carry_a_per_sender_sequence(bus: InMemoryBus) -> None:
    bus.register("a")
    bus.register("b")
    inbox = bus.register("c")

    bus.send("a", "c", Ping(nonce=0))
    bus.send("b", "c", Ping(nonce=0))
    bus.send("a", "c", Ping(nonce=1))

    seqs = [(await inbox.recv_envelope()) for _ in range(3)]
    assert [(e.src, e.seq) for e in seqs] == [("a", 0), ("b", 0), ("a", 1)]


def test_bus_satisfies_the_transport_protocol(bus: InMemoryBus) -> None:
    """Day 8 swaps in SimulatedNetwork behind this same protocol."""
    assert isinstance(bus, Transport)


async def test_concurrent_senders_all_get_through(bus: InMemoryBus) -> None:
    """Three nodes talking at once, 300 messages, none lost."""
    inboxes = {node: bus.register(node) for node in ("n1", "n2", "n3")}

    async def chatter(me: str) -> None:
        for n in range(100):
            for peer in inboxes:
                if peer != me:
                    bus.send(me, peer, Ping(nonce=n))
            await asyncio.sleep(0)

    await asyncio.gather(*(chatter(node) for node in inboxes))

    assert bus.stats.sent == 600
    assert bus.stats.delivered == 600
    assert all(inbox.pending() == 200 for inbox in inboxes.values())
