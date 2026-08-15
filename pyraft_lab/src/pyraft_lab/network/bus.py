"""The Day 1 message bus: honest, in-memory, fault-free.

This is deliberately the dumbest transport that can work. Nothing here knows about
latency, drops or partitions - those arrive on Day 8 in ``SimulatedNetwork``, which
implements the same :class:`Transport` protocol so nodes never learn the difference.

One thing it does share with a real network: every message round-trips through the
codec on send. A node therefore cannot hand a peer a reference to its own mutable
state, and an unserializable message fails immediately rather than three phases later.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from typing import Protocol, runtime_checkable

from pyraft_lab import NodeId
from pyraft_lab.network.codec import DEFAULT_CODEC, Codec
from pyraft_lab.network.messages import Envelope, Message


class TransportError(Exception):
    """Base class for transport-level failures."""


class UnknownNode(TransportError):
    """Raised when registering or addressing a node the transport does not know."""


class DuplicateNode(TransportError):
    """Raised when a node id is registered twice."""


@dataclass
class BusStats:
    """Counters the experiment layer reads at the end of a run."""

    sent: int = 0
    delivered: int = 0
    dropped_unknown_dst: int = 0

    def as_dict(self) -> dict[str, int]:
        return {
            "sent": self.sent,
            "delivered": self.delivered,
            "dropped_unknown_dst": self.dropped_unknown_dst,
        }


class Inbox:
    """A node's receive queue.

    Yields ``(src, message)`` pairs in delivery order.
    """

    def __init__(self, owner: NodeId) -> None:
        self.owner = owner
        self._queue: asyncio.Queue[Envelope] = asyncio.Queue()

    def _put(self, envelope: Envelope) -> None:
        self._queue.put_nowait(envelope)

    async def recv(self) -> tuple[NodeId, Message]:
        """Wait for the next message. Blocks until one arrives."""
        envelope = await self._queue.get()
        return envelope.src, envelope.body

    async def recv_envelope(self) -> Envelope:
        """Wait for the next message, keeping its addressing metadata."""
        return await self._queue.get()

    def try_recv(self) -> tuple[NodeId, Message] | None:
        """Take the next message if one is already queued, else ``None``."""
        try:
            envelope = self._queue.get_nowait()
        except asyncio.QueueEmpty:
            return None
        return envelope.src, envelope.body

    def pending(self) -> int:
        """How many messages are queued and not yet received."""
        return self._queue.qsize()


@runtime_checkable
class Transport(Protocol):
    """What a Raft node needs from the network, and nothing more.

    ``InMemoryBus`` implements this today; ``SimulatedNetwork`` implements it on Day 8.
    """

    def register(self, node_id: NodeId) -> Inbox: ...

    def unregister(self, node_id: NodeId) -> None: ...

    def send(self, src: NodeId, dst: NodeId, message: Message) -> None: ...

    @property
    def nodes(self) -> frozenset[NodeId]: ...


@dataclass
class InMemoryBus:
    """A reliable in-process transport. Every message sent to a known node arrives."""

    codec: Codec = DEFAULT_CODEC
    stats: BusStats = field(default_factory=BusStats)
    _inboxes: dict[NodeId, Inbox] = field(default_factory=dict, repr=False)
    _seq: dict[NodeId, int] = field(default_factory=dict, repr=False)

    # --- membership -------------------------------------------------------------

    def register(self, node_id: NodeId) -> Inbox:
        """Attach a node to the bus and hand back its inbox."""
        if node_id in self._inboxes:
            raise DuplicateNode(f"{node_id!r} is already registered")
        inbox = Inbox(node_id)
        self._inboxes[node_id] = inbox
        self._seq.setdefault(node_id, 0)
        return inbox

    def unregister(self, node_id: NodeId) -> None:
        """Detach a node. Messages addressed to it are dropped from now on."""
        if self._inboxes.pop(node_id, None) is None:
            raise UnknownNode(f"{node_id!r} is not registered")

    @property
    def nodes(self) -> frozenset[NodeId]:
        return frozenset(self._inboxes)

    # --- transfer ---------------------------------------------------------------

    def send(self, src: NodeId, dst: NodeId, message: Message) -> None:
        """Deliver ``message`` from ``src`` to ``dst``.

        Never raises for an unreachable destination: a sender on a real network cannot
        tell a crashed peer from a slow one, and neither should this one. Unknown
        destinations are counted, not reported.
        """
        if src not in self._inboxes:
            raise UnknownNode(f"sender {src!r} is not registered")

        seq = self._seq[src]
        self._seq[src] = seq + 1
        envelope = Envelope(src=src, dst=dst, seq=seq, body=message)

        # Round-trip through the wire format, exactly as a real transport would.
        delivered = self.codec.decode(self.codec.encode(envelope))
        self.stats.sent += 1

        inbox = self._inboxes.get(dst)
        if inbox is None:
            self.stats.dropped_unknown_dst += 1
            return

        inbox._put(delivered)
        self.stats.delivered += 1

    def broadcast(self, src: NodeId, message: Message) -> None:
        """Send ``message`` to every registered node except ``src``."""
        for dst in sorted(self._inboxes):
            if dst != src:
                self.send(src, dst, message)

    def pending(self) -> int:
        """Total messages queued across all inboxes."""
        return sum(inbox.pending() for inbox in self._inboxes.values())
