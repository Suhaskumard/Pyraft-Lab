"""``SimulatedNetwork``: the same ``Transport`` every node already speaks, with
latency, jitter, loss and partitions layered on top.

A node cannot tell this apart from :class:`~pyraft_lab.network.bus.InMemoryBus` -
both round-trip every message through the codec and hand back the same ``Inbox``.
What differs is *when*, or whether, a sent message shows up: :meth:`send` samples a
per-link delay and a drop decision, then registers delivery with the injected
:class:`~pyraft_lab.network.clock.Clock` via ``call_at`` instead of delivering
synchronously - a plain callback, not a task, so registration happens in the same call
that samples the delay rather than waiting for a scheduling tick to run first.
"""

from __future__ import annotations

import random
from collections.abc import Iterable
from dataclasses import dataclass, field

from pyraft_lab import NodeId
from pyraft_lab.network.bus import DuplicateNode, Inbox, UnknownNode
from pyraft_lab.network.clock import Clock, WallClock
from pyraft_lab.network.codec import DEFAULT_CODEC, Codec
from pyraft_lab.network.link import IDEAL_LINK, LinkConfig
from pyraft_lab.network.messages import Envelope, Message


@dataclass
class SimulatorStats:
    """Counters the experiment layer reads at the end of a run.

    A superset of :class:`~pyraft_lab.network.bus.BusStats` - dropped-for-loss and
    dropped-for-partition are counted separately from dropped-for-unknown-destination,
    since only the first two are *faults*; the third is a topology fact.
    """

    sent: int = 0
    delivered: int = 0
    dropped_unknown_dst: int = 0
    dropped_partition: int = 0
    dropped_loss: int = 0

    def as_dict(self) -> dict[str, int]:
        return {
            "sent": self.sent,
            "delivered": self.delivered,
            "dropped_unknown_dst": self.dropped_unknown_dst,
            "dropped_partition": self.dropped_partition,
            "dropped_loss": self.dropped_loss,
        }


@dataclass
class SimulatedNetwork:
    """A fault-injecting ``Transport``. See the module docstring."""

    clock: Clock = field(default_factory=WallClock)
    codec: Codec = DEFAULT_CODEC
    rng: random.Random = field(default_factory=random.Random)
    stats: SimulatorStats = field(default_factory=SimulatorStats)

    _inboxes: dict[NodeId, Inbox] = field(default_factory=dict, repr=False)
    _seq: dict[NodeId, int] = field(default_factory=dict, repr=False)
    _default_link: LinkConfig = field(default_factory=lambda: IDEAL_LINK, repr=False)
    _links: dict[tuple[NodeId, NodeId], LinkConfig] = field(default_factory=dict, repr=False)
    _groups: dict[NodeId, int] | None = field(default=None, repr=False)

    # --- membership -----------------------------------------------------------------

    def register(self, node_id: NodeId) -> Inbox:
        if node_id in self._inboxes:
            raise DuplicateNode(f"{node_id!r} is already registered")
        inbox = Inbox(node_id)
        self._inboxes[node_id] = inbox
        self._seq.setdefault(node_id, 0)
        return inbox

    def unregister(self, node_id: NodeId) -> None:
        if self._inboxes.pop(node_id, None) is None:
            raise UnknownNode(f"{node_id!r} is not registered")

    @property
    def nodes(self) -> frozenset[NodeId]:
        return frozenset(self._inboxes)

    # --- fault configuration ----------------------------------------------------------
    # Day 8: latency/jitter/loss, applied network-wide unless a link is overridden.
    # Day 9: partitions, which sit orthogonal to latency/loss - a dropped-for-partition
    # message never even reaches the loss roll.

    def set_latency(self, min_seconds: float, max_seconds: float) -> None:
        """Give every link (without its own override) a uniform delay range."""
        self._default_link = LinkConfig(
            min_latency=min_seconds,
            max_latency=max_seconds,
            loss_probability=self._default_link.loss_probability,
        )

    def set_loss(self, probability: float) -> None:
        """Give every link (without its own override) an independent drop probability."""
        self._default_link = LinkConfig(
            min_latency=self._default_link.min_latency,
            max_latency=self._default_link.max_latency,
            loss_probability=probability,
        )

    def set_link(self, src: NodeId, dst: NodeId, config: LinkConfig) -> None:
        """Override one direction of one link, independent of the network default."""
        self._links[(src, dst)] = config

    def clear_link(self, src: NodeId, dst: NodeId) -> None:
        self._links.pop((src, dst), None)

    def reset_faults(self) -> None:
        """Back to an ideal network: no latency, no loss, no partition."""
        self._default_link = IDEAL_LINK
        self._links.clear()
        self._groups = None

    def partition(self, *groups: Iterable[NodeId]) -> None:
        """Split the network so nodes in different groups cannot reach each other.

        A node named in no group is unaffected - reachable from, and able to reach,
        everyone. Calling this again replaces any partition already in effect.
        """
        assignment: dict[NodeId, int] = {}
        for i, group in enumerate(groups):
            for node in group:
                assignment[node] = i
        self._groups = assignment

    def heal(self) -> None:
        """Undo :meth:`partition`. Every node can reach every other node again."""
        self._groups = None

    def _is_partitioned(self, src: NodeId, dst: NodeId) -> bool:
        if self._groups is None:
            return False
        src_group = self._groups.get(src)
        dst_group = self._groups.get(dst)
        if src_group is None or dst_group is None:
            return False
        return src_group != dst_group

    def _link_for(self, src: NodeId, dst: NodeId) -> LinkConfig:
        return self._links.get((src, dst), self._default_link)

    # --- transfer ---------------------------------------------------------------------

    def send(self, src: NodeId, dst: NodeId, message: Message) -> None:
        """Deliver ``message`` from ``src`` to ``dst``, subject to whatever faults are
        currently configured.

        Never raises for an unreachable destination, a lost packet or a partition -
        exactly as :class:`InMemoryBus.send` does not, and for the same reason: a
        sender on a real network cannot tell any of those apart from a slow peer.
        """
        if src not in self._inboxes:
            raise UnknownNode(f"sender {src!r} is not registered")

        seq = self._seq[src]
        self._seq[src] = seq + 1
        envelope = Envelope(src=src, dst=dst, seq=seq, body=message)
        # Round-trip through the wire format up front, exactly as InMemoryBus does - a
        # message that cannot serialize fails on the send that created it.
        delivered = self.codec.decode(self.codec.encode(envelope))
        self.stats.sent += 1

        if dst not in self._inboxes:
            self.stats.dropped_unknown_dst += 1
            return

        if self._is_partitioned(src, dst):
            self.stats.dropped_partition += 1
            return

        link = self._link_for(src, dst)
        if link.should_drop(self.rng):
            self.stats.dropped_loss += 1
            return

        delay = link.sample_delay(self.rng)
        self.clock.call_at(self.clock.now() + delay, lambda: self._deliver(dst, delivered))

    def _deliver(self, dst: NodeId, envelope: Envelope) -> None:
        inbox = self._inboxes.get(dst)
        if inbox is None:
            self.stats.dropped_unknown_dst += 1
            return
        inbox._put(envelope)
        self.stats.delivered += 1

    def pending(self) -> int:
        """Messages already queued in an inbox. Does not count a message whose delay
        has not yet elapsed - see the injected ``Clock`` for that (``VirtualClock.
        pending()``, or simply waiting, for a ``WallClock``)."""
        return sum(inbox.pending() for inbox in self._inboxes.values())
