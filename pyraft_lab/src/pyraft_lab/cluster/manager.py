"""``NodeManager`` and ``ClusterManager``: one member's lifecycle, and the whole
cluster's shared runtime.

The split: ``NodeManager`` owns exactly one member - its construction, its WAL path
(if any), and its start/stop/restart mechanics - and knows nothing about the rest of
the cluster, which is what makes it independently testable. ``ClusterManager`` owns
everything *shared* - the one ``SimulatedNetwork``, the one ``Tracer``/``LiveMetrics``
pair, the one ``WallClock``, the dict of ``NodeManager``s, and the one ``KVClient``
the REPL drives.

Transport choice: ``SimulatedNetwork`` with its default ``IDEAL_LINK``, not the
plainer ``InMemoryBus``. A ``RaftNode`` emits its own trace events regardless of
transport, but ``NODE_CRASHED``/``NODE_RECOVERED``/``PARTITION_*`` are narrated only
by ``SimulatedNetwork`` - ``InMemoryBus`` records nothing. Since ``restart_node`` is
implemented as ``node.stop(crashed=True)`` then a fresh start, using
``SimulatedNetwork`` means that shows up correctly in ``trace``/``node logs`` for
free, and costs nothing today (no fault is ever configured), while leaving room for a
future fault-injection REPL command with no transport swap needed later.
"""

from __future__ import annotations

import random
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from pyraft_lab import NodeId
from pyraft_lab.client.client import BusChannel, KVClient, RetryPolicy
from pyraft_lab.cluster.config import ClusterConfig
from pyraft_lab.cluster.lifecycle import LifecycleManager
from pyraft_lab.network.clock import Clock, WallClock
from pyraft_lab.network.simulator import SimulatedNetwork
from pyraft_lab.observability.metrics import LiveMetrics
from pyraft_lab.observability.tracer import Tracer
from pyraft_lab.raft.node import RaftNode
from pyraft_lab.raft.persistence import WriteAheadLog


class ClusterError(Exception):
    """A cluster-level operation could not be completed."""


@dataclass
class NodeManager:
    """One member's construction and start/stop/restart, in isolation."""

    node_id: NodeId
    peer_ids: list[NodeId]
    transport: SimulatedNetwork
    clock: Clock
    tracer: Tracer
    rng_seed: int
    wal_path: Path | None
    election_timeout_range: tuple[float, float]
    heartbeat_interval: float

    node: RaftNode | None = field(default=None, init=False)
    running: bool = field(default=False, init=False)

    def _build(self) -> RaftNode:
        wal = (
            WriteAheadLog(self.wal_path, node_id=self.node_id)
            if self.wal_path is not None
            else None
        )
        return RaftNode(
            node_id=self.node_id,
            peers=self.peer_ids,
            transport=self.transport,
            rng=random.Random(self.rng_seed),
            clock=self.clock,
            tracer=self.tracer,
            wal=wal,
            election_timeout_range=self.election_timeout_range,
            heartbeat_interval=self.heartbeat_interval,
        )

    def start(self) -> RaftNode:
        if self.node is None:
            self.node = self._build()
        self.node.start()
        self.running = True
        return self.node

    async def stop(self, *, crashed: bool = True) -> None:
        if self.node is not None and self.running:
            await self.node.stop(crashed=crashed)
        self.running = False

    async def restart(self) -> RaftNode:
        """Crash, then come back - from disk alone if this member is persistent.

        The same two-path pattern ``experiments/runner.py``'s WAL-backed recovery
        (Phase 8's chaos campaign) already established: reused here, not reinvented.
        A crash always discards the running task; only a WAL-backed member also
        discards the in-memory object, since ``RaftNode.stop`` never clears state on
        its own (only a fresh object, rebuilt from what actually made it to disk,
        can prove a restart is real).
        """
        await self.stop(crashed=True)
        if self.wal_path is not None:
            self.node = self._build()
        assert self.node is not None
        self.node.start()
        self.running = True
        return self.node


@dataclass(frozen=True)
class NodeSummary:
    """One node's place in the cluster, for ``topology()``."""

    node_id: NodeId
    alive: bool
    role: str | None
    term: int | None
    leader_id: NodeId | None
    peers: list[NodeId]

    def to_dict(self) -> dict[str, Any]:
        return {
            "node_id": self.node_id,
            "alive": self.alive,
            "role": self.role,
            "term": self.term,
            "leader_id": self.leader_id,
            "peers": self.peers,
        }


class ClusterManager:
    """A real, live cluster: build it, hand out a client, and stop it cleanly."""

    def __init__(self, config: ClusterConfig) -> None:
        self.config = config
        self.clock: Clock = WallClock()
        self.tracer = Tracer(self.clock)
        self.live = LiveMetrics(self.tracer)
        self.network = SimulatedNetwork(
            clock=self.clock, rng=random.Random(config.seed + 977), tracer=self.tracer
        )
        self.lifecycle = LifecycleManager(self.clock)

        ids = config.node_ids
        self.nodes: dict[NodeId, NodeManager] = {
            node_id: NodeManager(
                node_id=node_id,
                peer_ids=ids,
                transport=self.network,
                clock=self.clock,
                tracer=self.tracer,
                rng_seed=config.seed + index,
                wal_path=(config.data_dir / f"{node_id}.wal") if config.data_dir else None,
                election_timeout_range=config.election_timeout_range,
                heartbeat_interval=config.heartbeat_interval,
            )
            for index, node_id in enumerate(ids)
        }
        self._channel: BusChannel | None = None
        self.client: KVClient | None = None

    # --- lifecycle ------------------------------------------------------------------

    async def start(self) -> RaftNode:
        """Build and start every node, then wait for a leader.

        Raises :class:`~pyraft_lab.cluster.lifecycle.LifecycleTimeout` if none is
        elected - a live cluster with nobody leading is not a cluster worth handing
        a REPL session to.
        """
        if self.config.data_dir is not None:
            self.config.data_dir.mkdir(parents=True, exist_ok=True)
        for manager in self.nodes.values():
            manager.start()

        leader = await self.lifecycle.wait_for_leader(
            [manager.node for manager in self.nodes.values() if manager.node is not None],
            timeout=self.config.election_timeout_range[1] * 4,
        )

        self._channel = BusChannel(
            self.network, "repl-client", timeout=self.config.client_timeout, clock=self.clock
        )
        self.client = KVClient(
            "repl-client", self.config.node_ids, self._channel,
            policy=RetryPolicy(), clock=self.clock,
        )
        return leader

    async def stop(self) -> None:
        """Tear the cluster down cleanly - every node, ``crashed=False``."""
        if self._channel is not None:
            self._channel.close()
            self._channel = None
        self.client = None
        await self.lifecycle.shutdown(self.nodes.values())

    async def restart_node(self, node_id: NodeId) -> RaftNode:
        manager = self.nodes.get(node_id)
        if manager is None:
            raise ClusterError(f"no such node: {node_id!r}")
        node = await manager.restart()
        await self.lifecycle.wait_for_leader(
            [m.node for m in self.nodes.values() if m.node is not None],
            timeout=self.config.election_timeout_range[1] * 4,
        )
        return node

    # --- addressing a member ----------------------------------------------------------

    def node(self, node_id: NodeId) -> RaftNode:
        manager = self.nodes.get(node_id)
        if manager is None or manager.node is None:
            raise ClusterError(f"no such node: {node_id!r}")
        return manager.node

    # --- observing the cluster ---------------------------------------------------------

    def topology(self) -> list[NodeSummary]:
        rows: list[NodeSummary] = []
        for node_id in self.config.node_ids:
            manager = self.nodes[node_id]
            node = manager.node
            if node is None or not manager.running:
                rows.append(NodeSummary(node_id, False, None, None, None, []))
            else:
                rows.append(
                    NodeSummary(
                        node_id, True, node.role.value, node.current_term,
                        node.leader_id, list(node.peers),
                    )
                )
        return rows

    def status(self) -> dict[str, Any]:
        """A one-word health reading plus the live counters behind it - Phase 4's
        ``LiveMetrics``, folded from this cluster's own trace."""
        return self.live.snapshot()
