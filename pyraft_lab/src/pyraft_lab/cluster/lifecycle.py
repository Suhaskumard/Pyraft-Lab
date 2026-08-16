"""Startup and shutdown *sequencing* for a live cluster.

The real-time analogue of ``experiments/runner.py``'s ``ExperimentRunner._settle`` -
poll until a leader exists - and of ``tests/test_node_election.py``'s ad hoc
``wait_until``/``leaders`` helpers, factored out here instead of written a third time
in ``cluster/manager.py`` and a fourth in this package's own tests. Deliberately
reusable against a bare list of ``RaftNode``s with no ``ClusterManager`` involved.

Unlike ``_settle``, which gives up silently and lets a simulated run proceed
leaderless if nothing wins in time, ``wait_for_leader`` here raises. A live cluster a
human is about to run ``put``/``get`` against should fail loudly if it never settles,
not quietly hand back a session with nothing actually elected.
"""

from __future__ import annotations

from collections.abc import Iterable
from typing import TYPE_CHECKING

from pyraft_lab.network.clock import Clock, WallClock
from pyraft_lab.raft.node import RaftNode
from pyraft_lab.raft.state import Role

if TYPE_CHECKING:
    # Only for the type hint below - manager.py imports this module, so importing it
    # back at runtime would close a cycle.
    from pyraft_lab.cluster.manager import NodeManager


class LifecycleTimeout(Exception):
    """A cluster did not reach a required state in time."""


class LifecycleManager:
    """Sequencing only - never builds, owns, or configures a node itself."""

    def __init__(self, clock: Clock | None = None) -> None:
        self._clock: Clock = clock if clock is not None else WallClock()

    async def wait_for_leader(
        self, nodes: list[RaftNode], *, timeout: float, interval: float = 0.01
    ) -> RaftNode:
        """Poll ``nodes`` until exactly one reports ``Role.LEADER``.

        Raises :class:`LifecycleTimeout` rather than returning ``None`` - see the
        module docstring for why that is the right default here, unlike the
        simulated runner's more forgiving ``_settle``.
        """
        deadline = self._clock.now() + timeout
        while self._clock.now() < deadline:
            leaders = [n for n in nodes if n.role is Role.LEADER]
            if len(leaders) == 1:
                return leaders[0]
            await self._clock.sleep_until(self._clock.now() + interval)
        raise LifecycleTimeout(
            f"no leader elected within {timeout:.1f}s among {[n.node_id for n in nodes]}"
        )

    async def shutdown(self, managers: Iterable[NodeManager]) -> None:
        """Stop every still-running node manager, ``crashed=False``.

        Tolerates a manager that is already stopped, so a second call - or a REPL
        session tearing down after a restart was already mid-flight - is never an
        error.
        """
        for manager in managers:
            if manager.running:
                await manager.stop(crashed=False)
