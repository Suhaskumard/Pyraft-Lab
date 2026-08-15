"""Named fault descriptions: what Phase 5's scenario runner will eventually parse out
of a YAML fault timeline (docs/plan.md's ``at:``/``action:`` chain). Each fault knows
how to apply itself to a :class:`~pyraft_lab.network.simulator.SimulatedNetwork`, so
the runner never has to know the network's internals - only which fault a scenario
line names and when to apply it.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from typing import TYPE_CHECKING, Protocol, runtime_checkable

from pyraft_lab import NodeId

if TYPE_CHECKING:  # pragma: no cover
    from pyraft_lab.network.simulator import SimulatedNetwork


@runtime_checkable
class Fault(Protocol):
    """Anything a fault timeline can schedule: apply yourself to the network."""

    def apply(self, network: SimulatedNetwork) -> None: ...


@dataclass(frozen=True)
class Latency(Fault):
    """Every link gets a delivery delay drawn uniformly from this range."""

    min_seconds: float
    max_seconds: float

    def apply(self, network: SimulatedNetwork) -> None:
        network.set_latency(self.min_seconds, self.max_seconds)


@dataclass(frozen=True)
class PacketLoss(Fault):
    """Every link drops messages independently with this probability."""

    probability: float

    def apply(self, network: SimulatedNetwork) -> None:
        network.set_loss(self.probability)


@dataclass(frozen=True)
class Partition(Fault):
    """Split the cluster into groups that cannot hear each other.

    A node named in no group keeps reaching, and being reached by, everyone - a
    partial partition, not necessarily every node's problem.
    """

    groups: tuple[frozenset[NodeId], ...]

    def apply(self, network: SimulatedNetwork) -> None:
        network.partition(*self.groups)


def partition(*groups: Iterable[NodeId]) -> Partition:
    """Build a :class:`Partition` fault from plain iterables of node ids."""
    return Partition(tuple(frozenset(g) for g in groups))


@dataclass(frozen=True)
class HealPartition(Fault):
    """Undo the most recent :class:`Partition` - every node reaches every node again."""

    def apply(self, network: SimulatedNetwork) -> None:
        network.heal()


@dataclass(frozen=True)
class ResetFaults(Fault):
    """Back to an ideal network: no latency, no loss, no partition."""

    def apply(self, network: SimulatedNetwork) -> None:
        network.reset_faults()
