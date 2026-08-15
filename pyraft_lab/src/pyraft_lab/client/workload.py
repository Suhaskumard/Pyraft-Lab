"""What the clients actually do during an experiment.

A workload is the thing that turns a scenario into evidence. It issues operations at a
configured rate against the run's clock, and records every one of them into a
:class:`~pyraft_lab.consistency.history.History` - invocation before the call, response
after it - so the same run that produces latency numbers also produces the history the
linearizability checker reads.

Two shapes, both named by the specification:

- ``put_get_mix`` - a steady stream at a fixed rate, the default for every scenario.
- ``burst`` - the same total volume delivered in clumps of simultaneous operations,
  which is what actually produces *overlapping* operations. A steady one-at-a-time
  stream from a single client has no concurrency in it at all, and a history with no
  concurrency is linearizable for trivial reasons.

Failures are recorded, never raised. A client giving up under a partition is the
measurement, not an error interrupting it.
"""

from __future__ import annotations

import asyncio
import random
from dataclasses import dataclass
from typing import Any

from pyraft_lab.client.client import KVClient, RequestFailed
from pyraft_lab.consistency.history import History, Operation, OpKind
from pyraft_lab.network.clock import Clock

PUT_GET_MIX = "put_get_mix"
BURST = "burst"


@dataclass(frozen=True)
class WorkloadConfig:
    """A scenario's ``workload:`` block.

    ``puts_per_sec`` and ``get_ratio`` are the specification's; ``keys``, ``clients``
    and ``burst_size`` are this implementation's, and each earns its place:

    - ``keys`` bounds how far the history spreads. Every key is checked independently
      (Herlihy & Wing locality), so more keys means a cheaper check but weaker evidence
      per key - operations on distinct keys never contend.
    - ``clients`` is what makes a history concurrent at all. One client issuing one
      operation at a time produces a totally ordered history that is linearizable
      whatever the cluster does, which would make every check vacuous.
    - ``burst_size`` is how many operations a ``burst`` workload issues at once.
    """

    type: str = PUT_GET_MIX
    puts_per_sec: float = 10.0
    get_ratio: float = 0.5
    keys: int = 4
    clients: int = 2
    burst_size: int = 5

    @property
    def operations_per_sec(self) -> float:
        """Total operation rate.

        The specification fixes the *put* rate and the share of operations that are
        reads, so the total follows: at 10 puts/s with half the traffic being reads,
        the cluster sees 20 operations a second.
        """
        share_that_are_puts = max(1e-9, 1.0 - self.get_ratio)
        return self.puts_per_sec / share_that_are_puts

    def to_dict(self) -> dict[str, Any]:
        return {
            "type": self.type,
            "puts_per_sec": self.puts_per_sec,
            "get_ratio": self.get_ratio,
            "keys": self.keys,
            "clients": self.clients,
            "burst_size": self.burst_size,
        }


class UnknownWorkload(Exception):
    """Raised for a ``workload.type`` this build does not implement."""

    def __init__(self, name: str) -> None:
        super().__init__(f"unknown workload type {name!r}; known types: {PUT_GET_MIX}, {BURST}")
        self.name = name


# --- one operation, recorded ---------------------------------------------------------


async def _record_put(
    client: KVClient, history: History, clock: Clock, key: str, value: Any
) -> Operation:
    op = history.invoke(client.client_id, OpKind.PUT, key, clock.now(), value=value)
    try:
        result = await client.put(key, value)
    except RequestFailed as exc:
        history.fail(op, clock.now(), str(exc), target=client.leader_hint)
        return op
    history.complete(op, clock.now(), result=result, ok=True, target=client.leader_hint)
    return op


async def _record_get(client: KVClient, history: History, clock: Clock, key: str) -> Operation:
    op = history.invoke(client.client_id, OpKind.GET, key, clock.now())
    try:
        value, found = await client.read(key)
    except RequestFailed as exc:
        history.fail(op, clock.now(), str(exc), target=client.leader_hint)
        return op
    history.complete(op, clock.now(), result=value, ok=found, target=client.leader_hint)
    return op


# --- the generators --------------------------------------------------------------------


class Workload:
    """Drives one client until the run's deadline.

    Each client gets its own :class:`random.Random`, seeded from the run's seed and the
    client's index, so a replay reproduces not just the same faults but the same
    operations in the same order.
    """

    def __init__(
        self,
        config: WorkloadConfig,
        client: KVClient,
        history: History,
        *,
        clock: Clock,
        rng: random.Random,
        index: int = 0,
    ) -> None:
        if config.type not in (PUT_GET_MIX, BURST):
            raise UnknownWorkload(config.type)
        self.config = config
        self.client = client
        self.history = history
        self.clock = clock
        self.rng = rng
        self.index = index
        self.issued = 0

    def _key(self) -> str:
        return f"k{self.rng.randrange(self.config.keys)}"

    def _value(self) -> str:
        """A value unique to this client and operation.

        Unique on purpose: if two writes could store the same value, a read returning it
        would not identify which write it saw, and a duplicated or resurrected write
        would be indistinguishable from a legitimate one.
        """
        return f"{self.client.client_id}-{self.issued}"

    async def run(self, until: float) -> None:
        """Issue operations until the clock reaches ``until``."""
        if self.config.type == BURST:
            await self._run_burst(until)
        else:
            await self._run_steady(until)

    async def _run_steady(self, until: float) -> None:
        interval = self.config.clients / max(1e-9, self.config.operations_per_sec)
        # Stagger the clients so they do not all fire on the same instant forever, which
        # would make a multi-client run behave like one client issuing batches.
        next_at = self.clock.now() + interval * (self.index / max(1, self.config.clients))

        while self.clock.now() < until:
            await self.clock.sleep_until(next_at)
            if self.clock.now() >= until:
                return
            await self._one()
            next_at = max(self.clock.now(), next_at + interval)

    async def _run_burst(self, until: float) -> None:
        size = max(1, self.config.burst_size)
        gap = size * self.config.clients / max(1e-9, self.config.operations_per_sec)
        next_at = self.clock.now()

        while self.clock.now() < until:
            await self.clock.sleep_until(next_at)
            if self.clock.now() >= until:
                return
            # Issued together and awaited together: these operations genuinely overlap,
            # which is the only way a history gets any concurrency to check.
            await asyncio.gather(*(self._one() for _ in range(size)))
            next_at = max(self.clock.now(), next_at + gap)

    async def _one(self) -> None:
        key = self._key()
        is_read = self.rng.random() < self.config.get_ratio
        self.issued += 1

        if is_read:
            await _record_get(self.client, self.history, self.clock, key)
        else:
            await _record_put(self.client, self.history, self.clock, key, self._value())
