"""Time, made pluggable: the wall clock nodes use today, and the deterministic virtual
clock everything from here on can run against instead.

``raft/node.py``'s timers and ``network/simulator.py``'s scheduled deliveries both go
through a :class:`Clock` rather than asyncio's own time, so a test - or Phase 7's
replay - can drive a whole run's worth of elections and message delays from a single
seed without waiting on real wall time.
"""

from __future__ import annotations

import asyncio
import heapq
import itertools
from collections.abc import Callable
from typing import Protocol, runtime_checkable


@runtime_checkable
class Clock(Protocol):
    """What a timer needs: the current time, a way to wait for a later one, and a way
    to schedule a plain callback for a later one without suspending a coroutine at
    all - what ``SimulatedNetwork`` schedules a delayed delivery with, since that has
    no coroutine of its own to suspend.
    """

    def now(self) -> float: ...

    async def sleep_until(self, deadline: float) -> None: ...

    def call_at(self, deadline: float, callback: Callable[[], None]) -> None: ...


class WallClock:
    """The real event loop clock. What every node used before Day 8."""

    def now(self) -> float:
        return asyncio.get_running_loop().time()

    async def sleep_until(self, deadline: float) -> None:
        remaining = max(0.0, deadline - self.now())
        await asyncio.sleep(remaining)

    def call_at(self, deadline: float, callback: Callable[[], None]) -> None:
        delay = max(0.0, deadline - self.now())
        asyncio.get_running_loop().call_later(delay, callback)


class VirtualClock:
    """A clock that only moves when told to.

    Every timer in a simulated run - a node's election deadline, a link's delivery
    delay - registers a waiter here instead of touching wall time. Nothing happens
    until :meth:`advance` is called, which is what makes a run reproducible: the same
    sequence of ``advance`` calls against the same seeded RNGs produces the same
    outcome regardless of how fast the test machine happens to be.
    """

    def __init__(self, start: float = 0.0) -> None:
        self._now = start
        self._pending: list[tuple[float, int, Callable[[], None]]] = []
        self._counter = itertools.count()

    def now(self) -> float:
        return self._now

    def call_at(self, deadline: float, callback: Callable[[], None]) -> None:
        """Register synchronously - unlike ``sleep_until``, there is no coroutine here
        to suspend, so nothing has to wait for a scheduling tick before the callback
        counts as registered. This is what lets ``SimulatedNetwork.send`` register a
        delivery's timer in the same call that samples its delay, rather than a task
        that might not get its first turn to run before the next ``advance``.
        """
        if deadline <= self._now:
            callback()
            return
        heapq.heappush(self._pending, (deadline, next(self._counter), callback))

    async def sleep_until(self, deadline: float) -> None:
        if deadline <= self._now:
            return
        event = asyncio.Event()
        self.call_at(deadline, event.set)
        await event.wait()

    async def advance(self, delta: float) -> None:
        """Move time forward by ``delta`` seconds, firing whatever falls due."""
        await self.advance_to(self._now + delta)

    async def advance_to(self, target: float) -> None:
        """Move time forward to ``target``, firing whatever falls due along the way.

        Callbacks run one at a time, yielding after each so a callback that reacts by
        registering another zero-delay timer (a heartbeat immediately re-arming, say)
        is caught by the next pass rather than left stranded until some later advance.
        """
        if target < self._now:
            raise ValueError(f"virtual clock cannot move backward ({target} < {self._now})")

        self._now = target
        while self._pending and self._pending[0][0] <= target:
            _, _, callback = heapq.heappop(self._pending)
            callback()
            await asyncio.sleep(0)

    def pending(self) -> int:
        """How many timers are still waiting for a future ``advance``."""
        return len(self._pending)
