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
from collections.abc import Awaitable, Callable
from typing import Protocol, TypeVar, runtime_checkable

T = TypeVar("T")


@runtime_checkable
class TimerHandle(Protocol):
    """What ``call_at`` hands back: a way to say "never mind" about a timer that has
    not fired yet.

    Needed because a scheduled callback can outlive the coroutine that asked for it -
    ``sleep_until``'s caller can be cancelled (``race_deadline`` does this to whichever
    side loses the race) while its timer is still sitting in the clock waiting to fire.
    Without a handle to cancel, that timer has nothing to tell it the wait is over.
    """

    def cancel(self) -> None: ...


@runtime_checkable
class Clock(Protocol):
    """What a timer needs: the current time, a way to wait for a later one, and a way
    to schedule a plain callback for a later one without suspending a coroutine at
    all - what ``SimulatedNetwork`` schedules a delayed delivery with, since that has
    no coroutine of its own to suspend.
    """

    def now(self) -> float: ...

    async def sleep_until(self, deadline: float) -> None: ...

    def call_at(self, deadline: float, callback: Callable[[], None]) -> TimerHandle: ...


class WallClock:
    """The real event loop clock. What every node used before Day 8."""

    def now(self) -> float:
        return asyncio.get_running_loop().time()

    async def sleep_until(self, deadline: float) -> None:
        remaining = max(0.0, deadline - self.now())
        await asyncio.sleep(remaining)

    def call_at(self, deadline: float, callback: Callable[[], None]) -> TimerHandle:
        delay = max(0.0, deadline - self.now())
        # asyncio's own TimerHandle already has the ``cancel()`` this protocol asks
        # for - nothing to wrap.
        return asyncio.get_running_loop().call_later(delay, callback)


class _VirtualTimerHandle:
    """A cancel flag a heap entry can be tagged with.

    ``heapq`` has no way to remove an entry from the middle, so cancellation is lazy
    deletion: mark it and let whoever next looks at the front of the heap discard it.
    Cheap because the alternative - a timer nobody ever cancels - is not: see the
    ``VirtualClock`` docstring for what leaving these to pile up costs.
    """

    __slots__ = ("cancelled",)

    def __init__(self) -> None:
        self.cancelled = False

    def cancel(self) -> None:
        self.cancelled = True


class VirtualClock:
    """A clock that only moves when told to.

    Every timer in a simulated run - a node's election deadline, a link's delivery
    delay - registers a waiter here instead of touching wall time. Nothing happens
    until :meth:`advance` is called, which is what makes a run reproducible: the same
    sequence of ``advance`` calls against the same seeded RNGs produces the same
    outcome regardless of how fast the test machine happens to be.

    Cancellation matters here in a way it would not on a real clock. ``race_deadline``
    cancels whichever side of a race loses, and on a busy cluster the *deadline* side -
    a node's election timeout, a client's retry backoff - loses almost every time,
    because a message arrives first. Every one of those losing timers has already been
    pushed onto :attr:`_pending`; if cancelling the coroutine did not also cancel the
    timer, each one would sit in the heap forever, un-fired and unreachable, while a
    busy run keeps creating more of them. The heap does not merely waste memory in that
    case - :meth:`next_deadline` keeps reporting the oldest of these dead timers as the
    next thing to happen, so the driver spends one whole outer iteration retiring each
    one, and a run that should finish in milliseconds instead grinds forward in
    sub-millisecond steps against a backlog that grows faster than it drains.
    """

    def __init__(self, start: float = 0.0) -> None:
        self._now = start
        self._pending: list[tuple[float, int, _VirtualTimerHandle, Callable[[], None]]] = []
        self._counter = itertools.count()

    def now(self) -> float:
        return self._now

    def call_at(self, deadline: float, callback: Callable[[], None]) -> TimerHandle:
        """Register synchronously - unlike ``sleep_until``, there is no coroutine here
        to suspend, so nothing has to wait for a scheduling tick before the callback
        counts as registered. This is what lets ``SimulatedNetwork.send`` register a
        delivery's timer in the same call that samples its delay, rather than a task
        that might not get its first turn to run before the next ``advance``.
        """
        handle = _VirtualTimerHandle()
        if deadline <= self._now:
            callback()
            return handle
        heapq.heappush(self._pending, (deadline, next(self._counter), handle, callback))
        return handle

    async def sleep_until(self, deadline: float) -> None:
        if deadline <= self._now:
            return
        event = asyncio.Event()
        handle = self.call_at(deadline, event.set)
        try:
            await event.wait()
        except asyncio.CancelledError:
            # The coroutine waiting on this timer was cancelled out from under it -
            # ``race_deadline`` does exactly this to the side that loses a race. Without
            # this, the timer we just registered would stay in the heap with nothing
            # left that will ever look at it firing.
            handle.cancel()
            raise

    async def advance(self, delta: float) -> None:
        """Move time forward by ``delta`` seconds, firing whatever falls due."""
        await self.advance_to(self._now + delta)

    async def advance_to(self, target: float) -> None:
        """Move time forward to ``target``, firing whatever falls due along the way.

        Callbacks run one at a time, yielding after each so a callback that reacts by
        registering another zero-delay timer (a heartbeat immediately re-arming, say)
        is caught by the next pass rather than left stranded until some later advance.
        A cancelled entry is discarded without yielding for it - nothing happened, so
        there is nothing for another task to react to.
        """
        if target < self._now:
            raise ValueError(f"virtual clock cannot move backward ({target} < {self._now})")

        self._now = target
        while self._pending and self._pending[0][0] <= target:
            _, _, handle, callback = heapq.heappop(self._pending)
            if handle.cancelled:
                continue
            callback()
            await asyncio.sleep(0)

    def _prune(self) -> None:
        """Drop cancelled entries waiting at the front of the heap.

        Only the front needs pruning - anything behind a live entry cannot affect
        :meth:`next_deadline`'s answer, and :meth:`advance_to` discards the rest as it
        reaches them. Called from both read methods below so neither one reports a
        cancelled timer's deadline as something that is actually going to happen.
        """
        while self._pending and self._pending[0][2].cancelled:
            heapq.heappop(self._pending)

    def pending(self) -> int:
        """How many *live* timers are still waiting for a future ``advance``."""
        self._prune()
        return len(self._pending)

    def next_deadline(self) -> float | None:
        """When the earliest waiting timer is due, or ``None`` if nothing is waiting.

        What lets a driver jump straight to the next thing that happens instead of
        ticking through the gap. Empty simulated time then costs nothing, which is the
        difference between a 30-second scenario taking milliseconds and taking 6000
        pointless steps.
        """
        self._prune()
        return self._pending[0][0] if self._pending else None


async def race_deadline(clock: Clock, awaitable: Awaitable[T], deadline: float) -> T | None:
    """Await ``awaitable``, abandoning it if ``deadline`` passes on ``clock``.

    Returns what it produced, or ``None`` if the deadline won. Every timeout in the
    system that has to work on both clocks goes through here - a node waiting for a
    message, a client waiting for a reply - because the two clocks need genuinely
    different mechanisms and neither caller should have to know that.

    The wall-clock path is plain ``asyncio.wait_for``: proven, and cheaper than the
    general path below, which costs two extra tasks per call. That matters where this
    is called every heartbeat - 12 ms in the test suite - and the overhead was once
    enough to shift scheduling under load and make a replication test flaky.

    A ``VirtualClock`` cannot use ``wait_for`` at all, since its timeout would be
    counted in real seconds against virtual time. It races the work against
    ``sleep_until`` instead, which brings one obligation: both branches - and the case
    where this call is itself cancelled mid-wait - must leave neither task behind as a
    waiter on a queue. An orphaned receive is invisible and permanent, because nothing
    will ever await it again.
    """
    if isinstance(clock, WallClock):
        try:
            return await asyncio.wait_for(awaitable, max(0.0, deadline - clock.now()))
        except TimeoutError:
            return None

    work = asyncio.ensure_future(awaitable)
    timeout: asyncio.Future[None] = asyncio.ensure_future(clock.sleep_until(deadline))
    tasks: tuple[asyncio.Future[object], ...] = (work, timeout)

    try:
        done, _ = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
    except asyncio.CancelledError:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        raise

    for task in tasks:
        if task not in done:
            task.cancel()
    await asyncio.gather(*(t for t in tasks if t not in done), return_exceptions=True)

    return work.result() if work in done else None
