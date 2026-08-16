"""Day 8: the two ``Clock`` implementations node.py and the simulator run against."""

from __future__ import annotations

import asyncio

import pytest

from pyraft_lab.network.clock import Clock, TimerHandle, VirtualClock, WallClock, race_deadline


def test_wall_clock_satisfies_the_clock_protocol() -> None:
    assert isinstance(WallClock(), Clock)


def test_virtual_clock_satisfies_the_clock_protocol() -> None:
    assert isinstance(VirtualClock(), Clock)


def test_virtual_clock_call_at_returns_something_cancellable() -> None:
    assert isinstance(VirtualClock().call_at(1.0, lambda: None), TimerHandle)


async def test_wall_clock_call_at_returns_something_cancellable() -> None:
    handle = WallClock().call_at(WallClock().now() + 10.0, lambda: None)
    assert isinstance(handle, TimerHandle)
    handle.cancel()  # must not raise


async def test_wall_clock_now_tracks_the_event_loop() -> None:
    clock = WallClock()
    before = asyncio.get_running_loop().time()
    assert clock.now() >= before


async def test_wall_clock_sleep_until_waits_the_remaining_time() -> None:
    clock = WallClock()
    start = clock.now()
    await clock.sleep_until(start + 0.02)

    # Not an exact ``>= 0.02``: the event loop's timer has finite resolution, and on
    # Windows ``asyncio.sleep`` returns up to ~5ms early against ``loop.time()`` for
    # about 8% of sleeps - measured, not guessed. Asserting to the microsecond tests the
    # platform's timer granularity rather than this method. What ``sleep_until`` owes a
    # caller is that it waits substantially the requested time instead of returning
    # straight away, and the past-deadline case below covers the other direction.
    elapsed = clock.now() - start
    assert elapsed >= 0.014


async def test_wall_clock_sleep_until_a_past_deadline_returns_immediately() -> None:
    clock = WallClock()
    await clock.sleep_until(clock.now() - 1.0)  # already passed; must not block


def test_virtual_clock_starts_at_zero_by_default() -> None:
    assert VirtualClock().now() == 0.0


def test_virtual_clock_can_start_elsewhere() -> None:
    assert VirtualClock(start=100.0).now() == 100.0


async def test_virtual_clock_does_not_move_on_its_own() -> None:
    clock = VirtualClock()
    await asyncio.sleep(0.05)  # real time passes; virtual time must not follow
    assert clock.now() == 0.0


async def test_virtual_clock_sleep_until_a_past_deadline_returns_immediately() -> None:
    clock = VirtualClock(start=10.0)
    await clock.sleep_until(5.0)  # already behind - no waiter should be registered
    assert clock.pending() == 0


async def test_advance_wakes_a_waiter_whose_deadline_has_passed() -> None:
    clock = VirtualClock()
    woke = asyncio.Event()

    async def waiter() -> None:
        await clock.sleep_until(5.0)
        woke.set()

    task = asyncio.create_task(waiter())
    await asyncio.sleep(0)  # let the waiter register itself
    assert clock.pending() == 1

    await clock.advance(4.0)
    assert not woke.is_set()  # 4.0 < 5.0, not due yet

    await clock.advance(1.0)
    assert woke.is_set()
    assert clock.now() == 5.0
    await task


async def test_advance_wakes_waiters_in_deadline_order() -> None:
    clock = VirtualClock()
    order: list[str] = []

    async def waiter(name: str, deadline: float) -> None:
        await clock.sleep_until(deadline)
        order.append(name)

    tasks = [
        asyncio.create_task(waiter("late", 3.0)),
        asyncio.create_task(waiter("early", 1.0)),
        asyncio.create_task(waiter("mid", 2.0)),
    ]
    await asyncio.sleep(0)

    await clock.advance(3.0)
    await asyncio.gather(*tasks)

    assert order == ["early", "mid", "late"]


async def test_advance_past_several_deadlines_at_once_wakes_all_of_them() -> None:
    clock = VirtualClock()
    woke: list[int] = []

    async def waiter(deadline: float) -> None:
        await clock.sleep_until(deadline)
        woke.append(int(deadline))

    tasks = [asyncio.create_task(waiter(d)) for d in (1.0, 2.0, 3.0)]
    await asyncio.sleep(0)

    await clock.advance(10.0)
    await asyncio.gather(*tasks)

    assert sorted(woke) == [1, 2, 3]


async def test_advance_to_moves_time_even_with_no_waiters() -> None:
    clock = VirtualClock()
    await clock.advance_to(42.0)
    assert clock.now() == 42.0


async def test_advance_to_rejects_moving_backward() -> None:
    clock = VirtualClock(start=5.0)
    with pytest.raises(ValueError, match="backward"):
        await clock.advance_to(4.0)


# --- cancellation: a lost race must not leak its timer ----------------------------
#
# ``race_deadline`` cancels whichever side of a race loses. On a busy cluster that is
# almost always the ``sleep_until`` side - a message beats the deadline nearly every
# time - so this is the ordinary case, not an edge case, and it happens on every single
# wait a node or client ever does. A leak here does not fail loudly: the run keeps
# going, just slower and slower, which is why this needed a regression test of its own
# rather than trusting the visible symptom (a hung experiment) to point back here.


async def test_cancelling_a_waiter_removes_its_timer() -> None:
    clock = VirtualClock()
    fired = asyncio.Event()

    async def waiter() -> None:
        await clock.sleep_until(5.0)
        fired.set()  # must never run - the waiter is cancelled before its deadline

    task = asyncio.create_task(waiter())
    await asyncio.sleep(0)
    assert clock.pending() == 1

    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert clock.pending() == 0
    await clock.advance(10.0)
    assert not fired.is_set()


async def test_a_cancelled_timer_does_not_appear_as_the_next_deadline() -> None:
    """The failure mode this actually caused: a driver reading ``next_deadline`` to
    decide how far to jump must not be told a cancelled timer is still coming, or it
    wastes a whole step retiring something that was never going to fire.
    """
    clock = VirtualClock()

    async def waiter(deadline: float) -> None:
        await clock.sleep_until(deadline)

    soon = asyncio.create_task(waiter(1.0))
    later = asyncio.create_task(waiter(10.0))
    await asyncio.sleep(0)
    assert clock.next_deadline() == 1.0

    soon.cancel()
    with pytest.raises(asyncio.CancelledError):
        await soon

    assert clock.next_deadline() == 10.0
    await clock.advance(10.0)
    await later


async def test_many_lost_races_do_not_accumulate() -> None:
    """The shape of the actual bug: hundreds of waits, each racing a fast winner and
    losing, on a clock that never advances between them. Before the fix, every one of
    these left a dead entry behind; ``pending()`` should stay at zero throughout.
    """
    clock = VirtualClock()

    async def race_and_lose() -> None:
        loser = asyncio.ensure_future(clock.sleep_until(clock.now() + 300.0))
        winner_won = asyncio.Event()
        winner_won.set()
        await winner_won.wait()  # the "message arrived first" side of the race
        loser.cancel()
        with pytest.raises(asyncio.CancelledError):
            await loser

    for _ in range(500):
        await race_and_lose()

    assert clock.pending() == 0


async def test_race_deadline_losing_repeatedly_does_not_leak() -> None:
    """The exact shape of the real bug: ``race_deadline`` on a ``VirtualClock``, racing
    a deadline that is always still far in the future against work that always finishes
    first - a node reacting to a message that beats its election timeout, over and over,
    on a cluster under load. Before the ``sleep_until`` fix, every one of these left a
    dead timer in the heap; a busy run could accumulate tens of thousands of them
    without the clock ever advancing far enough to retire any of it.
    """
    clock = VirtualClock()

    async def instant() -> str:
        return "message arrived"

    for _ in range(500):
        result = await race_deadline(clock, instant(), clock.now() + 300.0)
        assert result == "message arrived"

    assert clock.pending() == 0


async def test_a_timer_that_already_fired_tolerates_a_late_cancel() -> None:
    """Cancelling after the deadline has already been reached and the callback has
    already run must not raise or resurrect anything - the handle is just inert by then.
    """
    clock = VirtualClock()
    event = asyncio.Event()
    handle = clock.call_at(clock.now(), event.set)  # fires immediately: deadline <= now

    assert event.is_set()
    handle.cancel()  # no-op, must not raise
    assert clock.pending() == 0


async def test_a_zero_delay_rearm_is_caught_by_the_same_advance() -> None:
    """A task that reacts to being woken by immediately re-arming at the same instant
    (a heartbeat re-scheduling itself) must resolve within one ``advance`` call, not
    be stranded until some later one.
    """
    clock = VirtualClock()
    ticks: list[float] = []

    async def ticker() -> None:
        deadline = 1.0
        for _ in range(3):
            await clock.sleep_until(deadline)
            ticks.append(clock.now())
            deadline = clock.now()  # re-arm at the instant we just woke at

    task = asyncio.create_task(ticker())
    await asyncio.sleep(0)

    await clock.advance(1.0)

    assert ticks == [1.0, 1.0, 1.0]
    await task
