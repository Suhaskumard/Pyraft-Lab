"""Day 8: the two ``Clock`` implementations node.py and the simulator run against."""

from __future__ import annotations

import asyncio

import pytest

from pyraft_lab.network.clock import Clock, VirtualClock, WallClock


def test_wall_clock_satisfies_the_clock_protocol() -> None:
    assert isinstance(WallClock(), Clock)


def test_virtual_clock_satisfies_the_clock_protocol() -> None:
    assert isinstance(VirtualClock(), Clock)


async def test_wall_clock_now_tracks_the_event_loop() -> None:
    clock = WallClock()
    before = asyncio.get_running_loop().time()
    assert clock.now() >= before


async def test_wall_clock_sleep_until_waits_the_remaining_time() -> None:
    clock = WallClock()
    start = clock.now()
    await clock.sleep_until(start + 0.02)
    assert clock.now() - start >= 0.02


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
