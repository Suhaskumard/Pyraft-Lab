"""Phase 11, 2.0 item 3: the local dashboard.

Four things are under test:

1. ``collect_snapshot`` reads the right values off a real, running cluster - term,
   leader, per-node role, the leader's own commit/applied/log-length, and (once some
   traffic has actually gone through ``manager.client``) p99 latency and availability.
2. ``render`` is pure and ASCII-only - a regression guard for the Windows-console
   encoding discipline the rest of the project already follows (``consistency/
   history.py``'s ``Operation.describe()``), and the reason this phase didn't just
   reproduce 2.0's own box-drawing mock-up literally.
3. ``run_dashboard``'s bounded ``refreshes`` mode prints exactly that many frames and
   returns - the deterministic path every other test drives, mirroring how
   ``test_cli_repl.py`` drives ``run_repl`` with an injected ``read_line``.
4. a ``KeyboardInterrupt`` raised out of the injected ``sleep`` is swallowed here, not
   left to propagate - Ctrl+C must back out of the dashboard, not the whole session.
"""

from __future__ import annotations

from tests.test_node_election import FAST_ELECTION, FAST_HEARTBEAT

from pyraft_lab.cluster.config import ClusterConfig
from pyraft_lab.cluster.manager import ClusterManager
from pyraft_lab.dashboard.app import (
    DashboardSnapshot,
    NodeRow,
    collect_snapshot,
    render,
    run_dashboard,
)


def a_config(**overrides: object) -> ClusterConfig:
    data = {
        "nodes": 3,
        "election_timeout_range": FAST_ELECTION,
        "heartbeat_interval": FAST_HEARTBEAT,
    }
    data.update(overrides)
    return ClusterConfig(**data)


def a_snapshot(**overrides: object) -> DashboardSnapshot:
    data = {
        "nodes": 3,
        "term": 5,
        "leader": "n2",
        "status": "HEALTHY",
        "rows": (
            NodeRow("n1", True, "follower"),
            NodeRow("n2", True, "leader"),
            NodeRow("n3", True, "follower"),
        ),
        "commit_index": 42,
        "applied": 42,
        "log_entries": 42,
        "election_time_ms": 180.0,
        "p99_latency_ms": 12.5,
        "availability": 1.0,
        "elections_started": 1,
        "dropped_packets": 0,
        "dropped_by_loss": 0,
        "dropped_by_partition": 0,
    }
    data.update(overrides)
    return DashboardSnapshot(**data)


# --- collect_snapshot: real work against a real, running cluster ---------------------------


async def test_collect_snapshot_reads_term_leader_and_roles() -> None:
    manager = ClusterManager(a_config())
    await manager.start()
    try:
        snapshot = collect_snapshot(manager)
        assert snapshot.nodes == 3
        assert snapshot.leader in {"n1", "n2", "n3"}
        assert snapshot.status == "HEALTHY"
        roles = {row.node_id: row.role for row in snapshot.rows}
        assert roles[snapshot.leader] == "leader"
    finally:
        await manager.stop()


async def test_collect_snapshot_reads_commit_applied_and_log_length_from_the_leader() -> None:
    manager = ClusterManager(a_config())
    await manager.start()
    try:
        assert manager.client is not None
        await manager.client.put("a", "1")

        snapshot = collect_snapshot(manager)
        assert snapshot.commit_index is not None and snapshot.commit_index >= 1
        assert snapshot.applied == snapshot.commit_index
        assert snapshot.log_entries is not None and snapshot.log_entries >= snapshot.commit_index
    finally:
        await manager.stop()


async def test_collect_snapshot_has_no_latency_data_before_any_traffic() -> None:
    manager = ClusterManager(a_config())
    await manager.start()
    try:
        snapshot = collect_snapshot(manager)
        assert snapshot.p99_latency_ms is None
        assert snapshot.availability is None
    finally:
        await manager.stop()


async def test_collect_snapshot_reports_p99_and_availability_after_traffic() -> None:
    manager = ClusterManager(a_config())
    await manager.start()
    try:
        assert manager.client is not None
        for i in range(5):
            await manager.client.put(f"k{i}", i)

        snapshot = collect_snapshot(manager)
        assert snapshot.p99_latency_ms is not None and snapshot.p99_latency_ms >= 0
        assert snapshot.availability == 1.0
    finally:
        await manager.stop()


# --- render: pure formatting -----------------------------------------------------------


def test_render_shows_the_leader_in_capitals_and_followers_titlecased() -> None:
    text = render(a_snapshot())
    assert "LEADER" in text
    assert "Follower" in text


def test_render_shows_missing_data_as_na_not_zero() -> None:
    text = render(a_snapshot(election_time_ms=None, p99_latency_ms=None, availability=None))
    assert text.count("n/a") == 3


def test_render_is_plain_ascii() -> None:
    """A Windows console under its default code page cannot encode arbitrary Unicode -
    see consistency/history.py's Operation.describe() for the same discipline applied
    here, deliberately, instead of 2.0's own box-drawing mock-up characters."""
    text = render(a_snapshot())
    assert text.isascii()


def test_render_marks_a_down_node() -> None:
    snapshot = a_snapshot(
        rows=(
            NodeRow("n1", False, "follower"),
            NodeRow("n2", True, "leader"),
            NodeRow("n3", True, "follower"),
        )
    )
    assert "DOWN" in render(snapshot)


# --- run_dashboard: the only piece that loops and sleeps -----------------------------------


async def test_run_dashboard_prints_exactly_the_requested_number_of_frames() -> None:
    manager = ClusterManager(a_config())
    await manager.start()
    printed: list[str] = []
    sleeps: list[float] = []

    async def fake_sleep(seconds: float) -> None:
        sleeps.append(seconds)

    try:
        await run_dashboard(
            manager, refreshes=3, interval=0.25, sleep=fake_sleep, echo=printed.append
        )
    finally:
        await manager.stop()

    frames = [line for line in printed if line.startswith("-- refresh")]
    assert frames == ["-- refresh 1 --", "-- refresh 2 --", "-- refresh 3 --"]
    assert sleeps == [0.25, 0.25]  # slept between frames, not after the last one


async def test_run_dashboard_swallows_a_keyboard_interrupt_from_sleep() -> None:
    manager = ClusterManager(a_config())
    await manager.start()
    printed: list[str] = []

    async def interrupting_sleep(seconds: float) -> None:
        raise KeyboardInterrupt

    try:
        await run_dashboard(  # must not raise
            manager, refreshes=None, interval=0.01, sleep=interrupting_sleep, echo=printed.append
        )
    finally:
        await manager.stop()

    frames = [line for line in printed if line.startswith("-- refresh")]
    assert frames == ["-- refresh 1 --"]  # one frame printed before the interrupt


async def test_run_dashboard_default_echo_and_sleep_do_not_require_injection() -> None:
    """Smoke test: the real defaults (``print``, ``asyncio.sleep``) work end to end
    for a single bounded refresh, which is what a real terminal session gets."""
    manager = ClusterManager(a_config())
    await manager.start()
    try:
        await run_dashboard(manager, refreshes=1, interval=0.01)
    finally:
        await manager.stop()
