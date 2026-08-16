"""A live, terminal-based view of one running cluster (2.0 item 3).

Two seams, the same split ``cli/repl.py`` already uses:

- :func:`collect_snapshot` does the real work - reading a live
  :class:`~pyraft_lab.cluster.manager.ClusterManager` - and :func:`render` is pure,
  turning a :class:`DashboardSnapshot` into text with no I/O of its own. Splitting
  them means the formatting can be tested against a hand-built snapshot without a
  cluster, and the data collection can be tested without caring how it prints.
- :func:`run_dashboard` is the only piece that loops and sleeps. Its ``sleep``
  argument is injectable, the same way ``run_repl``'s ``read_line`` is, so a test can
  drive a bounded number of refreshes with no real delay and no real terminal.

Commit index, applied index and log length come straight from the current leader's
own :class:`~pyraft_lab.raft.node.RaftNode` (the same way ``cli/repl.py``'s
``node inspect`` already reads them) rather than through
:class:`~pyraft_lab.observability.metrics.LiveMetrics`. That module's whole point
(docs/architecture.md P4) is to stay derivable from the event trace alone and never
reach into a node - extending its vocabulary just so a dashboard could read three
numbers already sitting on the live object would trade that purity for nothing.

P99 latency and availability come from the one :class:`~pyraft_lab.client.client.
KVClient` a live cluster ever has - ``manager.client``, the same client every REPL
command already shares. The dashboard injects no synthetic traffic of its own to fill
those numbers in: if nothing has been asked of the cluster yet, they read ``n/a``,
honestly, rather than a zero that would read as "fast and available" when it actually
means "untested." See docs/architecture.md P11 for the rest of this phase's decisions,
including why the rendering below is plain ASCII rather than the box-drawing
characters 2.0's own mock-up uses.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import dataclass

from pyraft_lab import NodeId
from pyraft_lab.cluster.manager import ClusterError, ClusterManager
from pyraft_lab.experiments.metrics import percentile

MS = 1000.0

Echo = Callable[[str], None]
Sleep = Callable[[float], Awaitable[None]]


@dataclass(frozen=True)
class NodeRow:
    node_id: NodeId
    alive: bool
    role: str | None


@dataclass(frozen=True)
class DashboardSnapshot:
    """Everything one frame needs, already read off the cluster. See the module
    docstring for where each field comes from."""

    nodes: int
    term: int
    leader: NodeId | None
    status: str
    rows: tuple[NodeRow, ...]
    commit_index: int | None
    applied: int | None
    log_entries: int | None
    election_time_ms: float | None
    p99_latency_ms: float | None
    availability: float | None
    elections_started: int
    dropped_packets: int
    dropped_by_loss: int
    dropped_by_partition: int


def collect_snapshot(manager: ClusterManager) -> DashboardSnapshot:
    """Read everything one dashboard frame needs off a live, running cluster."""
    snap = manager.status()
    rows = tuple(NodeRow(row.node_id, row.alive, row.role) for row in manager.topology())

    leader_id = snap["leader"]
    commit_index = applied = log_entries = None
    if leader_id is not None:
        try:
            leader = manager.node(leader_id)
        except ClusterError:
            leader = None
        if leader is not None:
            commit_index = leader.state.commit_index
            applied = leader.state.last_applied
            log_entries = leader.state.log.last_index

    election_ms = None
    if manager.live.last_election_seconds is not None:
        election_ms = manager.live.last_election_seconds * MS

    p99_ms = availability = None
    client = manager.client
    if client is not None:
        metrics = client.metrics
        if metrics.latencies:
            p99_ms = percentile([latency * MS for latency in metrics.latencies], 0.99)
        if metrics.successes + metrics.failures:
            availability = metrics.availability

    stats = manager.network.stats
    return DashboardSnapshot(
        nodes=len(manager.nodes),
        term=snap["term"],
        leader=leader_id,
        status=snap["status"],
        rows=rows,
        commit_index=commit_index,
        applied=applied,
        log_entries=log_entries,
        election_time_ms=election_ms,
        p99_latency_ms=p99_ms,
        availability=availability,
        elections_started=snap["elections_started"],
        dropped_packets=stats.dropped,
        dropped_by_loss=stats.dropped_loss,
        dropped_by_partition=stats.dropped_partition,
    )


def _fmt_ms(value: float | None) -> str:
    return "n/a" if value is None else f"{value:.0f}ms"


def _fmt_pct(value: float | None) -> str:
    return "n/a" if value is None else f"{value * 100:.2f}%"


def _fmt_int(value: int | None) -> str:
    return "n/a" if value is None else f"{value:,}"


def _role_label(row: NodeRow) -> str:
    if not row.alive:
        return "DOWN"
    if row.role is None:
        return "-"
    return "LEADER" if row.role == "leader" else row.role.title()


def render(snapshot: DashboardSnapshot) -> str:
    """The frame text. Plain ASCII throughout, deliberately - see the module
    docstring and docs/architecture.md P11 for why."""
    header = f"Cluster: {snapshot.nodes} nodes"
    term = f"Term: {snapshot.term}"
    leader_line = f"Leader: {snapshot.leader or '(none)'}"
    status_line = f"Status: {snapshot.status}"

    width = max(len(header) + len(term), len(leader_line) + len(status_line)) + 10
    top = "+" + "-" * (width - 2) + "+"

    def box_row(left: str, right: str) -> str:
        gap = width - 4 - len(left) - len(right)
        return f"| {left}{' ' * max(gap, 1)}{right} |"

    column = max(9, max((len(row.node_id) for row in snapshot.rows), default=0) + 1)
    ids = "".join(f"{row.node_id:<{column}}" for row in snapshot.rows)
    roles = "".join(f"{_role_label(row):<{column}}" for row in snapshot.rows)

    lines = [
        "PyRaft Lab Dashboard",
        top,
        box_row(header, term),
        box_row(leader_line, status_line),
        top,
        "",
        ids.rstrip(),
        roles.rstrip(),
        "",
        f"Commit Index:   {_fmt_int(snapshot.commit_index)}",
        f"Applied:        {_fmt_int(snapshot.applied)}",
        f"Log Entries:    {_fmt_int(snapshot.log_entries)}",
        "",
        f"Election Time:  {_fmt_ms(snapshot.election_time_ms)}",
        f"P99 Latency:    {_fmt_ms(snapshot.p99_latency_ms)}",
        f"Availability:   {_fmt_pct(snapshot.availability)}",
        "",
        f"Elections:      {snapshot.elections_started}",
        f"Dropped pkts:   {snapshot.dropped_packets}"
        f"  (loss {snapshot.dropped_by_loss}, partition {snapshot.dropped_by_partition})",
    ]
    return "\n".join(lines)


async def run_dashboard(
    manager: ClusterManager,
    *,
    refreshes: int | None = None,
    interval: float = 1.0,
    sleep: Sleep = asyncio.sleep,
    echo: Echo = print,
) -> None:
    """Render one frame, sleep, repeat - until ``refreshes`` frames have printed, or
    the caller interrupts it.

    Sleeping through ``asyncio.sleep`` rather than a blocking call is what keeps every
    node's own timers running while a frame sits on screen (the same reasoning as
    ``cli/repl.py``'s ``asyncio.to_thread`` read - docs/architecture.md P9). A
    ``KeyboardInterrupt`` raised out of that sleep is caught *here*, not left to
    propagate into ``run_repl``'s own loop, so Ctrl+C backs out of the dashboard and
    returns to the prompt rather than ending the whole session.
    """
    count = 0
    try:
        while refreshes is None or count < refreshes:
            echo(f"-- refresh {count + 1} --")
            echo(render(collect_snapshot(manager)))
            count += 1
            if refreshes is not None and count >= refreshes:
                return
            await sleep(interval)
    except KeyboardInterrupt:
        return
