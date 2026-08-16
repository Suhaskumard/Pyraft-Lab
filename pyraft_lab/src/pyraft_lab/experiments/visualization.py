"""Plots: latency over time, who was leading when, and how far behind the replicas ran.

Three pictures, each answering a question a table of percentiles cannot. A p99 tells
you the tail was bad; the latency plot tells you it was bad *at 10 seconds, when the
leader crashed*, which is the difference between a number and an explanation.

Matplotlib is imported inside the functions, and only ever with the ``Agg`` backend.
Importing it at module scope would put a second of import cost on every ``pyraft-lab``
invocation including ``version``, and choosing a backend that wants a display would
make plotting fail on exactly the machines experiments run on - CI, and a server over
ssh.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from pyraft_lab.consistency.history import OpKind
from pyraft_lab.experiments.runner import RunResult
from pyraft_lab.observability.events import EventKind

MS = 1000.0

#: Distinct enough in greyscale to survive being printed into a report.
LEADER_COLORS = ("#4C72B0", "#DD8452", "#55A868", "#C44E52", "#8172B3", "#937860")


def _pyplot() -> Any:
    import matplotlib

    matplotlib.use("Agg", force=True)
    import matplotlib.pyplot as plt

    return plt


def _fault_markers(result: RunResult, axis: Any) -> None:
    """Draw a dashed line wherever the scenario broke something.

    Every plot gets these. A latency spike with no marker beside it is a different
    finding from one that lines up with a crash, and the reader should not have to
    cross-reference a JSON file to tell which they are looking at.
    """
    for fault in result.faults:
        if not fault.applied:
            continue
        axis.axvline(fault.at, color="#B0B0B0", linestyle="--", linewidth=0.9, zorder=0)
        axis.annotate(
            fault.action.replace("_", " "),
            xy=(fault.at, 1.0),
            xycoords=("data", "axes fraction"),
            xytext=(2, -10),
            textcoords="offset points",
            fontsize=7,
            color="#666666",
            rotation=90,
            va="top",
        )


def plot_latency(result: RunResult, path: Path) -> Path:
    """Every operation, plotted where it happened and how long it took."""
    plt = _pyplot()
    figure, axis = plt.subplots(figsize=(10, 4))

    for kind, color, label in (
        (OpKind.PUT, "#4C72B0", "writes"),
        (OpKind.GET, "#55A868", "reads"),
    ):
        points = [
            (op.completed_at, (op.duration or 0.0) * MS)
            for op in result.history.completed
            if op.kind is kind
        ]
        if points:
            xs, ys = zip(*points, strict=True)
            axis.scatter(xs, ys, s=8, alpha=0.65, color=color, label=label)

    # Operations that never came back have no duration to plot, but leaving them out
    # entirely would hide the outage they are evidence of.
    for op in result.history.incomplete:
        axis.axvline(op.invoked_at, color="#C44E52", alpha=0.25, linewidth=0.8, zorder=0)

    _fault_markers(result, axis)
    axis.set_xlabel("time (s)")
    axis.set_ylabel("latency (ms)")
    axis.set_title(f"{result.scenario.name} - client latency")
    axis.set_xlim(0, max(result.duration, 0.001))
    if result.history.completed:
        axis.legend(loc="upper left", frameon=False, fontsize=8)
    axis.grid(alpha=0.2)

    figure.tight_layout()
    figure.savefig(path, dpi=120)
    plt.close(figure)
    return path


def plot_leadership(result: RunResult, path: Path) -> Path:
    """A bar per node showing the stretches in which it was the elected leader."""
    plt = _pyplot()
    figure, axis = plt.subplots(figsize=(10, 3.2))

    ids = [node.node_id for node in result.nodes]
    rows = {node_id: index for index, node_id in enumerate(ids)}

    elections = [e for e in result.events if e.kind is EventKind.LEADER_ELECTED and e.node]
    for index, event in enumerate(elections):
        end = elections[index + 1].timestamp if index + 1 < len(elections) else result.duration
        row = rows.get(event.node or "", 0)
        axis.broken_barh(
            [(event.timestamp, max(end - event.timestamp, 1e-4))],
            (row - 0.35, 0.7),
            facecolors=LEADER_COLORS[(event.term or 0) % len(LEADER_COLORS)],
        )

    for event in result.events:
        if event.kind is EventKind.NODE_CRASHED and event.node in rows:
            axis.plot(event.timestamp, rows[event.node or ""], "x", color="#C44E52", markersize=7)
        elif event.kind is EventKind.NODE_RECOVERED and event.node in rows:
            axis.plot(event.timestamp, rows[event.node or ""], "o", color="#55A868", markersize=5)

    _fault_markers(result, axis)
    axis.set_yticks(range(len(ids)))
    axis.set_yticklabels(ids)
    axis.set_xlabel("time (s)")
    axis.set_title(f"{result.scenario.name} - leadership (x = crash, o = rejoin)")
    axis.set_xlim(0, max(result.duration, 0.001))
    axis.grid(axis="x", alpha=0.2)

    figure.tight_layout()
    figure.savefig(path, dpi=120)
    plt.close(figure)
    return path


def plot_replication(result: RunResult, path: Path) -> Path:
    """Each node's commit index over time, and the spread between them."""
    plt = _pyplot()
    figure, (top, bottom) = plt.subplots(
        2, 1, figsize=(10, 5), sharex=True, gridspec_kw={"height_ratios": [2, 1]}
    )

    series: dict[str, list[tuple[float, int]]] = {n.node_id: [(0.0, 0)] for n in result.nodes}
    commit = dict.fromkeys(series, 0)
    spread: list[tuple[float, int]] = [(0.0, 0)]

    for event in result.events:
        if event.kind is not EventKind.LOG_COMMITTED or event.node not in commit:
            continue
        commit[event.node or ""] = int(event.details.get("to", 0))
        series[event.node or ""].append((event.timestamp, commit[event.node or ""]))
        spread.append((event.timestamp, max(commit.values()) - min(commit.values())))

    for index, (node_id, points) in enumerate(sorted(series.items())):
        xs, ys = zip(*points, strict=True)
        top.step(
            xs,
            ys,
            where="post",
            label=node_id,
            linewidth=1.2,
            color=LEADER_COLORS[index % len(LEADER_COLORS)],
        )

    xs, ys = zip(*spread, strict=True)
    bottom.fill_between(xs, ys, step="post", color="#C44E52", alpha=0.35)

    for axis in (top, bottom):
        _fault_markers(result, axis)
        axis.grid(alpha=0.2)
        axis.set_xlim(0, max(result.duration, 0.001))

    top.set_ylabel("commit index")
    top.set_title(f"{result.scenario.name} - replication")
    top.legend(loc="upper left", frameon=False, fontsize=8, ncol=len(series))
    bottom.set_ylabel("lag (entries)")
    bottom.set_xlabel("time (s)")

    figure.tight_layout()
    figure.savefig(path, dpi=120)
    plt.close(figure)
    return path


def plot_consistency_availability(result: RunResult, path: Path) -> Path:
    """The "why consensus works" picture (Phase 10, 2.0 item 12): each node's committed
    prefix over time, and every client operation's outcome, on one page.

    Not a new measurement - everything here is already in ``plot_replication``'s commit
    series and ``plot_latency``'s operation scatter, just combined for a presentation
    rather than split across two diagnostic plots. A node cut off by a partition shows
    up as a flat line while the rest keep climbing; healing is the moment it turns
    upward again and catches the others. See docs/architecture.md P10 for why this is
    the generic, reusable form item 12's demo takes rather than a scenario-specific one.
    """
    plt = _pyplot()
    figure, (top, bottom) = plt.subplots(
        2, 1, figsize=(10, 5.5), sharex=True, gridspec_kw={"height_ratios": [2, 1]}
    )

    series: dict[str, list[tuple[float, int]]] = {n.node_id: [(0.0, 0)] for n in result.nodes}
    commit = dict.fromkeys(series, 0)
    for event in result.events:
        if event.kind is not EventKind.LOG_COMMITTED or event.node not in commit:
            continue
        commit[event.node or ""] = int(event.details.get("to", 0))
        series[event.node or ""].append((event.timestamp, commit[event.node or ""]))

    # A node cut off by a partition has no further LOG_COMMITTED events at all - its
    # series would otherwise just stop at its last one, which draws as the line
    # vanishing rather than as the flat stall it actually is. Carrying the last value
    # forward to the run's end is what makes a stalled minority visible as a plateau
    # instead of an absence.
    for node_id, points in series.items():
        if points[-1][0] < result.duration:
            points.append((result.duration, commit[node_id]))

    for index, (node_id, points) in enumerate(sorted(series.items())):
        xs, ys = zip(*points, strict=True)
        top.step(
            xs,
            ys,
            where="post",
            label=node_id,
            linewidth=1.3,
            color=LEADER_COLORS[index % len(LEADER_COLORS)],
        )
    top.set_ylabel("commit index")
    top.set_title(f"{result.scenario.name} - consistency vs availability")
    top.legend(loc="upper left", frameon=False, fontsize=8, ncol=len(series))

    for op in result.history.completed:
        marker, color = ("o", "#55A868") if op.ok else ("x", "#C44E52")
        bottom.scatter(op.completed_at, 1, marker=marker, s=18, color=color, alpha=0.7)
    for op in result.history.incomplete:
        bottom.axvline(op.invoked_at, color="#C44E52", alpha=0.25, linewidth=0.8, zorder=0)
    bottom.set_yticks([])
    bottom.set_ylabel("writes/reads\n(o=accepted, x=rejected)")
    bottom.set_xlabel("time (s)")

    for axis in (top, bottom):
        _fault_markers(result, axis)
        axis.grid(alpha=0.2)
        axis.set_xlim(0, max(result.duration, 0.001))

    figure.tight_layout()
    figure.savefig(path, dpi=120)
    plt.close(figure)
    return path


def plot_run(result: RunResult, directory: str | Path, *, prefix: str | None = None) -> list[Path]:
    """Write every plot for a run. Returns the paths written, in order."""
    directory = Path(directory)
    directory.mkdir(parents=True, exist_ok=True)
    stem = prefix or f"{_slug(result.scenario.name)}-{result.run_id}"

    return [
        plot_latency(result, directory / f"{stem}-latency.png"),
        plot_leadership(result, directory / f"{stem}-leadership.png"),
        plot_replication(result, directory / f"{stem}-replication.png"),
        plot_consistency_availability(result, directory / f"{stem}-consistency.png"),
    ]


#: Distinguishes each node count's line the same way ``LEADER_COLORS`` distinguishes a
#: term's - reused here rather than duplicated, since a benchmark comparison plot has
#: exactly the same "a handful of distinct series, greyscale-safe" requirement.
_BENCHMARK_METRICS: dict[str, tuple[tuple[str, str], ...]] = {
    "throughput": (("throughput_per_sec", "throughput (ops/s)"),),
    "latency": (
        ("latency_p50_ms", "p50 (ms)"),
        ("latency_p95_ms", "p95 (ms)"),
        ("latency_p99_ms", "p99 (ms)"),
    ),
    "election-recovery": (
        ("election_time_ms", "election time (ms)"),
        ("recovery_time_ms", "recovery time (ms)"),
    ),
    "resources": (
        ("cpu_seconds", "CPU (s)"),
        ("peak_memory_mb", "peak memory (MB)"),
    ),
}


def _plot_benchmark_series(
    by_nodes: dict[int, list[Any]], path: Path, fields: tuple[tuple[str, str], ...], *, title: str
) -> Path:
    plt = _pyplot()
    figure, axes = plt.subplots(len(fields), 1, figsize=(8, 2.8 * len(fields)), sharex=True)
    axes = [axes] if len(fields) == 1 else list(axes)

    for axis, (attr, ylabel) in zip(axes, fields, strict=True):
        for index, (nodes, cells) in enumerate(sorted(by_nodes.items())):
            points = [
                (cell.loss_rate * 100, value)
                for cell in cells
                if (value := getattr(cell, attr)) is not None
            ]
            if not points:
                continue
            xs, ys = zip(*points, strict=True)
            axis.plot(
                xs,
                ys,
                marker="o",
                label=f"{nodes} nodes",
                color=LEADER_COLORS[index % len(LEADER_COLORS)],
            )
        axis.set_ylabel(ylabel)
        axis.grid(alpha=0.2)

    axes[0].set_title(title)
    axes[0].legend(loc="best", frameon=False, fontsize=8)
    axes[-1].set_xlabel("packet loss (%)")

    figure.tight_layout()
    figure.savefig(path, dpi=120)
    plt.close(figure)
    return path


def plot_benchmark(report: Any, directory: str | Path) -> list[Path]:
    """The comparison graphs 2.0 item 11 asks for: throughput, latency percentiles,
    election/recovery time and CPU/memory, each plotted against packet loss with one
    line per cluster size. Takes a ``BenchmarkReport`` (``experiments/benchmark.py``)
    by structural typing rather than importing it, so ``benchmark.py`` never has to
    import this module back.
    """
    directory = Path(directory)
    directory.mkdir(parents=True, exist_ok=True)

    by_nodes: dict[int, list[Any]] = {}
    for cell in report.cells:
        by_nodes.setdefault(cell.nodes, []).append(cell)
    for cells in by_nodes.values():
        cells.sort(key=lambda c: c.loss_rate)

    return [
        _plot_benchmark_series(
            by_nodes,
            directory / f"benchmark-{name}.png",
            fields,
            title=f"{name.replace('-', ' & ')} vs packet loss",
        )
        for name, fields in _BENCHMARK_METRICS.items()
    ]


def _slug(name: str) -> str:
    return "".join(c if c.isalnum() or c in "-_" else "-" for c in name.lower()).strip("-")
