"""``experiments/visualization.py`` had no direct tests before Phase 10 - it shipped in
Phase 5's Day 12 scope but nothing in the CLI ever called it (``pyraft-lab run``'s
``--plot`` and ``pyraft-lab benchmark`` are the first to, both Phase 10 additions). Not
a full visual-regression suite - just smoke tests that every plotting function runs
against a real run and a real benchmark report and produces a non-empty file, since a
plot that silently fails to write is a lab tool nobody can trust.
"""

from __future__ import annotations

from pathlib import Path

from pyraft_lab.experiments.benchmark import BenchmarkCell, BenchmarkConfig, BenchmarkReport
from pyraft_lab.experiments.runner import run_scenario
from pyraft_lab.experiments.scenario import Scenario
from pyraft_lab.experiments.visualization import (
    plot_benchmark,
    plot_consistency_availability,
    plot_run,
)

A_SCENARIO = Scenario.from_dict(
    {
        "name": "viz-smoke",
        "duration": 5,
        "nodes": 3,
        "seed": 1,
        "faults": [
            {"at": 1.0, "action": "partition", "groups": [[1], [2, 3]]},
            {"at": 3.0, "action": "heal_partition"},
        ],
        "workload": {
            "type": "put_get_mix",
            "puts_per_sec": 15,
            "get_ratio": 0.5,
            "keys": 3,
            "clients": 1,
        },
    }
)


def _non_empty(path: Path) -> bool:
    return path.exists() and path.stat().st_size > 0


async def test_plot_run_writes_all_four_plots(tmp_path: Path) -> None:
    result = await run_scenario(A_SCENARIO)

    paths = plot_run(result, tmp_path)

    assert len(paths) == 4
    assert all(_non_empty(p) for p in paths)
    assert any("consistency" in p.name for p in paths)


async def test_plot_consistency_availability_alone(tmp_path: Path) -> None:
    result = await run_scenario(A_SCENARIO)

    path = plot_consistency_availability(result, tmp_path / "consistency.png")

    assert _non_empty(path)


def _fake_cell(nodes: int, loss_rate: float) -> BenchmarkCell:
    return BenchmarkCell(
        nodes=nodes,
        loss_rate=loss_rate,
        seed=0,
        run_id="x",
        throughput_per_sec=10.0,
        latency_p50_ms=5.0,
        latency_p95_ms=15.0,
        latency_p99_ms=25.0,
        election_time_ms=100.0,
        recovery_time_ms=200.0,
        cpu_seconds=0.1,
        peak_memory_mb=1.0,
        data_loss=False,
        duration_sec=10.0,
        persisted_at=None,
    )


def test_plot_benchmark_writes_four_comparison_graphs(tmp_path: Path) -> None:
    cells = [_fake_cell(nodes, loss_rate) for nodes in (3, 5) for loss_rate in (0.0, 0.10)]
    report = BenchmarkReport(
        config=BenchmarkConfig(node_counts=(3, 5), loss_rates=(0.0, 0.10)), cells=cells
    )

    paths = plot_benchmark(report, tmp_path)

    assert len(paths) == 4
    assert all(_non_empty(p) for p in paths)
    names = {p.stem for p in paths}
    assert names == {
        "benchmark-throughput",
        "benchmark-latency",
        "benchmark-election-recovery",
        "benchmark-resources",
    }
