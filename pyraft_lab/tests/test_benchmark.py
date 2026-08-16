"""Phase 10, 2.0 item 11: the benchmarking sweep.

Four things are under test:

1. cell generation is deterministic and pure - the same ``(base_seed, nodes,
   loss_rate)`` always produces the same seed and the same scenario, and a 0% cell
   carries no packet-loss fault at all (a clean baseline, not a `set_loss(0.0)` no-op).
2. an end-to-end sweep produces one cell per ``(nodes, loss_rate)`` combination, and
   every cell's report carries the full metric set 2.0 item 11 asks for.
3. CPU/memory sampling actually measures something, per docs/architecture.md P10.
4. persist-on-failure is wired correctly, tested in isolation by forcing a data-loss
   cell rather than by trying to make Raft actually lose data.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from pyraft_lab.client.workload import WorkloadConfig
from pyraft_lab.experiments import benchmark
from pyraft_lab.experiments.benchmark import (
    BenchmarkConfig,
    benchmark_scenario,
    cell_seed,
    run_benchmark,
)
from pyraft_lab.experiments.scenario import CRASH, PACKET_LOSS, RECOVER


def test_cell_seed_is_a_pure_function_of_its_inputs() -> None:
    assert cell_seed(3, 5, 0.10) == cell_seed(3, 5, 0.10)
    assert cell_seed(3, 5, 0.10) != cell_seed(3, 5, 0.20)
    assert cell_seed(3, 5, 0.10) != cell_seed(3, 7, 0.10)
    assert cell_seed(3, 5, 0.10) != cell_seed(4, 5, 0.10)


def test_zero_loss_cell_carries_no_packet_loss_fault() -> None:
    scenario = benchmark_scenario(
        nodes=3, loss_rate=0.0, duration=10.0, seed=1, workload=WorkloadConfig()
    )
    actions = [step.action for step in scenario.faults]
    assert PACKET_LOSS not in actions
    assert CRASH in actions and RECOVER in actions


def test_lossy_cell_carries_a_packet_loss_fault_from_the_start() -> None:
    scenario = benchmark_scenario(
        nodes=3, loss_rate=0.1, duration=10.0, seed=1, workload=WorkloadConfig()
    )
    loss_steps = [s for s in scenario.faults if s.action == PACKET_LOSS]
    assert len(loss_steps) == 1
    assert loss_steps[0].at == 0.0
    assert loss_steps[0].params["rate"] == 0.1


def test_generating_the_same_cell_twice_is_identical() -> None:
    first = benchmark_scenario(
        nodes=5, loss_rate=0.05, duration=12.0, seed=cell_seed(2, 5, 0.05),
        workload=WorkloadConfig(),
    )
    second = benchmark_scenario(
        nodes=5, loss_rate=0.05, duration=12.0, seed=cell_seed(2, 5, 0.05),
        workload=WorkloadConfig(),
    )
    assert first.to_dict() == second.to_dict()


async def test_run_benchmark_produces_one_cell_per_combination() -> None:
    config = BenchmarkConfig(
        node_counts=(3, 5), loss_rates=(0.0, 0.10), trial_duration=3.0, base_seed=1
    )

    report = await run_benchmark(config)
    document = report.to_dict()

    assert document["cells"] == 4
    combos = {(c["nodes"], c["loss_rate"]) for c in document["results"]}
    assert combos == {(3, 0.0), (3, 0.10), (5, 0.0), (5, 0.10)}


async def test_every_cell_reports_the_full_metric_set() -> None:
    config = BenchmarkConfig(
        node_counts=(3,), loss_rates=(0.0,), trial_duration=3.0, base_seed=2
    )

    report = await run_benchmark(config)
    document = report.to_dict()

    assert set(document) >= {
        "node_counts", "loss_rates", "trial_duration_sec", "base_seed",
        "cells", "data_loss_cells", "results",
    }
    cell = document["results"][0]
    assert set(cell) >= {
        "nodes", "loss_rate", "seed", "run_id", "throughput_per_sec",
        "latency_p50_ms", "latency_p95_ms", "latency_p99_ms",
        "election_time_ms", "recovery_time_ms", "cpu_seconds", "peak_memory_mb",
        "data_loss", "duration_sec", "persisted_at",
    }
    # The initial election in every run's _settle() always gives election_time_ms a
    # sample; the crash/recover pair benchmark_scenario adds always gives recovery one.
    assert cell["election_time_ms"] is not None
    assert cell["recovery_time_ms"] is not None


async def test_cpu_and_memory_are_actually_measured() -> None:
    config = BenchmarkConfig(
        node_counts=(3,), loss_rates=(0.0,), trial_duration=3.0, base_seed=3
    )

    report = await run_benchmark(config)
    cell = report.cells[0]

    assert cell.cpu_seconds >= 0.0
    assert cell.peak_memory_mb > 0.0


async def test_a_data_loss_cell_is_persisted_and_a_clean_one_is_not(
    tmp_path: Path, monkeypatch
) -> None:
    calls = {"n": 0}

    def fake_report(result, *, check_consistency=True):
        calls["n"] += 1
        lossy = calls["n"] == 1
        return {
            "summary": {"data_loss": lossy, "throughput_per_sec": 5.0},
            "timing": {
                "p50_commit_latency_ms": 10.0,
                "p95_commit_latency_ms": 20.0,
                "p99_commit_latency_ms": 30.0,
            },
            "leadership": {"election_time_ms": 100.0, "recovery_time_ms": 200.0},
            "duration_sec": 3.0,
        }

    monkeypatch.setattr(benchmark.metrics, "report", fake_report)

    config = BenchmarkConfig(
        node_counts=(3,), loss_rates=(0.0, 0.05), trial_duration=1.0, base_seed=4,
        results_dir=tmp_path,
    )
    report = await run_benchmark(config)

    lossy_cell, clean_cell = report.cells
    assert lossy_cell.data_loss is True
    assert lossy_cell.persisted_at is not None
    assert Path(lossy_cell.persisted_at, "manifest.json").exists()

    assert clean_cell.data_loss is False
    assert clean_cell.persisted_at is None


def test_summary_includes_every_cell() -> None:
    from pyraft_lab.experiments.benchmark import BenchmarkCell, BenchmarkReport

    config = BenchmarkConfig(node_counts=(3,), loss_rates=(0.0,), base_seed=0)
    cell = BenchmarkCell(
        nodes=3, loss_rate=0.0, seed=1, run_id="abc123",
        throughput_per_sec=9.5, latency_p50_ms=12.0, latency_p95_ms=25.0,
        latency_p99_ms=40.0, election_time_ms=150.0, recovery_time_ms=None,
        cpu_seconds=0.5, peak_memory_mb=2.0, data_loss=False, duration_sec=15.0,
        persisted_at=None,
    )
    text = BenchmarkReport(config=config, cells=[cell]).summary()

    assert "abc123" not in text  # run_id isn't in the table, but nothing should crash
    assert "9.50" in text
    assert "n/a" in text  # recovery_time_ms is None


@pytest.mark.slow
async def test_the_default_grid_covers_every_combination() -> None:
    config = BenchmarkConfig(trial_duration=8.0, base_seed=5)

    report = await run_benchmark(config)

    combos = {(c.nodes, c.loss_rate) for c in report.cells}
    expected = {(n, r) for n in config.node_counts for r in config.loss_rates}
    assert combos == expected
    assert len(report.cells) == len(config.node_counts) * len(config.loss_rates)
