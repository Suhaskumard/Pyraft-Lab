"""Benchmarking (Phase 10, 2.0 item 11): throughput/latency/election/recovery/CPU/memory
compared across cluster sizes and packet-loss rates.

Extends Phase 5's existing result schema rather than being a new subsystem, exactly as
2.0's own note for this item says: every number here already exists in
``experiments/metrics.py``'s ``report()`` - a benchmark cell is one ordinary
:class:`~pyraft_lab.experiments.runner.ExperimentRunner` run, read back through
``metrics.report()`` the same way ``pyraft-lab run`` does. What is new is only the grid
this module sweeps (``{3,5,7} nodes x {0,5,10,20}% loss``, 2.0's own numbers) and the
CPU/memory sampling `report()` cannot provide on its own.

Every cell carries a constant packet-loss rate for its whole duration (skipped entirely
at 0%, so that cell is a clean, fault-free baseline rather than a `set_loss(0.0)` no-op)
plus one leader crash and recovery partway through - `report()`'s `election_time_ms`
and `recovery_time_ms` are empty unless a run actually has an election to measure, and
the *initial* election that already happens for free in every run's `_settle()` only
gives one of the two.

CPU and memory are measured for the whole cell's ``ExperimentRunner.run()`` via
``time.process_time()`` and ``tracemalloc`` - stdlib, cross-platform, and no new
dependency - rather than per simulated node: every node in a cell runs as a coroutine
in this one process on a shared ``VirtualClock``, so there is no OS-level unit smaller
than the whole run to attribute CPU or memory to. See docs/architecture.md P10.
"""

from __future__ import annotations

import time
import tracemalloc
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from pyraft_lab.client.workload import WorkloadConfig
from pyraft_lab.experiments import metrics
from pyraft_lab.experiments.replay import persist_run
from pyraft_lab.experiments.runner import CLIENT_TIMEOUT, ExperimentRunner, RunResult
from pyraft_lab.experiments.scenario import CRASH, PACKET_LOSS, RECOVER, FaultStep, Scenario
from pyraft_lab.raft.election import ELECTION_TIMEOUT_RANGE, HEARTBEAT_INTERVAL

DEFAULT_NODE_COUNTS: tuple[int, ...] = (3, 5, 7)
DEFAULT_LOSS_RATES: tuple[float, ...] = (0.0, 0.05, 0.10, 0.20)


def cell_seed(base_seed: int, nodes: int, loss_rate: float) -> int:
    """A pure function of ``(base_seed, nodes, loss_rate)`` - a different multiplier
    than ``stress.trial_seed``/``chaos.trial_seed`` so a shared base seed never
    accidentally lines up the same run across any two of these three commands."""
    return base_seed * 3_000_017 + nodes * 1009 + round(loss_rate * 10_000)


def benchmark_scenario(
    *, nodes: int, loss_rate: float, duration: float, seed: int, workload: WorkloadConfig
) -> Scenario:
    """One benchmark cell's scenario: a constant loss rate (if any) for the whole run,
    plus a leader crash and recovery partway through so ``recovery_time_ms`` has
    something to measure - the initial election in every run's ``_settle()`` already
    gives ``election_time_ms`` one sample for free."""
    faults: list[FaultStep] = []
    if loss_rate > 0:
        faults.append(FaultStep(at=0.0, action=PACKET_LOSS, params={"rate": loss_rate}))

    crash_at = duration * 0.4
    recover_at = duration * 0.6
    faults.append(FaultStep(at=crash_at, action=CRASH, target="leader"))
    faults.append(FaultStep(at=recover_at, action=RECOVER))
    faults.sort(key=lambda step: step.at)

    return Scenario(
        name=f"benchmark-{nodes}n-{round(loss_rate * 100)}pct",
        duration=duration,
        nodes=nodes,
        faults=tuple(faults),
        workload=workload,
        expected={},
        seed=seed,
    )


@dataclass(frozen=True)
class BenchmarkConfig:
    node_counts: tuple[int, ...] = DEFAULT_NODE_COUNTS
    loss_rates: tuple[float, ...] = DEFAULT_LOSS_RATES
    trial_duration: float = 15.0
    base_seed: int = 0
    workload: WorkloadConfig = field(default_factory=WorkloadConfig)
    results_dir: Path = Path("results")
    election_timeout_range: tuple[float, float] = ELECTION_TIMEOUT_RANGE
    heartbeat_interval: float = HEARTBEAT_INTERVAL
    client_timeout: float = CLIENT_TIMEOUT


@dataclass(frozen=True)
class BenchmarkCell:
    """Everything measured for one ``(nodes, loss_rate)`` combination."""

    nodes: int
    loss_rate: float
    seed: int
    run_id: str
    throughput_per_sec: float
    latency_p50_ms: float | None
    latency_p95_ms: float | None
    latency_p99_ms: float | None
    election_time_ms: float | None
    recovery_time_ms: float | None
    cpu_seconds: float
    peak_memory_mb: float
    data_loss: bool
    duration_sec: float
    persisted_at: str | None

    def to_dict(self) -> dict[str, Any]:
        return {
            "nodes": self.nodes,
            "loss_rate": self.loss_rate,
            "seed": self.seed,
            "run_id": self.run_id,
            "throughput_per_sec": self.throughput_per_sec,
            "latency_p50_ms": self.latency_p50_ms,
            "latency_p95_ms": self.latency_p95_ms,
            "latency_p99_ms": self.latency_p99_ms,
            "election_time_ms": self.election_time_ms,
            "recovery_time_ms": self.recovery_time_ms,
            "cpu_seconds": self.cpu_seconds,
            "peak_memory_mb": self.peak_memory_mb,
            "data_loss": self.data_loss,
            "duration_sec": self.duration_sec,
            "persisted_at": self.persisted_at,
        }


@dataclass(frozen=True)
class BenchmarkReport:
    config: BenchmarkConfig
    cells: list[BenchmarkCell]

    def to_dict(self) -> dict[str, Any]:
        return {
            "node_counts": list(self.config.node_counts),
            "loss_rates": list(self.config.loss_rates),
            "trial_duration_sec": self.config.trial_duration,
            "base_seed": self.config.base_seed,
            "cells": len(self.cells),
            "data_loss_cells": sum(1 for c in self.cells if c.data_loss),
            "results": [c.to_dict() for c in self.cells],
        }

    def summary(self) -> str:
        document = self.to_dict()
        loss_pct = [r * 100 for r in document["loss_rates"]]
        lines = [
            f"benchmark: nodes={document['node_counts']}  loss%={loss_pct}"
            f"  ({document['trial_duration_sec']:.1f}s/cell, base seed {document['base_seed']})",
            f"  cells         {document['cells']}",
            f"  data loss     {document['data_loss_cells']} cell(s)",
            "",
            f"  {'nodes':>5}  {'loss%':>5}  {'ops/s':>8}  {'p50 ms':>8}  {'p99 ms':>8}"
            f"  {'election ms':>12}  {'recovery ms':>12}  {'cpu s':>7}  {'mem MB':>8}",
        ]
        for cell in self.cells:
            lines.append(
                f"  {cell.nodes:>5}  {cell.loss_rate * 100:>5.0f}  {cell.throughput_per_sec:>8.2f}"
                f"  {_fmt(cell.latency_p50_ms):>8}  {_fmt(cell.latency_p99_ms):>8}"
                f"  {_fmt(cell.election_time_ms):>12}  {_fmt(cell.recovery_time_ms):>12}"
                f"  {cell.cpu_seconds:>7.2f}  {cell.peak_memory_mb:>8.2f}"
            )
        return "\n".join(lines)


def _fmt(value: float | None) -> str:
    return "n/a" if value is None else f"{value:.1f}"


async def _run_cell(config: BenchmarkConfig, nodes: int, loss_rate: float) -> BenchmarkCell:
    seed = cell_seed(config.base_seed, nodes, loss_rate)
    scenario = benchmark_scenario(
        nodes=nodes,
        loss_rate=loss_rate,
        duration=config.trial_duration,
        seed=seed,
        workload=config.workload,
    )

    tracemalloc.start()
    cpu_before = time.process_time()
    try:
        result: RunResult = await ExperimentRunner(
            scenario,
            election_timeout_range=config.election_timeout_range,
            heartbeat_interval=config.heartbeat_interval,
            client_timeout=config.client_timeout,
        ).run()
        cpu_seconds = time.process_time() - cpu_before
        _, peak_bytes = tracemalloc.get_traced_memory()
    finally:
        tracemalloc.stop()

    document = metrics.report(result)
    data_loss = bool(document["summary"]["data_loss"])
    persisted_at = str(persist_run(result, config.results_dir)) if data_loss else None

    return BenchmarkCell(
        nodes=nodes,
        loss_rate=loss_rate,
        seed=seed,
        run_id=result.run_id,
        throughput_per_sec=document["summary"]["throughput_per_sec"],
        latency_p50_ms=document["timing"]["p50_commit_latency_ms"],
        latency_p95_ms=document["timing"]["p95_commit_latency_ms"],
        latency_p99_ms=document["timing"]["p99_commit_latency_ms"],
        election_time_ms=document["leadership"]["election_time_ms"],
        recovery_time_ms=document["leadership"]["recovery_time_ms"],
        cpu_seconds=cpu_seconds,
        peak_memory_mb=peak_bytes / (1024 * 1024),
        data_loss=data_loss,
        duration_sec=document["duration_sec"],
        persisted_at=persisted_at,
    )


async def run_benchmark(config: BenchmarkConfig) -> BenchmarkReport:
    """Run every ``(nodes, loss_rate)`` cell of the grid and collect the results."""
    cells = [
        await _run_cell(config, nodes, loss_rate)
        for nodes in config.node_counts
        for loss_rate in config.loss_rates
    ]
    return BenchmarkReport(config=config, cells=cells)


def write_report(report: BenchmarkReport, path: str | Path) -> Path:
    return metrics.write_report(report.to_dict(), path)
