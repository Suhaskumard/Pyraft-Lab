"""Stress campaigns (2.0 item 7): many trials, each a distinct fault combination,
scored and tallied into one ``stress-report.json``.

A stress trial is not a hand-written scenario - it is *generated*: one entry from a
small, bounded catalog of fault archetypes (crash the leader, crash a follower,
partition off a minority, isolate the leader from a majority, a latency spike, a burst
of packet loss, a churn of several crash/recover cycles, a crash plus a latency spike)
is picked deterministically per trial and built straight into a :class:`Scenario`, no
YAML involved. Cycling through the catalog by ``trial_index`` means N trials sweep
every archetype rather than repeating one, and every random choice inside a builder
(timing, target, split size) is drawn from a ``random.Random`` seeded from
``(base_seed, trial_index)`` alone - nothing here calls the ``random`` module
unseeded, so a campaign is exactly as reproducible as any hand-written scenario.

Trials stay in memory (no WAL): stress's job is coverage across many cheap trials, and
persistence only changes the answer for a restart that keeps its state - that is
chaos's job (``chaos.py``), not this one's. See docs/architecture.md P8.

Only a *failing* trial is persisted via Phase 7's ``persist_run`` - plan.md's own
framing is "a failing trial's seed becomes a replay-able regression case", and writing
a full run directory for every trial of every campaign would grow ``results/`` for no
benefit passing trials don't need.
"""

from __future__ import annotations

import random
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from pyraft_lab.client.workload import WorkloadConfig
from pyraft_lab.experiments import metrics
from pyraft_lab.experiments.replay import persist_run
from pyraft_lab.experiments.runner import CLIENT_TIMEOUT, ExperimentRunner, RunResult
from pyraft_lab.experiments.scenario import (
    CRASH,
    HEAL_PARTITION,
    LATENCY,
    PACKET_LOSS,
    PARTITION,
    RECOVER,
    RESET_FAULTS,
    FaultStep,
    Scenario,
)
from pyraft_lab.raft.election import ELECTION_TIMEOUT_RANGE, HEARTBEAT_INTERVAL

FaultBuilder = Callable[[random.Random, int, float], tuple[FaultStep, ...]]


def _span(rng: random.Random, duration: float, low: float, high: float) -> float:
    """A random instant within ``[low, high]`` fractions of ``duration``."""
    return rng.uniform(duration * low, duration * high)


def _short_hold(rng: random.Random, duration: float, minimum: float, cap: float) -> float:
    """A random hold time, long enough to matter and short enough to fit the trial."""
    return rng.uniform(minimum, max(minimum, min(cap, duration * 0.3)))


def _crash_leader(rng: random.Random, nodes: int, duration: float) -> tuple[FaultStep, ...]:
    at = _span(rng, duration, 0.15, 0.4)
    return (
        FaultStep(at=at, action=CRASH, target="leader"),
        FaultStep(at=at + _short_hold(rng, duration, 1.0, 5.0), action=RECOVER),
    )


def _crash_follower(rng: random.Random, nodes: int, duration: float) -> tuple[FaultStep, ...]:
    target = str(rng.randint(1, nodes))
    at = _span(rng, duration, 0.15, 0.4)
    return (
        FaultStep(at=at, action=CRASH, target=target),
        FaultStep(at=at + _short_hold(rng, duration, 1.0, 5.0), action=RECOVER),
    )


def _minority_partition(rng: random.Random, nodes: int, duration: float) -> tuple[FaultStep, ...]:
    if nodes < 3:
        return _crash_follower(rng, nodes, duration)
    size = rng.randint(1, (nodes - 1) // 2)
    minority = [str(i) for i in rng.sample(range(1, nodes + 1), size)]
    majority = [str(i) for i in range(1, nodes + 1) if str(i) not in minority]
    at = _span(rng, duration, 0.15, 0.4)
    return (
        FaultStep(at=at, action=PARTITION, params={"groups": [minority, majority]}),
        FaultStep(at=at + _short_hold(rng, duration, 2.0, 10.0), action=HEAL_PARTITION),
    )


def _majority_partition(rng: random.Random, nodes: int, duration: float) -> tuple[FaultStep, ...]:
    if nodes < 2:
        return _crash_leader(rng, nodes, duration)
    at = _span(rng, duration, 0.15, 0.4)
    everyone = [str(i) for i in range(1, nodes + 1)]
    return (
        FaultStep(at=at, action=PARTITION, params={"groups": [["leader"], everyone]}),
        FaultStep(at=at + _short_hold(rng, duration, 3.0, 12.0), action=HEAL_PARTITION),
    )


def _latency_spike(rng: random.Random, nodes: int, duration: float) -> tuple[FaultStep, ...]:
    at = _span(rng, duration, 0.1, 0.5)
    low = rng.uniform(0.0, 0.05)
    high = low + rng.uniform(0.05, 0.25)
    return (
        FaultStep(at=at, action=LATENCY, params={"min_seconds": low, "max_seconds": high}),
        FaultStep(at=at + _short_hold(rng, duration, 2.0, 8.0), action=RESET_FAULTS),
    )


def _packet_loss_burst(rng: random.Random, nodes: int, duration: float) -> tuple[FaultStep, ...]:
    at = _span(rng, duration, 0.1, 0.5)
    return (
        FaultStep(at=at, action=PACKET_LOSS, params={"rate": rng.uniform(0.1, 0.5)}),
        FaultStep(at=at + _short_hold(rng, duration, 2.0, 8.0), action=RESET_FAULTS),
    )


def _churn(rng: random.Random, nodes: int, duration: float) -> tuple[FaultStep, ...]:
    """Several crash/recover cycles, never more than one node down at once."""
    targets = ["leader", *[str(i) for i in range(1, nodes + 1)]]
    steps: list[FaultStep] = []
    t = duration * 0.1
    for _ in range(rng.randint(2, 4)):
        if t >= duration * 0.85:
            break
        recover_at = t + _short_hold(rng, duration, 1.0, 3.0)
        steps.append(FaultStep(at=t, action=CRASH, target=rng.choice(targets)))
        steps.append(FaultStep(at=recover_at, action=RECOVER))
        t = recover_at + _short_hold(rng, duration, 1.0, 3.0)
    return tuple(steps)


def _crash_plus_latency(rng: random.Random, nodes: int, duration: float) -> tuple[FaultStep, ...]:
    combined = _crash_follower(rng, nodes, duration) + _latency_spike(rng, nodes, duration)
    return tuple(sorted(combined, key=lambda step: step.at))


@dataclass(frozen=True)
class FaultKind:
    """One archetype in the stress catalog, and how to build it for a given trial."""

    name: str
    build: FaultBuilder


STRESS_FAULT_KINDS: tuple[FaultKind, ...] = (
    FaultKind("crash_leader", _crash_leader),
    FaultKind("crash_follower", _crash_follower),
    FaultKind("minority_partition", _minority_partition),
    FaultKind("majority_partition", _majority_partition),
    FaultKind("latency_spike", _latency_spike),
    FaultKind("packet_loss_burst", _packet_loss_burst),
    FaultKind("churn", _churn),
    FaultKind("crash_plus_latency", _crash_plus_latency),
)


def trial_seed(base_seed: int, trial_index: int) -> int:
    """Every trial's seed is a pure function of the campaign's own seed and its index -
    the only source of randomness this module ever reads from."""
    return base_seed * 1_000_003 + trial_index


def generate_trial_scenario(
    base_seed: int,
    trial_index: int,
    *,
    nodes: int,
    duration: float,
    workload: WorkloadConfig,
) -> Scenario:
    """The scenario one stress trial runs: a catalog entry, chosen by cycling through
    ``STRESS_FAULT_KINDS`` so N trials sweep every archetype, built with a
    ``random.Random`` seeded from this trial alone."""
    seed = trial_seed(base_seed, trial_index)
    kind = STRESS_FAULT_KINDS[trial_index % len(STRESS_FAULT_KINDS)]
    faults = kind.build(random.Random(seed), nodes, duration)
    return Scenario(
        name=f"stress-{kind.name}-{trial_index}",
        duration=duration,
        nodes=nodes,
        faults=faults,
        workload=workload,
        expected={},
        seed=seed,
    )


@dataclass(frozen=True)
class StressConfig:
    nodes: int = 3
    trial_duration: float = 10.0
    trials: int = 20
    base_seed: int = 0
    workload: WorkloadConfig = field(default_factory=WorkloadConfig)
    results_dir: Path = Path("results")
    election_timeout_range: tuple[float, float] = ELECTION_TIMEOUT_RANGE
    heartbeat_interval: float = HEARTBEAT_INTERVAL
    client_timeout: float = CLIENT_TIMEOUT


@dataclass(frozen=True)
class StressTrialResult:
    trial_index: int
    run_id: str
    seed: int
    fault_kind: str
    passed: bool
    data_loss: bool
    divergent: bool
    linearizable_verdict: str
    recovery_time_ms: float | None
    duration_sec: float
    persisted_at: str | None

    def to_dict(self) -> dict[str, Any]:
        return {
            "trial_index": self.trial_index,
            "run_id": self.run_id,
            "seed": self.seed,
            "fault_kind": self.fault_kind,
            "passed": self.passed,
            "data_loss": self.data_loss,
            "divergent": self.divergent,
            "linearizable_verdict": self.linearizable_verdict,
            "recovery_time_ms": self.recovery_time_ms,
            "duration_sec": self.duration_sec,
            "persisted_at": self.persisted_at,
        }


def _percentiles(values: list[float]) -> dict[str, float | None]:
    return {
        "p50": metrics.percentile(values, 0.50),
        "p95": metrics.percentile(values, 0.95),
        "p99": metrics.percentile(values, 0.99),
        "max": max(values) if values else None,
    }


@dataclass(frozen=True)
class StressReport:
    config: StressConfig
    trials: list[StressTrialResult]

    @property
    def passed(self) -> int:
        return sum(1 for t in self.trials if t.passed)

    @property
    def failed(self) -> int:
        return len(self.trials) - self.passed

    def to_dict(self) -> dict[str, Any]:
        recoveries = [t.recovery_time_ms for t in self.trials if t.recovery_time_ms is not None]
        verdicts = [t.linearizable_verdict for t in self.trials]
        return {
            "nodes": self.config.nodes,
            "trial_duration_sec": self.config.trial_duration,
            "base_seed": self.config.base_seed,
            "trials": len(self.trials),
            "passed": self.passed,
            "failed": self.failed,
            "data_loss_trials": sum(1 for t in self.trials if t.data_loss),
            "divergent_trials": sum(1 for t in self.trials if t.divergent),
            "linearizability": {
                "PASS": verdicts.count("PASS"),
                "FAIL": verdicts.count("FAIL"),
                "UNPROVEN": verdicts.count("UNPROVEN"),
            },
            "recovery_time_ms": _percentiles(recoveries),
            "trial_results": [t.to_dict() for t in self.trials],
        }

    def summary(self) -> str:
        document = self.to_dict()
        recovery = document["recovery_time_ms"]
        lines = [
            f"stress: {document['nodes']} nodes, {document['trials']} trials "
            f"({document['trial_duration_sec']:.1f}s each, base seed {document['base_seed']})",
            f"  passed        {document['passed']}",
            f"  failed        {document['failed']}",
            f"  data loss     {document['data_loss_trials']} trial(s)",
            f"  divergence    {document['divergent_trials']} trial(s)",
            f"  linearizable  PASS {document['linearizability']['PASS']}"
            f"  FAIL {document['linearizability']['FAIL']}"
            f"  UNPROVEN {document['linearizability']['UNPROVEN']}",
            "  recovery time (ms)  "
            f"p50 {_fmt(recovery['p50'])}  p95 {_fmt(recovery['p95'])}"
            f"  p99 {_fmt(recovery['p99'])}  max {_fmt(recovery['max'])}",
        ]
        for trial in self.trials:
            if not trial.passed:
                lines.append(
                    f"  FAILED trial {trial.trial_index} ({trial.fault_kind})"
                    f"  run_id={trial.run_id}  -> pyraft-lab replay {trial.run_id}"
                )
        return "\n".join(lines)


def _fmt(value: float | None) -> str:
    return "n/a" if value is None else f"{value:.1f}"


async def _run_trial(config: StressConfig, trial_index: int) -> tuple[RunResult, StressTrialResult]:
    scenario = generate_trial_scenario(
        config.base_seed,
        trial_index,
        nodes=config.nodes,
        duration=config.trial_duration,
        workload=config.workload,
    )
    result = await ExperimentRunner(
        scenario,
        election_timeout_range=config.election_timeout_range,
        heartbeat_interval=config.heartbeat_interval,
        client_timeout=config.client_timeout,
    ).run()

    document = metrics.report(result)
    divergent = bool(document["summary"]["divergent_logs"])
    # Never trust document["passed"] alone here: it folds in `expected:` and data_loss
    # but not divergent_logs (a real gap in metrics.report, see docs/architecture.md
    # P8) - a stress trial with diverged logs must fail regardless.
    passed = bool(document["passed"]) and not divergent

    persisted_at = None if passed else str(persist_run(result, config.results_dir))
    fault_kind = STRESS_FAULT_KINDS[trial_index % len(STRESS_FAULT_KINDS)].name

    trial = StressTrialResult(
        trial_index=trial_index,
        run_id=result.run_id,
        seed=result.seed,
        fault_kind=fault_kind,
        passed=passed,
        data_loss=bool(document["summary"]["data_loss"]),
        divergent=divergent,
        linearizable_verdict=document.get("consistency", {}).get("verdict", "UNPROVEN"),
        recovery_time_ms=document["leadership"]["recovery_time_ms"],
        duration_sec=document["duration_sec"],
        persisted_at=persisted_at,
    )
    return result, trial


async def run_stress(config: StressConfig) -> StressReport:
    """Run every trial of a stress campaign and tally the results."""
    trials = []
    for index in range(config.trials):
        _, trial = await _run_trial(config, index)
        trials.append(trial)
    return StressReport(config=config, trials=trials)


def write_report(report: StressReport, path: str | Path) -> Path:
    return metrics.write_report(report.to_dict(), path)
