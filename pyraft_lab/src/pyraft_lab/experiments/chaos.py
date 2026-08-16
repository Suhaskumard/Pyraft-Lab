"""Chaos campaigns (2.0 item 13): a randomized fault sequence per trial, checking one
safety invariant every time - **committed data is never silently lost** - and printing
a ``CHAOS TEST REPORT``.

Where ``stress.py`` picks one entry from a small, bounded catalog per trial, a chaos
trial's fault timeline is an open-ended random walk (:func:`random_fault_sequence`):
crashes, partitions, latency and packet-loss changes, at random times and targets,
with no attempt to keep a quorum alive. That is deliberate - chaos's only invariant is
about data safety, not availability, so it is free to go further than stress ever
does. Every choice still comes from one ``random.Random`` seeded from
``(base_seed, trial_index)``, so a campaign is exactly as reproducible as any other run.

Persistence is not a knob here, it is the point of the command: every trial runs with
a real per-node WAL (``ExperimentRunner(..., wal_dir=...)``), so a ``crash`` genuinely
discards a node's in-memory state and ``recover`` rebuilds it from nothing but what
was on disk - closing the gap docs/architecture.md's P6 note left open for this phase.
See ``experiments/runner.py``'s ``_recover`` and P8 in docs/architecture.md.

The invariant is checked explicitly from ``metrics.lost_writes``/a local
``_divergent_nodes_all`` (see below), never from ``metrics.report()``'s own
``passed`` field, which folds in `expected:` expectations and does not consider log
divergence at all. Linearizability is still recorded per trial, but only
informationally - plan.md's invariant is specifically about lost writes, a narrower
and always-decidable question next to linearizability's occasional ``UNPROVEN``.

One more broadening beyond what ``metrics.py`` does by default: ``RunResult.
surviving_log`` (and so ``metrics.report()``'s own ``summary.lost_writes``/
``divergent_logs``) only look at nodes still alive when a run ends - a fine
assumption for Phase 5's hand-written scenarios, which always let a crashed node
recover before the scenario finishes, but not one chaos's randomized fault sequence
ever promises: a trial can legitimately end mid-crash. A node stopping never clears
its in-memory state (only its task and file handle, see ``RaftNode.stop``), so a node
that is merely down - not gone - at the instant a trial ends still durably holds
whatever it had committed before that crash. Counting only alive nodes would report
that data as lost when a moment's recovery would show it right back, so this module's
own ``_surviving_log_all_nodes``/``_divergent_nodes_all`` use every node's snapshot,
crashed or not.
"""

from __future__ import annotations

import random
import tempfile
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

_ACTIONS = (CRASH, PARTITION, LATENCY, PACKET_LOSS, RESET_FAULTS)
_WEIGHTS = (0.35, 0.20, 0.15, 0.15, 0.15)


def trial_seed(base_seed: int, trial_index: int) -> int:
    """A different multiplier than ``stress.trial_seed`` so a shared base seed never
    accidentally produces the same trial for both commands."""
    return base_seed * 2_000_003 + trial_index


def random_fault_sequence(
    rng: random.Random, *, nodes: int, duration: float
) -> tuple[FaultStep, ...]:
    """A randomized fault timeline: no catalog, no quorum-preserving guarantee - only
    the timing and choice of each step is bounded, so a trial stays inside its
    duration and every fault is one the runner already knows how to apply."""
    all_nodes = [str(i) for i in range(1, nodes + 1)]
    steps: list[FaultStep] = []
    t = 0.0
    limit = duration * 0.9
    # An undo step (recover/heal) scheduled at exactly `duration` can never fire: the
    # runner's drive loop only processes a pending fault while clock.now() < duration,
    # so it exits at the boundary without ever applying a step sitting right on it.
    # Clamping just short of the end, rather than to `duration` itself, is what
    # guarantees every crash this generator writes gets its paired recovery a chance
    # to run before teardown inspects who is still down.
    end = duration * 0.99

    while t < limit:
        t += rng.uniform(duration * 0.05, duration * 0.2)
        if t >= limit:
            break
        action = rng.choices(_ACTIONS, weights=_WEIGHTS, k=1)[0]

        if action == CRASH:
            target = rng.choice(["leader", *all_nodes])
            steps.append(FaultStep(at=t, action=CRASH, target=target))
            if rng.random() < 0.85:
                recover_at = min(end, t + rng.uniform(0.5, max(0.5, duration * 0.25)))
                steps.append(FaultStep(at=recover_at, action=RECOVER))
        elif action == PARTITION and nodes >= 2:
            size = rng.randint(1, nodes - 1)
            group = [str(i) for i in rng.sample(range(1, nodes + 1), size)]
            rest = [n for n in all_nodes if n not in group]
            steps.append(FaultStep(at=t, action=PARTITION, params={"groups": [group, rest]}))
            if rng.random() < 0.7:
                heal_at = min(end, t + rng.uniform(1.0, max(1.0, duration * 0.3)))
                steps.append(FaultStep(at=heal_at, action=HEAL_PARTITION))
        elif action == LATENCY:
            low = rng.uniform(0.0, 0.05)
            high = low + rng.uniform(0.05, 0.3)
            steps.append(
                FaultStep(at=t, action=LATENCY, params={"min_seconds": low, "max_seconds": high})
            )
        elif action == PACKET_LOSS:
            rate = rng.uniform(0.05, 0.4)
            steps.append(FaultStep(at=t, action=PACKET_LOSS, params={"rate": rate}))
        else:
            steps.append(FaultStep(at=t, action=RESET_FAULTS))

    return tuple(sorted(steps, key=lambda step: step.at))


@dataclass(frozen=True)
class ChaosConfig:
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
class ChaosTrialResult:
    trial_index: int
    run_id: str
    seed: int
    passed: bool
    data_loss: bool
    divergent: bool
    lost_write_count: int
    linearizable_verdict: str
    recovery_time_ms: float | None
    fault_count: int
    duration_sec: float
    persisted_at: str | None

    def to_dict(self) -> dict[str, Any]:
        return {
            "trial_index": self.trial_index,
            "run_id": self.run_id,
            "seed": self.seed,
            "passed": self.passed,
            "data_loss": self.data_loss,
            "divergent": self.divergent,
            "lost_write_count": self.lost_write_count,
            "linearizable_verdict": self.linearizable_verdict,
            "recovery_time_ms": self.recovery_time_ms,
            "fault_count": self.fault_count,
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


def _fmt(value: float | None) -> str:
    return "n/a" if value is None else f"{value:.1f}"


@dataclass(frozen=True)
class ChaosReport:
    config: ChaosConfig
    trials: list[ChaosTrialResult]

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

    def explain(self) -> str:
        document = self.to_dict()
        recovery = document["recovery_time_ms"]
        lines = [
            "CHAOS TEST REPORT",
            "=================",
            f"nodes: {document['nodes']}   trial duration: {document['trial_duration_sec']:.1f}s"
            f"   trials: {document['trials']}   base seed: {document['base_seed']}",
            "",
            "invariant: committed data is never silently lost",
            f"  trials run       {document['trials']}",
            f"  passed           {document['passed']}",
            f"  failed           {document['failed']}",
            f"  data loss        {document['data_loss_trials']} trial(s)",
            f"  log divergence   {document['divergent_trials']} trial(s)",
            "",
            "recovery time (ms)   "
            f"p50 {_fmt(recovery['p50'])}  p95 {_fmt(recovery['p95'])}"
            f"  p99 {_fmt(recovery['p99'])}  max {_fmt(recovery['max'])}",
            "linearizability (informational only)   "
            f"PASS {document['linearizability']['PASS']}"
            f"  FAIL {document['linearizability']['FAIL']}"
            f"  UNPROVEN {document['linearizability']['UNPROVEN']}",
        ]
        failing = [t for t in self.trials if not t.passed]
        if failing:
            lines += ["", "failing trials (replay-able):"]
            for trial in failing:
                parts = (
                    "DATA LOSS" if trial.data_loss else "",
                    "DIVERGENT" if trial.divergent else "",
                )
                tag = " ".join(part for part in parts if part)
                lines.append(
                    f"  trial {trial.trial_index:>3}  run_id={trial.run_id}"
                    f"  seed={trial.seed}  {tag}"
                )
                lines.append(
                    f"    pyraft-lab replay {trial.run_id} --results-dir {self.config.results_dir}"
                )
        return "\n".join(lines)


def _surviving_log_all_nodes(result: RunResult) -> list[Any]:
    """Like ``RunResult.surviving_log``, but over every node - crashed or not.

    A node stopping never clears its in-memory state (only its task and file handle -
    ``RaftNode.stop``), so a node that is merely down, not gone, at the instant a
    chaos trial ends still durably holds whatever it had committed before that crash.
    Restricting to alive nodes (what ``surviving_log`` does, correctly, for Phase 5's
    scenarios) would report that data as lost when a moment's recovery would show it
    right back - and chaos, unlike a hand-written scenario, never promises a trial
    ends with every crash already recovered.
    """
    return max((n.committed for n in result.nodes), key=len, default=[])


def _divergent_nodes_all(result: RunResult) -> list[dict[str, Any]]:
    """``metrics.divergent_nodes``, but over every node - the same broadening as
    :func:`_surviving_log_all_nodes`, for the same reason: a crashed node's log at the
    moment it went down is still evidence, and a real Log Matching violation should be
    caught whether or not that node happens to still be up when the trial ends.
    """
    problems: list[dict[str, Any]] = []
    snapshots = result.nodes
    for i, first in enumerate(snapshots):
        for second in snapshots[i + 1 :]:
            shared = min(len(first.committed), len(second.committed))
            for index in range(shared):
                if first.committed[index] != second.committed[index]:
                    problems.append(
                        {
                            "nodes": [first.node_id, second.node_id],
                            "index": index + 1,
                            "left": first.committed[index],
                            "right": second.committed[index],
                        }
                    )
                    break
    return problems


async def _run_trial(config: ChaosConfig, trial_index: int) -> tuple[RunResult, ChaosTrialResult]:
    seed = trial_seed(config.base_seed, trial_index)
    rng = random.Random(seed)
    faults = random_fault_sequence(rng, nodes=config.nodes, duration=config.trial_duration)
    scenario = Scenario(
        name=f"chaos-{trial_index}",
        duration=config.trial_duration,
        nodes=config.nodes,
        faults=faults,
        workload=config.workload,
        expected={},
        seed=seed,
    )

    with tempfile.TemporaryDirectory(prefix=f"chaos-{trial_index}-wal-") as wal_dir:
        result = await ExperimentRunner(
            scenario,
            wal_dir=wal_dir,
            election_timeout_range=config.election_timeout_range,
            heartbeat_interval=config.heartbeat_interval,
            client_timeout=config.client_timeout,
        ).run()

        document = metrics.report(result)
        lost = metrics.lost_writes(result.history, _surviving_log_all_nodes(result))
        divergent_list = _divergent_nodes_all(result)
        data_loss = bool(lost)
        divergent = bool(divergent_list)
        # The invariant, computed explicitly - never document["passed"] (which ignores
        # divergent_logs entirely) and never document["summary"]'s own data_loss/
        # divergent_logs (which only look at nodes still alive when the trial ends -
        # see the module docstring and docs/architecture.md P8 for why that undercounts
        # here specifically).
        passed = not data_loss and not divergent
        persisted_at = None if passed else str(persist_run(result, config.results_dir))

    trial = ChaosTrialResult(
        trial_index=trial_index,
        run_id=result.run_id,
        seed=result.seed,
        passed=passed,
        data_loss=data_loss,
        divergent=divergent,
        lost_write_count=len(lost),
        linearizable_verdict=document.get("consistency", {}).get("verdict", "UNPROVEN"),
        recovery_time_ms=document["leadership"]["recovery_time_ms"],
        fault_count=len(scenario.faults),
        duration_sec=document["duration_sec"],
        persisted_at=persisted_at,
    )
    return result, trial


async def run_chaos(config: ChaosConfig) -> ChaosReport:
    """Run every trial of a chaos campaign and tally the results."""
    trials = []
    for index in range(config.trials):
        _, trial = await _run_trial(config, index)
        trials.append(trial)
    return ChaosReport(config=config, trials=trials)


def write_report(report: ChaosReport, path: str | Path) -> Path:
    return metrics.write_report(report.to_dict(), path)
