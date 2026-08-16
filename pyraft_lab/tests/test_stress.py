"""Phase 8, 2.0 item 7: stress campaigns.

Three things are under test:

1. trial generation is deterministic and sweeps the catalog - the same
   ``(base_seed, trial_index)`` always builds the same scenario, and different indices
   produce genuinely different fault timelines rather than N copies of one archetype.
2. an end-to-end campaign produces a report with the shape plan.md asks for
   (trials/passed/failed/data-loss/linearizability/recovery percentiles). Individual
   trials are *not* asserted to pass - a short, randomized trial can legitimately come
   back ``UNPROVEN``, and plan.md's own testing rule warns against a suite that is
   flaky rather than wrong (docs/plan.md, "Testing rule").
3. persist-on-failure is wired correctly, tested in isolation by forcing a failing
   trial rather than by trying to make Raft actually lose data.
"""

from __future__ import annotations

import random
from pathlib import Path

import pytest

from pyraft_lab.client.workload import WorkloadConfig
from pyraft_lab.experiments import stress
from pyraft_lab.experiments.stress import (
    STRESS_FAULT_KINDS,
    StressConfig,
    generate_trial_scenario,
    run_stress,
)


def test_generating_the_same_trial_twice_is_identical() -> None:
    first = generate_trial_scenario(7, 3, nodes=3, duration=10.0, workload=WorkloadConfig())
    second = generate_trial_scenario(7, 3, nodes=3, duration=10.0, workload=WorkloadConfig())

    assert first.to_dict() == second.to_dict()
    assert first.seed == second.seed


def test_trials_sweep_the_catalog_rather_than_repeating_one_archetype() -> None:
    scenarios = [
        generate_trial_scenario(7, i, nodes=5, duration=20.0, workload=WorkloadConfig())
        for i in range(len(STRESS_FAULT_KINDS))
    ]

    names = {s.name.rsplit("-", 1)[0] for s in scenarios}
    assert len(names) == len(STRESS_FAULT_KINDS)

    fault_shapes = {tuple(step.action for step in s.faults) for s in scenarios}
    assert len(fault_shapes) > 1


def test_every_fault_kind_builds_a_valid_scenario_across_cluster_sizes() -> None:
    """Guards the small-cluster edge cases (partition archetypes on nodes < 3)."""
    for nodes in (1, 2, 3, 5):
        for index in range(len(STRESS_FAULT_KINDS)):
            scenario = generate_trial_scenario(
                1, index, nodes=nodes, duration=10.0, workload=WorkloadConfig()
            )
            for step in scenario.faults:
                assert 0.0 <= step.at <= scenario.duration


def test_no_unseeded_randomness_leaks_into_a_generated_trial() -> None:
    """Every draw a fault-kind builder makes must come from the ``rng`` it is handed,
    not from the module-level ``random`` functions - otherwise two runs of the same
    trial index would diverge."""
    kind = STRESS_FAULT_KINDS[0]
    a = kind.build(random.Random(123), 5, 20.0)
    b = kind.build(random.Random(123), 5, 20.0)
    assert a == b


async def test_run_stress_produces_the_report_shape_plan_md_asks_for() -> None:
    config = StressConfig(nodes=3, trial_duration=3.0, trials=3, base_seed=1)

    report = await run_stress(config)
    document = report.to_dict()

    assert document["trials"] == 3
    assert document["passed"] + document["failed"] == 3
    assert set(document) >= {
        "trials",
        "passed",
        "failed",
        "data_loss_trials",
        "divergent_trials",
        "linearizability",
        "recovery_time_ms",
        "trial_results",
    }
    assert set(document["linearizability"]) == {"PASS", "FAIL", "UNPROVEN"}
    assert set(document["recovery_time_ms"]) == {"p50", "p95", "p99", "max"}
    assert len(document["trial_results"]) == 3
    for trial in document["trial_results"]:
        assert trial["fault_kind"] in {kind.name for kind in STRESS_FAULT_KINDS}


async def test_a_failing_trial_is_persisted_and_a_passing_one_is_not(
    tmp_path: Path, monkeypatch
) -> None:
    calls = {"n": 0}

    def fake_report(result, *, check_consistency=True):
        calls["n"] += 1
        failing = calls["n"] == 1
        return {
            "summary": {"data_loss": failing, "divergent_logs": [], "lost_writes": []},
            "leadership": {"recovery_time_ms": 12.5},
            "duration_sec": 1.0,
            "consistency": {"verdict": "PASS"},
            "passed": not failing,
        }

    monkeypatch.setattr(stress.metrics, "report", fake_report)

    config = StressConfig(nodes=3, trial_duration=1.0, trials=1, base_seed=1, results_dir=tmp_path)
    _, failing_trial = await stress._run_trial(config, 0)
    assert failing_trial.passed is False
    assert failing_trial.persisted_at is not None
    assert Path(failing_trial.persisted_at, "manifest.json").exists()

    _, passing_trial = await stress._run_trial(config, 1)
    assert passing_trial.passed is True
    assert passing_trial.persisted_at is None


async def test_divergent_logs_fail_a_trial_even_when_report_says_passed(
    tmp_path: Path, monkeypatch
) -> None:
    """Pins the found gap: metrics.report()'s own "passed" field does not consider
    divergent_logs, so stress must never trust it alone."""

    def fake_report(result, *, check_consistency=True):
        return {
            "summary": {
                "data_loss": False,
                "divergent_logs": [{"nodes": ["n1", "n2"]}],
                "lost_writes": [],
            },
            "leadership": {"recovery_time_ms": None},
            "duration_sec": 1.0,
            "consistency": {"verdict": "PASS"},
            "passed": True,
        }

    monkeypatch.setattr(stress.metrics, "report", fake_report)

    config = StressConfig(nodes=3, trial_duration=1.0, trials=1, base_seed=1, results_dir=tmp_path)
    _, trial = await stress._run_trial(config, 0)

    assert trial.passed is False
    assert trial.divergent is True


@pytest.mark.slow
async def test_a_fuller_sweep_covers_every_fault_kind_at_least_once() -> None:
    config = StressConfig(
        nodes=5, trial_duration=15.0, trials=len(STRESS_FAULT_KINDS) * 2, base_seed=3
    )

    report = await run_stress(config)

    kinds_seen = {trial.fault_kind for trial in report.trials}
    assert kinds_seen == {kind.name for kind in STRESS_FAULT_KINDS}
    assert len(report.trials) == config.trials
