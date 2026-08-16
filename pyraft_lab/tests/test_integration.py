"""Phase 12: the full integration suite - proves the system end to end, across phase
boundaries, rather than module by module the way every other test file in this
directory does.

**Every shipped scenario passes its own acceptance criteria.** ``test_scenario.py``
only checks that ``scenarios/*.yaml`` parse and carry the required names;
``test_stress.py``/``test_chaos.py`` exercise the same runner through their own
hand-built fault catalogs, never these files. Nothing before this test actually ran
every shipped scenario through the real stack (``run_scenario`` -> ``metrics.report``)
and checked its own ``expected:`` block - docs/architecture.md's P8 entry names this
exact gap for ``baseline.yaml`` alone ("nothing before Phase 8 had ever actually *run*
that scenario end-to-end and checked its ``expected:`` block against a real
execution"). This closes it for all eight, permanently, as a regression suite rather
than the one-time manual check P8 did.

**The CLI commands chain into the pipeline plan.md describes, not just work one at a
time.** Every other CLI test drives one command (or ``cluster start``'s one session)
in isolation. The pipeline test below runs ``init`` -> ``run`` -> ``results`` ->
``replay`` -> ``run --plot`` as one sequence against the real Typer entry point, and a
separate test proves a WAL-backed cluster's committed state survives across two
genuinely independent ``cluster start`` invocations (nothing but the ``--data-dir`` on
disk is shared between them) - the crash-recovery claim Phase 6/9 make, exercised at
the boundary a user actually crosses rather than only at the ``NodeManager``/
``ClusterManager`` level ``test_cluster.py`` already covers.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from typer.testing import CliRunner

from pyraft_lab.cli.main import app
from pyraft_lab.experiments.metrics import report
from pyraft_lab.experiments.runner import run_scenario
from pyraft_lab.experiments.scenario import Scenario, load_all

REPO_ROOT = Path(__file__).resolve().parents[1]
SCENARIOS_DIR = REPO_ROOT / "scenarios"

cli = CliRunner()


# --- every shipped scenario, run for real, against its own expectations ------------------


@pytest.mark.parametrize(
    "scenario", load_all(SCENARIOS_DIR), ids=lambda s: s.name if isinstance(s, Scenario) else s
)
async def test_every_shipped_scenario_passes_its_own_expectations(scenario: Scenario) -> None:
    result = await run_scenario(scenario)
    document = report(result)

    assert document["passed"], (
        f"{scenario.name} failed its own expected: block: {document['expectations']}"
    )


def test_at_least_seven_scenarios_are_shipped() -> None:
    # plan.md's Phase 5 acceptance: seven required scenarios, minimum - the parametrized
    # test above is only as complete as this directory actually is.
    assert len(load_all(SCENARIOS_DIR)) >= 7


# --- the CLI pipeline: init -> run -> results -> replay -> run --plot --------------------


def test_cli_pipeline_init_run_results_replay_plot(tmp_path: Path) -> None:
    config = tmp_path / "cluster.yaml"
    scenarios_dir = tmp_path / "scenarios"
    results_dir = tmp_path / "results"
    plot_dir = tmp_path / "plots"

    init_result = cli.invoke(
        app,
        [
            "init",
            "--config",
            str(config),
            "--scenarios-dir",
            str(scenarios_dir),
            "--results-dir",
            str(results_dir),
        ],
    )
    assert init_result.exit_code == 0, init_result.stdout
    assert config.exists()

    scenario = SCENARIOS_DIR / "leader_crash.yaml"
    run_result = cli.invoke(
        app,
        [
            "run",
            "--scenario",
            str(scenario),
            "--results-dir",
            str(results_dir),
            "--plot",
            str(plot_dir),
        ],
    )
    assert run_result.exit_code == 0, run_result.stdout
    pngs = list(plot_dir.glob("*.png"))
    assert len(pngs) == 4

    run_dirs = [d for d in results_dir.iterdir() if d.is_dir()]
    assert len(run_dirs) == 1
    run_id = run_dirs[0].name

    results_result = cli.invoke(app, ["results", "--results-dir", str(results_dir)])
    assert results_result.exit_code == 0, results_result.stdout
    assert run_id in results_result.stdout
    assert "PASS" in results_result.stdout

    replay_result = cli.invoke(app, ["replay", run_id, "--results-dir", str(results_dir)])
    assert replay_result.exit_code == 0, replay_result.stdout
    assert "MATCH" in replay_result.stdout

    report_doc = json.loads((run_dirs[0] / "report.json").read_text(encoding="utf-8"))
    assert report_doc["passed"] is True


# --- durability across two genuinely separate cluster processes --------------------------


def test_wal_backed_cluster_survives_across_two_cli_invocations(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    config = tmp_path / "no-such-config.yaml"
    args = [
        "cluster",
        "start",
        "--nodes",
        "3",
        "--data-dir",
        str(data_dir),
        "--config",
        str(config),
    ]

    first = cli.invoke(app, args, input="put color blue\nput shape square\nexit\n")
    assert first.exit_code == 0, first.stdout
    assert "ok: color = blue" in first.stdout
    assert "ok: shape = square" in first.stdout

    # A brand-new ClusterManager, from scratch - the only thing carried across this
    # boundary is what actually reached the WAL files on disk.
    second = cli.invoke(app, args, input="get color\nget shape\nexit\n")
    assert second.exit_code == 0, second.stdout
    assert "color = blue" in second.stdout
    assert "shape = square" in second.stdout
