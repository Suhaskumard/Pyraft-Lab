"""Phase 9: the CLI commands ``init``, ``run``, ``results`` and ``cluster start``,
driven the way a user actually would - through Typer's ``CliRunner`` - rather than by
calling the underlying Python API directly (that's what ``tests/test_cluster.py`` and
``tests/test_cli_repl.py`` are for).
"""

from __future__ import annotations

import json
from pathlib import Path

from typer.testing import CliRunner

from pyraft_lab.cli.main import app

runner = CliRunner()

A_SHORT_SCENARIO = """\
name: cli-smoke
duration: 1s
nodes: 3
seed: 1
faults: {}
workload:
  type: put_get_mix
  puts_per_sec: 20
  get_ratio: 0.5
  keys: 3
  clients: 2
"""

AN_UNSATISFIABLE_SCENARIO = """\
name: cli-smoke-unsatisfiable
duration: 1s
nodes: 3
seed: 1
faults: {}
workload:
  type: put_get_mix
  puts_per_sec: 20
  get_ratio: 0.5
  keys: 3
  clients: 2
expected:
  total_requests: ">999999"
"""


# --- init --------------------------------------------------------------------------------


def test_init_writes_a_default_config_and_directories(tmp_path: Path) -> None:
    result = runner.invoke(
        app,
        [
            "init",
            "--config", str(tmp_path / "cluster.yaml"),
            "--scenarios-dir", str(tmp_path / "scenarios"),
            "--results-dir", str(tmp_path / "results"),
        ],
    )

    assert result.exit_code == 0
    assert (tmp_path / "cluster.yaml").exists()
    assert (tmp_path / "scenarios").is_dir()
    assert (tmp_path / "results").is_dir()


def test_init_does_not_overwrite_an_existing_config(tmp_path: Path) -> None:
    config = tmp_path / "cluster.yaml"
    args = [
        "init",
        "--config", str(config),
        "--scenarios-dir", str(tmp_path / "scenarios"),
        "--results-dir", str(tmp_path / "results"),
    ]

    assert runner.invoke(app, args).exit_code == 0
    config.write_text("nodes: 7\n", encoding="utf-8")

    result = runner.invoke(app, args)

    assert result.exit_code == 0
    assert "already exists" in result.stdout
    assert config.read_text(encoding="utf-8") == "nodes: 7\n"


# --- run ---------------------------------------------------------------------------------


def test_run_executes_and_persists_a_scenario(tmp_path: Path) -> None:
    scenario = tmp_path / "smoke.yaml"
    scenario.write_text(A_SHORT_SCENARIO, encoding="utf-8")
    results_dir = tmp_path / "results"

    result = runner.invoke(
        app, ["run", "--scenario", str(scenario), "--results-dir", str(results_dir)]
    )

    assert result.exit_code == 0, result.stdout
    run_dirs = list(results_dir.iterdir())
    assert len(run_dirs) == 1
    assert (run_dirs[0] / "manifest.json").exists()


def test_run_exits_2_for_a_missing_scenario_file(tmp_path: Path) -> None:
    result = runner.invoke(
        app,
        ["run", "--scenario", str(tmp_path / "nope.yaml"), "--results-dir", str(tmp_path / "r")],
    )

    assert result.exit_code == 2


def test_run_exits_1_when_expectations_fail(tmp_path: Path) -> None:
    scenario = tmp_path / "unsatisfiable.yaml"
    scenario.write_text(AN_UNSATISFIABLE_SCENARIO, encoding="utf-8")

    result = runner.invoke(
        app,
        ["run", "--scenario", str(scenario), "--results-dir", str(tmp_path / "results")],
    )

    assert result.exit_code == 1


# --- results -----------------------------------------------------------------------------


def test_results_lists_persisted_runs(tmp_path: Path) -> None:
    scenario = tmp_path / "smoke.yaml"
    scenario.write_text(A_SHORT_SCENARIO, encoding="utf-8")
    results_dir = tmp_path / "results"

    for _ in range(2):
        assert runner.invoke(
            app, ["run", "--scenario", str(scenario), "--results-dir", str(results_dir)]
        ).exit_code == 0

    result = runner.invoke(app, ["results", "--results-dir", str(results_dir)])

    assert result.exit_code == 0
    run_ids = [d.name for d in results_dir.iterdir()]
    assert len(run_ids) == 2
    for run_id in run_ids:
        assert run_id in result.stdout
    assert "PASS" in result.stdout


def test_results_exits_2_for_a_missing_directory(tmp_path: Path) -> None:
    result = runner.invoke(app, ["results", "--results-dir", str(tmp_path / "nope")])
    assert result.exit_code == 2


def test_results_says_so_when_there_are_none(tmp_path: Path) -> None:
    empty = tmp_path / "results"
    empty.mkdir()

    result = runner.invoke(app, ["results", "--results-dir", str(empty)])

    assert result.exit_code == 0
    assert "no runs" in result.stdout


def test_results_reports_an_unreadable_manifest_without_crashing(tmp_path: Path) -> None:
    results_dir = tmp_path / "results"
    run_dir = results_dir / "broken"
    run_dir.mkdir(parents=True)
    (run_dir / "manifest.json").write_text("{not json", encoding="utf-8")

    result = runner.invoke(app, ["results", "--results-dir", str(results_dir)])

    assert result.exit_code == 0
    assert "unreadable manifest" in result.stdout


# --- cluster start (end to end, piped stdin) --------------------------------------------


def test_cluster_start_serves_a_full_session_over_piped_stdin(tmp_path: Path) -> None:
    result = runner.invoke(
        app,
        ["cluster", "start", "--nodes", "3", "--config", str(tmp_path / "no-such-config.yaml")],
        input="put color blue\nget color\nstatus\ntopology\nexit\n",
    )

    assert result.exit_code == 0, result.stdout
    assert "ok: color = blue" in result.stdout
    assert "color = blue" in result.stdout
    assert "status: HEALTHY" in result.stdout
    assert "leader" in result.stdout


def test_cluster_start_serves_the_dashboard_then_continues(tmp_path: Path) -> None:
    result = runner.invoke(
        app,
        ["cluster", "start", "--nodes", "3", "--config", str(tmp_path / "no-such-config.yaml")],
        input="dashboard --refreshes 2 --interval 0.01\nstatus\nexit\n",
    )

    assert result.exit_code == 0, result.stdout
    assert result.stdout.count("PyRaft Lab Dashboard") == 2
    assert "LEADER" in result.stdout
    assert "status: HEALTHY" in result.stdout


def test_cluster_start_ends_cleanly_on_eof_with_no_explicit_exit(tmp_path: Path) -> None:
    result = runner.invoke(
        app,
        ["cluster", "start", "--nodes", "3", "--config", str(tmp_path / "no-such-config.yaml")],
        input="status\n",
    )

    assert result.exit_code == 0, result.stdout
    assert "status: HEALTHY" in result.stdout


def test_cluster_start_rejects_a_bad_nodes_override(tmp_path: Path) -> None:
    result = runner.invoke(
        app,
        ["cluster", "start", "--nodes", "0", "--config", str(tmp_path / "no-such-config.yaml")],
    )

    assert result.exit_code == 2


def test_cluster_start_reads_a_config_file(tmp_path: Path) -> None:
    from tests.test_node_election import FAST_ELECTION, FAST_HEARTBEAT

    from pyraft_lab.cluster.config import ClusterConfig

    config = tmp_path / "cluster.yaml"
    ClusterConfig(
        nodes=5, election_timeout_range=FAST_ELECTION, heartbeat_interval=FAST_HEARTBEAT
    ).save(config)

    result = runner.invoke(
        app, ["cluster", "start", "--config", str(config)], input="topology\nexit\n"
    )

    assert result.exit_code == 0, result.stdout
    # five nodes, one row each, in the topology table's body
    for i in range(1, 6):
        assert f"n{i}" in result.stdout


def test_run_plot_writes_the_four_plots(tmp_path: Path) -> None:
    scenario = tmp_path / "smoke.yaml"
    scenario.write_text(A_SHORT_SCENARIO, encoding="utf-8")
    plot_dir = tmp_path / "plots"

    result = runner.invoke(
        app,
        [
            "run", "--scenario", str(scenario),
            "--results-dir", str(tmp_path / "results"),
            "--plot", str(plot_dir),
        ],
    )

    assert result.exit_code == 0, result.stdout
    pngs = list(plot_dir.glob("*.png"))
    assert len(pngs) == 4
    assert all(p.stat().st_size > 0 for p in pngs)


def test_cli_run_output_and_python_report_agree_on_pass_fail(tmp_path: Path) -> None:
    """Not a duplicate of test_run_executes_and_persists_a_scenario: this pins the
    CLI's exit code to the *same* document metrics.report() would compute, by reading
    report.json back and checking it against what "passed" the exit code implied."""
    scenario = tmp_path / "smoke.yaml"
    scenario.write_text(A_SHORT_SCENARIO, encoding="utf-8")
    results_dir = tmp_path / "results"

    result = runner.invoke(
        app, ["run", "--scenario", str(scenario), "--results-dir", str(results_dir)]
    )
    assert result.exit_code == 0

    run_dir = next(results_dir.iterdir())
    document = json.loads((run_dir / "report.json").read_text(encoding="utf-8"))
    assert document["passed"] is True


# --- benchmark -----------------------------------------------------------------------


def test_benchmark_writes_a_report_and_graphs_for_every_cell(tmp_path: Path) -> None:
    output_dir = tmp_path / "graphs"

    result = runner.invoke(
        app,
        [
            "benchmark",
            "--nodes", "3,5",
            "--loss", "0,10",
            "--duration", "2s",
            "--results-dir", str(tmp_path / "results"),
            "--output-dir", str(output_dir),
            "--report", str(tmp_path / "benchmark-report.json"),
        ],
    )

    assert result.exit_code == 0, result.stdout
    document = json.loads((tmp_path / "benchmark-report.json").read_text(encoding="utf-8"))
    assert document["cells"] == 4

    pngs = list(output_dir.glob("*.png"))
    assert len(pngs) == 4
    assert all(p.stat().st_size > 0 for p in pngs)


def test_benchmark_rejects_an_empty_nodes_or_loss_list(tmp_path: Path) -> None:
    result = runner.invoke(
        app,
        [
            "benchmark", "--nodes", "", "--loss", "0",
            "--report", str(tmp_path / "r.json"), "--output-dir", str(tmp_path / "g"),
        ],
    )
    assert result.exit_code == 2
