"""The ``pyraft-lab`` command line.

Commands appear only once the feature behind them is real; the rest are declared in the
plan and land with their phases. Nothing here prints a fabricated result in the meantime.

Phase 6 adds the two commands that only need a file to answer honestly: reading back a
WAL and reading back a snapshot. 2.0 item 10's ``--node`` forms of these (``snapshot
--node node-3``, ``inspect-snapshot --node node-3``) need a running cluster to address,
so they arrive with the cluster controller in Phase 9 rather than being faked now.

Phase 7 adds ``replay``, addressed by ``run_id`` against a directory of persisted runs.

Phase 9 adds the cluster controller and closes the ``pyraft-lab run`` gap the two notes
above deferred: ``init`` scaffolds a starter project, ``cluster start`` builds a real,
live cluster and hands control to an interactive session (``put``/``get``/``status``/
``show-log``/``node inspect``/``node logs``/``trace``/``restart``/``topology`` are REPL
commands *inside* that session, not separate CLI invocations - there is deliberately no
daemon behind ``cluster start`` for a later, separate ``pyraft-lab put`` to address; see
docs/architecture.md P9), and ``run``/``results`` run a single scenario file and list
what has been persisted, exactly the missing piece ``persist_run`` was always usable for
from the Python API.
"""

from __future__ import annotations

import asyncio
import os
from pathlib import Path
from typing import Annotated

import typer

from pyraft_lab import __version__

app = typer.Typer(
    name="pyraft-lab",
    help="A Raft KV store and its fault-injection laboratory.",
    no_args_is_help=True,
    add_completion=False,
)

cluster_app = typer.Typer(
    name="cluster",
    help="Build and run a live, interactive cluster.",
    no_args_is_help=True,
    add_completion=False,
)
app.add_typer(cluster_app, name="cluster")


@app.command()
def version() -> None:
    """Print the installed version."""
    typer.echo(f"pyraft-lab {__version__}")


@app.command()
def kinds() -> None:
    """List the message kinds this build can put on the wire."""
    from pyraft_lab.network import registered_kinds

    for kind in sorted(registered_kinds()):
        typer.echo(kind)


@app.command("inspect-wal")
def inspect_wal(
    path: Annotated[Path, typer.Argument(help="The WAL file to read.")],
    repair: Annotated[
        bool,
        typer.Option("--repair", help="Accept losing a damaged record and everything after it."),
    ] = False,
    records: Annotated[bool, typer.Option("--records", help="Also list every record.")] = False,
) -> None:
    """Validate a WAL and print what a restart would recover from it.

    Read-only unless ``--repair`` is given. A damaged final record is a torn write and
    recovers cleanly; damage with valid records after it is refused, because folding
    past it would produce state the node never had.
    """
    from pyraft_lab.raft.persistence import WalError, read_wal

    if not path.exists():
        typer.echo(f"no such WAL: {path}")
        raise typer.Exit(2)

    try:
        recovery = read_wal(path, repair=repair)
    except WalError as exc:
        typer.echo(f"WAL corrupt: {exc}")
        raise typer.Exit(1) from exc

    for line in recovery.report_lines():
        typer.echo(line)

    # read_wal never writes, so --repair has to persist the truncation itself - the same
    # one WriteAheadLog.recover does - or the next read finds the damage still there.
    if repair and recovery.discarded:
        os.truncate(path, recovery.valid_bytes)
        typer.echo(f"repaired {path}: truncated to {recovery.valid_bytes} bytes")

    if records:
        typer.echo("")
        typer.echo("Records:")
        for position, record in enumerate(_wal_records(path), start=1):
            kind = record.pop("kind")
            detail = " ".join(f"{key}={value}" for key, value in sorted(record.items()))
            typer.echo(f"  {position:>6} {kind:<9} {detail}")


def _wal_records(path: Path) -> list[dict[str, object]]:
    """Every record in ``path``, stopping at the first one that does not verify.

    Stopping rather than raising: by the time this runs the report above has already
    said what recovery made of the file, and a listing that shows the good prefix is
    more use than one that shows nothing.
    """
    from pyraft_lab.raft.persistence import WalCorruption, decode_record

    found: list[dict[str, object]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        try:
            found.append(decode_record(line))
        except WalCorruption:
            break
    return found


@app.command("inspect-snapshot")
def inspect_snapshot(
    path: Annotated[Path, typer.Argument(help="A snapshot file, or a directory of them.")],
    keys: Annotated[int, typer.Option("--keys", help="Show this many keys from the payload.")] = 0,
) -> None:
    """Print a snapshot's metadata, verifying its checksum.

    Takes a path rather than 2.0's ``--node``: addressing a node by name needs the
    cluster controller from Phase 9. A directory prints every snapshot it holds.
    """
    from pyraft_lab.raft.snapshot import Snapshot, SnapshotError, SnapshotStore

    if not path.exists():
        typer.echo(f"no such snapshot: {path}")
        raise typer.Exit(2)

    try:
        if path.is_dir():
            metas = SnapshotStore(path).metas()
            if not metas:
                typer.echo(f"no readable snapshots in {path}")
                raise typer.Exit(1)
            for meta in metas:
                typer.echo(f"{meta.filename}  {meta}")
            return

        snapshot = Snapshot.from_json(path.read_text(encoding="utf-8"))
    except SnapshotError as exc:
        typer.echo(f"snapshot unusable: {exc}")
        raise typer.Exit(1) from exc

    meta = snapshot.meta
    typer.echo(f"Snapshot: {path}")
    typer.echo(f"  Node: {meta.node_id or '?'}")
    typer.echo(f"  Last included index: {meta.last_included_index:,}")
    typer.echo(f"  Last included term: {meta.last_included_term:,}")
    typer.echo(f"  Keys: {meta.keys:,}")
    typer.echo(f"  Created at: {meta.created_at:.3f}")
    typer.echo(f"  Checksum: {meta.checksum} (verified)")

    for key in sorted(snapshot.data)[:keys]:
        typer.echo(f"    {key} = {snapshot.data[key]!r}")


@app.command("replay")
def replay(
    run_id: Annotated[str, typer.Argument(help="The run_id to replay, from its manifest.")],
    results_dir: Annotated[
        Path, typer.Option("--results-dir", help="Where persisted runs live.")
    ] = Path("results"),
) -> None:
    """Reconstruct a persisted run from its manifest and check it reproduces exactly.

    2.0 item 5: every run persisted by ``persist_run`` can be handed back to
    ``ExperimentRunner`` and produce the identical event trace, because every source of
    randomness derives from one seed and time only moves because the runner advances a
    virtual clock. A mismatch means something about the run stopped being deterministic
    - a real finding, not a tooling failure - so it is reported rather than raised.
    """
    from pyraft_lab.experiments.manifest import ManifestError
    from pyraft_lab.experiments.replay import ReplayError, replay_run
    from pyraft_lab.experiments.scenario import ScenarioError

    try:
        comparison, _ = asyncio.run(replay_run(results_dir, run_id))
    except (ReplayError, ManifestError, ScenarioError) as exc:
        typer.echo(f"cannot replay {run_id!r}: {exc}")
        raise typer.Exit(2) from exc

    typer.echo(comparison.explain())
    if not comparison.matched:
        raise typer.Exit(1)


@app.command()
def stress(
    nodes: Annotated[int, typer.Option("--nodes", help="Cluster size for every trial.")] = 3,
    duration: Annotated[
        str, typer.Option("--duration", help="Trial duration, e.g. '10s'.")
    ] = "10s",
    trials: Annotated[int, typer.Option("--trials", help="Number of trials to run.")] = 20,
    seed: Annotated[int, typer.Option("--seed", help="Base seed every trial derives from.")] = 0,
    results_dir: Annotated[
        Path, typer.Option("--results-dir", help="Where a failing trial is persisted.")
    ] = Path("results"),
    report: Annotated[
        Path, typer.Option("--report", help="Where to write the JSON report.")
    ] = Path("stress-report.json"),
) -> None:
    """Auto-generated fault-combination trials over Phase 5's runner.

    Each trial is one entry from a bounded catalog of fault archetypes (crash the
    leader, crash a follower, isolate a minority or the leader alone, a latency spike,
    a packet-loss burst, churn, or a combination), cycled by trial index so N trials
    sweep every archetype. Trials run in memory. A failing trial is persisted via
    Phase 7's machinery, so its run_id is directly replayable.
    """
    from pyraft_lab.experiments.scenario import ScenarioError, parse_duration
    from pyraft_lab.experiments.stress import StressConfig, run_stress, write_report

    if nodes < 1 or trials < 1:
        typer.echo("--nodes and --trials must each be at least 1")
        raise typer.Exit(2)
    try:
        trial_duration = parse_duration(duration, what="--duration")
    except ScenarioError as exc:
        typer.echo(str(exc))
        raise typer.Exit(2) from exc

    config = StressConfig(
        nodes=nodes,
        trial_duration=trial_duration,
        trials=trials,
        base_seed=seed,
        results_dir=results_dir,
    )
    campaign = asyncio.run(run_stress(config))
    typer.echo(campaign.summary())
    path = write_report(campaign, report)
    typer.echo(f"\nwrote {path}")
    if campaign.failed:
        raise typer.Exit(1)


@app.command()
def chaos(
    nodes: Annotated[int, typer.Option("--nodes", help="Cluster size for every trial.")] = 3,
    duration: Annotated[
        str, typer.Option("--duration", help="Trial duration, e.g. '10s'.")
    ] = "10s",
    trials: Annotated[int, typer.Option("--trials", help="Number of trials to run.")] = 20,
    seed: Annotated[int, typer.Option("--seed", help="Base seed every trial derives from.")] = 0,
    results_dir: Annotated[
        Path, typer.Option("--results-dir", help="Where a failing trial is persisted.")
    ] = Path("results"),
    report: Annotated[
        Path, typer.Option("--report", help="Where to write the JSON report.")
    ] = Path("chaos-report.json"),
) -> None:
    """A randomized fault sequence per trial, checking one invariant every time:
    committed data is never silently lost.

    Unlike ``stress``, a chaos trial's timeline is an open-ended random walk with no
    attempt to keep a quorum alive, and every trial runs with a real per-node WAL, so
    a crash genuinely discards state and recovery rebuilds only from disk. A failing
    trial is persisted and directly replayable.
    """
    from pyraft_lab.experiments.chaos import ChaosConfig, run_chaos, write_report
    from pyraft_lab.experiments.scenario import ScenarioError, parse_duration

    if nodes < 1 or trials < 1:
        typer.echo("--nodes and --trials must each be at least 1")
        raise typer.Exit(2)
    try:
        trial_duration = parse_duration(duration, what="--duration")
    except ScenarioError as exc:
        typer.echo(str(exc))
        raise typer.Exit(2) from exc

    config = ChaosConfig(
        nodes=nodes,
        trial_duration=trial_duration,
        trials=trials,
        base_seed=seed,
        results_dir=results_dir,
    )
    campaign = asyncio.run(run_chaos(config))
    typer.echo(campaign.explain())
    path = write_report(campaign, report)
    typer.echo(f"\nwrote {path}")
    if campaign.failed:
        raise typer.Exit(1)


@app.command()
def benchmark(
    nodes: Annotated[
        str, typer.Option("--nodes", help="Comma-separated cluster sizes to compare.")
    ] = "3,5,7",
    loss: Annotated[
        str, typer.Option("--loss", help="Comma-separated packet-loss percentages to compare.")
    ] = "0,5,10,20",
    duration: Annotated[
        str, typer.Option("--duration", help="Per-cell trial duration, e.g. '15s'.")
    ] = "15s",
    seed: Annotated[int, typer.Option("--seed", help="Base seed every cell derives from.")] = 0,
    results_dir: Annotated[
        Path, typer.Option("--results-dir", help="Where a data-loss cell is persisted.")
    ] = Path("results"),
    output_dir: Annotated[
        Path, typer.Option("--output-dir", help="Where the comparison graphs are written.")
    ] = Path("results/benchmark"),
    report: Annotated[
        Path, typer.Option("--report", help="Where to write the JSON report.")
    ] = Path("benchmark-report.json"),
) -> None:
    """Compare throughput, latency percentiles, election/recovery time and CPU/memory
    across cluster sizes and packet-loss rates (2.0 item 11), plotting the results.

    Each cell is one ordinary scenario run over Phase 5's runner, read back through the
    same ``metrics.report()`` every other command uses - see docs/architecture.md P10
    for how CPU/memory are sampled and why every cell includes a leader crash.
    """
    from pyraft_lab.experiments.benchmark import BenchmarkConfig, run_benchmark, write_report
    from pyraft_lab.experiments.scenario import ScenarioError, parse_duration
    from pyraft_lab.experiments.visualization import plot_benchmark

    try:
        node_counts = tuple(sorted({int(n) for n in nodes.split(",") if n.strip()}))
        loss_rates = tuple(sorted({float(x) / 100.0 for x in loss.split(",") if x.strip()}))
    except ValueError as exc:
        typer.echo(f"cannot parse --nodes/--loss: {exc}")
        raise typer.Exit(2) from exc
    try:
        trial_duration = parse_duration(duration, what="--duration")
    except ScenarioError as exc:
        typer.echo(str(exc))
        raise typer.Exit(2) from exc

    if not node_counts or not loss_rates:
        typer.echo("--nodes and --loss must each name at least one value")
        raise typer.Exit(2)

    config = BenchmarkConfig(
        node_counts=node_counts,
        loss_rates=loss_rates,
        trial_duration=trial_duration,
        base_seed=seed,
        results_dir=results_dir,
    )
    campaign = asyncio.run(run_benchmark(config))
    typer.echo(campaign.summary())

    path = write_report(campaign, report)
    typer.echo(f"\nwrote {path}")

    for graph in plot_benchmark(campaign, output_dir):
        typer.echo(f"wrote {graph}")


@app.command()
def init(
    config: Annotated[
        Path, typer.Option("--config", help="Where to write the default cluster config.")
    ] = Path("cluster.yaml"),
    scenarios_dir: Annotated[
        Path, typer.Option("--scenarios-dir", help="Directory for scenario YAML files.")
    ] = Path("scenarios"),
    results_dir: Annotated[
        Path, typer.Option("--results-dir", help="Directory for persisted runs.")
    ] = Path("results"),
) -> None:
    """Scaffold a starter project: a default cluster config, and the two directories
    every other command reads from or writes to.

    Idempotent - a ``cluster.yaml`` that already exists is left alone, never
    overwritten, so hand-editing it is safe across repeated ``init`` calls.
    """
    from pyraft_lab.cluster.config import ClusterConfig

    scenarios_dir.mkdir(parents=True, exist_ok=True)
    results_dir.mkdir(parents=True, exist_ok=True)
    typer.echo(f"ensured {scenarios_dir}/ and {results_dir}/ exist")

    if config.exists():
        typer.echo(f"{config} already exists; leaving it alone")
        return
    ClusterConfig().save(config)
    typer.echo(f"wrote {config}")


@cluster_app.command("start")
def cluster_start(
    nodes: Annotated[
        int | None, typer.Option("--nodes", help="Cluster size (overrides --config).")
    ] = None,
    data_dir: Annotated[
        Path | None,
        typer.Option("--data-dir", help="Persist each node's WAL here (overrides --config)."),
    ] = None,
    seed: Annotated[
        int | None, typer.Option("--seed", help="Node RNG seed (overrides --config).")
    ] = None,
    config: Annotated[
        Path, typer.Option("--config", help="A cluster.yaml to start from, if one exists.")
    ] = Path("cluster.yaml"),
) -> None:
    """Build a real, live cluster and hand control to an interactive session.

    ``put``/``get``/``status``/``show-log``/``node inspect``/``node logs``/``trace``/
    ``restart``/``topology`` are commands typed *inside* that session (type ``help``
    once it starts), not separate ``pyraft-lab`` invocations - there is nothing
    outside this one process for a later, separate command to address. See
    docs/architecture.md P9.
    """
    from dataclasses import replace

    from pyraft_lab.cli.repl import run_repl
    from pyraft_lab.cluster.config import ClusterConfig, ClusterConfigError
    from pyraft_lab.cluster.lifecycle import LifecycleTimeout
    from pyraft_lab.cluster.manager import ClusterManager

    if config.exists():
        try:
            base = ClusterConfig.load(config)
        except ClusterConfigError as exc:
            typer.echo(f"cannot load {config}: {exc}")
            raise typer.Exit(2) from exc
    else:
        base = ClusterConfig()

    overrides = {
        key: value
        for key, value in (("nodes", nodes), ("data_dir", data_dir), ("seed", seed))
        if value is not None
    }
    try:
        cfg = replace(base, **overrides) if overrides else base
    except ClusterConfigError as exc:
        typer.echo(str(exc))
        raise typer.Exit(2) from exc

    manager = ClusterManager(cfg)

    async def _session() -> None:
        await manager.start()
        try:
            await run_repl(manager)
        finally:
            await manager.stop()

    try:
        asyncio.run(_session())
    except LifecycleTimeout as exc:
        typer.echo(f"cluster failed to start: {exc}")
        raise typer.Exit(1) from exc


@app.command()
def run(
    scenario: Annotated[Path, typer.Option("--scenario", help="A scenario YAML file to run.")],
    seed: Annotated[
        int | None, typer.Option("--seed", help="Override the scenario's own seed.")
    ] = None,
    results_dir: Annotated[
        Path, typer.Option("--results-dir", help="Where to persist the run.")
    ] = Path("results"),
    plot: Annotated[
        Path | None,
        typer.Option("--plot", help="Write this run's plots into this directory."),
    ] = None,
) -> None:
    """Run one scenario file, persist it, and print a summary - the single-scenario
    sibling of ``stress``/``chaos``, over the same Phase 5 runner and Phase 7
    persistence machinery. ``--plot`` writes the latency/leadership/replication/
    consistency-availability plots ``experiments/visualization.py`` has always been
    able to produce (Phase 10 is what finally wires it into the CLI).
    """
    from pyraft_lab.experiments.metrics import report, summarize
    from pyraft_lab.experiments.replay import persist_run
    from pyraft_lab.experiments.runner import run_scenario
    from pyraft_lab.experiments.scenario import Scenario, ScenarioError

    try:
        parsed = Scenario.load(scenario)
    except ScenarioError as exc:
        typer.echo(f"cannot load {scenario}: {exc}")
        raise typer.Exit(2) from exc

    result = asyncio.run(run_scenario(parsed, seed=seed))
    path = persist_run(result, results_dir)
    document = report(result)
    typer.echo(summarize(document))
    typer.echo(f"\nwrote {path}")

    if plot is not None:
        from pyraft_lab.experiments.visualization import plot_run

        for graph in plot_run(result, plot):
            typer.echo(f"wrote {graph}")

    if not document["passed"]:
        raise typer.Exit(1)


@app.command()
def serve(
    host: Annotated[str, typer.Option("--host", help="Address to bind.")] = "127.0.0.1",
    port: Annotated[int, typer.Option("--port", help="Port to bind.")] = 8000,
    workspace: Annotated[
        Path,
        typer.Option("--workspace", help="Where cluster.yaml, scenarios/ and results/ live."),
    ] = Path("."),
) -> None:
    """Serve the HTTP API the web front end drives this project through.

    Same one-process model as ``cluster start`` (docs/architecture.md P9): the live
    cluster this serves belongs to *this* process and lives for exactly as long as it.
    Campaigns started over the API run on worker threads of their own so a
    virtual-clock run cannot starve the live cluster's wall-clock timers.
    """
    try:
        import uvicorn

        from pyraft_lab.api.server import create_app
    except ImportError as exc:
        typer.echo(f"the API needs its extra dependencies: pip install -e '.[api]'  ({exc})")
        raise typer.Exit(2) from exc

    typer.echo(f"pyraft-lab API on http://{host}:{port}  (workspace: {workspace.resolve()})")
    uvicorn.run(create_app(workspace), host=host, port=port, log_level="info")


@app.command()
def results(
    results_dir: Annotated[
        Path, typer.Option("--results-dir", help="Where persisted runs live.")
    ] = Path("results"),
) -> None:
    """List every persisted run, newest first."""
    import json

    from pyraft_lab.experiments.manifest import ManifestError, RunManifest
    from pyraft_lab.experiments.replay import MANIFEST_FILE, REPORT_FILE

    if not results_dir.exists():
        typer.echo(f"no results directory at {results_dir}")
        raise typer.Exit(2)

    rows: list[tuple[RunManifest, bool | None]] = []
    for run_dir in sorted(results_dir.iterdir()):
        manifest_path = run_dir / MANIFEST_FILE
        if not manifest_path.exists():
            continue
        try:
            manifest = RunManifest.load(manifest_path)
        except ManifestError as exc:
            typer.echo(f"  {run_dir.name}: unreadable manifest ({exc})")
            continue

        passed: bool | None = None
        report_path = run_dir / REPORT_FILE
        if report_path.exists():
            passed = json.loads(report_path.read_text(encoding="utf-8")).get("passed")
        rows.append((manifest, passed))

    if not rows:
        typer.echo(f"no runs in {results_dir}")
        return

    for manifest, passed in sorted(rows, key=lambda row: row[0].created_at, reverse=True):
        mark = "n/a" if passed is None else ("PASS" if passed else "FAIL")
        typer.echo(
            f"{manifest.run_id}  {manifest.scenario_name:<20}  seed={manifest.seed:<6}  "
            f"{mark:<4}  {manifest.created_at}"
        )


if __name__ == "__main__":  # pragma: no cover
    app()
