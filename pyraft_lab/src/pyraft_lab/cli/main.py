"""The ``pyraft-lab`` command line.

Commands appear only once the feature behind them is real; the rest are declared in the
plan and land with their phases. Nothing here prints a fabricated result in the meantime.

Phase 6 adds the two commands that only need a file to answer honestly: reading back a
WAL and reading back a snapshot. 2.0 item 10's ``--node`` forms of these (``snapshot
--node node-3``, ``inspect-snapshot --node node-3``) need a running cluster to address,
so they arrive with the cluster controller in Phase 9 rather than being faked now.

Phase 7 adds ``replay``, addressed by ``run_id`` against a directory of persisted runs.
There is deliberately no ``pyraft-lab run`` yet to produce those directories with - that
command needs the cluster/experiment CLI surface plan.md assigns to Phase 9, and a run
is fully usable from the Python API in the meantime (``persist_run`` in
``experiments/replay.py``), exactly as Phase 5's scenario runner has been all along.
"""

from __future__ import annotations

import asyncio
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
        nodes=nodes, trial_duration=trial_duration, trials=trials, base_seed=seed,
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
        nodes=nodes, trial_duration=trial_duration, trials=trials, base_seed=seed,
        results_dir=results_dir,
    )
    campaign = asyncio.run(run_chaos(config))
    typer.echo(campaign.explain())
    path = write_report(campaign, report)
    typer.echo(f"\nwrote {path}")
    if campaign.failed:
        raise typer.Exit(1)


if __name__ == "__main__":  # pragma: no cover
    app()
