"""The ``pyraft-lab`` command line.

Commands appear only once the feature behind them is real; the rest are declared in the
plan and land with their phases. Nothing here prints a fabricated result in the meantime.

Phase 6 adds the two commands that only need a file to answer honestly: reading back a
WAL and reading back a snapshot. 2.0 item 10's ``--node`` forms of these (``snapshot
--node node-3``, ``inspect-snapshot --node node-3``) need a running cluster to address,
so they arrive with the cluster controller in Phase 9 rather than being faked now.
"""

from __future__ import annotations

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


if __name__ == "__main__":  # pragma: no cover
    app()
