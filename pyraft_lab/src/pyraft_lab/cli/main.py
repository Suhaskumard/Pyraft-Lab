"""The ``pyraft-lab`` command line.

Day 1 ships only the commands that can honestly be answered yet. The cluster and
experiment commands are declared in the plan and land with their phases; nothing here
prints a fabricated result in the meantime.
"""

from __future__ import annotations

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


if __name__ == "__main__":  # pragma: no cover
    app()
