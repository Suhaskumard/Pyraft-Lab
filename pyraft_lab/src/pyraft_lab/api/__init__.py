"""The HTTP API the browser front end drives this project through.

Everything here is a thin adapter. No consensus logic, no metric, and no fault model
lives in this package: a route reads a scenario, a live ``ClusterManager`` or a
persisted run through exactly the API ``pyraft-lab``'s own commands already use, and
shapes the answer as JSON. The rule the CLI set for itself in ``cli/main.py`` holds
here too - a question this build cannot answer honestly gets no endpoint rather than a
stubbed one.
"""

from __future__ import annotations

__all__ = ["create_app"]


def create_app(*args: object, **kwargs: object) -> object:
    """Build the FastAPI application. Imported lazily so ``pyraft_lab.api`` can be
    imported (and its version checked) without FastAPI installed."""
    from pyraft_lab.api.server import create_app as _create_app

    return _create_app(*args, **kwargs)  # type: ignore[arg-type]
