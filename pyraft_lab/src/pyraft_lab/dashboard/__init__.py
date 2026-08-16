"""The local dashboard (Phase 11, 2.0 item 3, "if time remains"): a terminal-based
live view of a running :class:`~pyraft_lab.cluster.manager.ClusterManager`, not a web
app - 2.0 item 16 keeps a web UI out of scope, and the whole project already rejects
anything that would need a socket (see docs/architecture.md D1, P9).

``app.py`` is deliberately framework-agnostic: it builds a plain string from a live
cluster's state and knows nothing about ``typer`` or a terminal. ``cli/repl.py``'s new
``dashboard`` command is the one place that actually prints it, the same layering
every other package in this project already keeps (``experiments/*.py`` builds
reports; ``cli/`` is what echoes them).
"""

from __future__ import annotations
