"""The cluster controller (Phase 9, 2.0 item 2): a real, live, in-process cluster a
human can start, interact with and stop.

Everything else in this project either runs a whole simulated experiment start-to-
finish (``experiments/``) or exercises a bare list of ``RaftNode``s directly (the
Phase 1 test suite). Neither is "a cluster you can `put` a value into and `get` it
back a minute later while thinking about something else" - that is what this package
is for.

``config.py`` (``ClusterConfig``) is the value a cluster is built from. ``lifecycle.py``
(``LifecycleManager``) is startup/shutdown sequencing, factored out so it is reusable
against a bare node list the same way it is used here. ``manager.py`` (``NodeManager``,
``ClusterManager``) is the orchestration itself - one member's lifecycle, and the whole
cluster's shared transport, tracer and client.

There is deliberately no daemon, socket, or PID file anywhere in here: a cluster lives
for exactly as long as the process that started it, and ``cli/repl.py`` is that
process's interactive session. See docs/architecture.md P9 for why.
"""

from __future__ import annotations
