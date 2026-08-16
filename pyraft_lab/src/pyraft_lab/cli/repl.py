"""The interactive session ``cluster start`` hands control to.

Two seams, kept deliberately separate so each is testable without the other:

- :func:`parse_command` is pure - a line of text in, a ``(verb, args)`` pair out - and
  never touches I/O. :func:`_dispatch` is one step up: it runs a real command against
  a real, running :class:`~pyraft_lab.cluster.manager.ClusterManager`, but still with
  no stdin/stdout of its own.
- :func:`run_repl` is the only piece that reads a line and prints a result. Its
  ``read_line`` argument is injectable specifically so a test can drive the whole loop
  with canned input and never touch a real terminal.

The read itself goes through ``asyncio.to_thread`` rather than a bare ``input()`` call
in the coroutine: a bare call would block the one event loop every node's heartbeat
and election timer in this process shares, freezing the cluster between keystrokes.
``asyncio.to_thread`` moves the blocking read to a worker thread while the loop - and
every ``RaftNode`` task on it - keeps running underneath the ``await``. See
docs/architecture.md P9 for the Windows/Ctrl+C caveat this carries.

``run_repl`` never stops the cluster itself - starting and stopping the
``ClusterManager`` is the caller's job (``cli/main.py``'s ``cluster start``), wrapped
in a ``try/finally`` so every exit path - ``exit``/``quit``, EOF, or an unexpected
error - still tears the cluster down cleanly.
"""

from __future__ import annotations

import asyncio
import shlex
from collections.abc import Awaitable, Callable

import typer

from pyraft_lab import NodeId
from pyraft_lab.client.client import RequestFailed
from pyraft_lab.cluster.lifecycle import LifecycleTimeout
from pyraft_lab.cluster.manager import ClusterError, ClusterManager
from pyraft_lab.dashboard.app import run_dashboard

PROMPT = "pyraft-lab> "

HELP_TEXT = """\
Commands:
  put <key> <value>            store a value
  get <key>                    read a value
  delete <key>                 remove a key
  cas <key> <expected> <new>   compare-and-swap
  status                       cluster health, term, leader
  topology                     each node's role, term, leader
  show-log [node]              the replicated Raft log (default: the leader)
  node inspect <id>            one node's full state
  node logs <id> [--last N]    that node's recent trace events (default 20)
  restart <id>                 crash and restart one node
  trace [--last N]             the cluster's recent trace events (default 50)
  dashboard [--refreshes N] [--interval S]
                                live view, refreshing every S seconds (default 1.0);
                                runs until Ctrl+C, or for N frames if given
  help                         this text
  exit | quit                  stop the cluster and leave"""


class ReplParseError(Exception):
    """A REPL line could not be parsed."""


class ReplCommandError(Exception):
    """A REPL command could not be carried out - bad syntax, unknown id, and so on."""


# --- parsing: pure, no I/O -----------------------------------------------------------


def parse_command(line: str) -> tuple[str, list[str]]:
    """Split one REPL line into ``(verb, args)``.

    A blank line or a ``#``-comment both parse to ``("", [])`` - the loop's cue to
    read again without doing anything. Uses ``shlex`` so ``put greeting "hello
    world"`` keeps the quoted value together as one argument.
    """
    stripped = line.strip()
    if not stripped or stripped.startswith("#"):
        return "", []
    try:
        parts = shlex.split(stripped)
    except ValueError as exc:  # unbalanced quotes
        raise ReplParseError(f"could not parse: {exc}") from exc
    if not parts:
        return "", []
    return parts[0].lower(), parts[1:]


def _parse_last(args: list[str], *, default: int) -> int:
    if not args:
        return default
    if len(args) == 2 and args[0] == "--last":
        try:
            return int(args[1])
        except ValueError as exc:
            raise ReplCommandError(f"--last needs a number, got {args[1]!r}") from exc
    raise ReplCommandError("usage: trace [--last N]")


def _parse_dashboard_args(args: list[str]) -> tuple[int | None, float]:
    """``[--refreshes N] [--interval S]``, either flag optional, in either order."""
    refreshes: int | None = None
    interval = 1.0
    remaining = list(args)
    while remaining:
        flag, remaining = remaining[0], remaining[1:]
        if flag == "--refreshes" and remaining:
            value, remaining = remaining[0], remaining[1:]
            try:
                refreshes = int(value)
            except ValueError as exc:
                raise ReplCommandError(f"--refreshes needs a number, got {value!r}") from exc
        elif flag == "--interval" and remaining:
            value, remaining = remaining[0], remaining[1:]
            try:
                interval = float(value)
            except ValueError as exc:
                raise ReplCommandError(f"--interval needs a number, got {value!r}") from exc
        else:
            raise ReplCommandError("usage: dashboard [--refreshes N] [--interval S]")
    return refreshes, interval


def _parse_id_and_last(args: list[str], *, default_last: int) -> tuple[NodeId, int]:
    if not args:
        raise ReplCommandError("usage: node logs <id> [--last N]")
    node_id, rest = args[0], args[1:]
    if not rest:
        return node_id, default_last
    if len(rest) == 2 and rest[0] == "--last":
        try:
            return node_id, int(rest[1])
        except ValueError as exc:
            raise ReplCommandError(f"--last needs a number, got {rest[1]!r}") from exc
    raise ReplCommandError("usage: node logs <id> [--last N]")


# --- commands: real work, no I/O of their own -----------------------------------------


async def _cmd_put(manager: ClusterManager, args: list[str]) -> str:
    if len(args) != 2:
        raise ReplCommandError("usage: put <key> <value>")
    key, value = args
    assert manager.client is not None
    await manager.client.put(key, value)
    return f"ok: {key} = {value}"


async def _cmd_get(manager: ClusterManager, args: list[str]) -> str:
    if len(args) != 1:
        raise ReplCommandError("usage: get <key>")
    (key,) = args
    assert manager.client is not None
    value, ok = await manager.client.read(key)
    return f"{key} = {value}" if ok else f"no such key: {key}"


async def _cmd_delete(manager: ClusterManager, args: list[str]) -> str:
    if len(args) != 1:
        raise ReplCommandError("usage: delete <key>")
    (key,) = args
    assert manager.client is not None
    existed = await manager.client.delete(key)
    return f"deleted {key}" if existed else f"no such key: {key}"


async def _cmd_cas(manager: ClusterManager, args: list[str]) -> str:
    if len(args) != 3:
        raise ReplCommandError("usage: cas <key> <expected> <new>")
    key, expected, new = args
    assert manager.client is not None
    won = await manager.client.cas(key, expected, new)
    return f"cas {key}: ok" if won else f"cas {key}: failed (value did not match {expected!r})"


async def _cmd_status(manager: ClusterManager, args: list[str]) -> str:
    if args:
        raise ReplCommandError("usage: status")
    snap = manager.status()
    return "\n".join(
        [
            f"status: {snap['status']}",
            f"term: {snap['term']}   leader: {snap['leader'] or '(none)'}",
            f"nodes: {', '.join(snap['nodes'])}",
            f"down: {', '.join(snap['down']) or '(none)'}",
            f"partitioned: {snap['partitioned']}",
        ]
    )


async def _cmd_topology(manager: ClusterManager, args: list[str]) -> str:
    if args:
        raise ReplCommandError("usage: topology")
    lines = [f"{'node':<7} {'alive':<6} {'role':<10} {'term':<5} leader"]
    for row in manager.topology():
        role = row.role or "-"
        term = "-" if row.term is None else str(row.term)
        leader = row.leader_id or "-"
        lines.append(f"{row.node_id:<7} {str(row.alive):<6} {role:<10} {term:<5} {leader}")
    return "\n".join(lines)


async def _cmd_show_log(manager: ClusterManager, args: list[str]) -> str:
    if len(args) > 1:
        raise ReplCommandError("usage: show-log [node]")
    node_id = args[0] if args else (manager.status()["leader"] or manager.config.node_ids[0])
    try:
        node = manager.node(node_id)
    except ClusterError as exc:
        raise ReplCommandError(str(exc)) from exc

    lines = [f"log for {node_id} (commit_index={node.state.commit_index}):"]
    for index in range(1, node.state.log.last_index + 1):
        entry = node.state.log.entry_at(index)
        if entry is None:
            continue
        committed = "committed" if index <= node.state.commit_index else "uncommitted"
        lines.append(f"  {index:>4}  term={entry.term}  {committed}  {entry.command}")
    return "\n".join(lines)


async def _node_inspect(manager: ClusterManager, args: list[str]) -> str:
    if len(args) != 1:
        raise ReplCommandError("usage: node inspect <id>")
    (node_id,) = args
    try:
        node = manager.node(node_id)
    except ClusterError as exc:
        raise ReplCommandError(str(exc)) from exc

    node_manager = manager.nodes[node_id]
    wal = str(node_manager.wal_path) if node_manager.wal_path is not None else "in-memory"
    return "\n".join(
        [
            f"node: {node_id}",
            f"  alive: {node_manager.running}",
            f"  role: {node.role.value}",
            f"  term: {node.current_term}",
            f"  leader_id: {node.leader_id or '(none)'}",
            f"  commit_index: {node.state.commit_index}",
            f"  last_applied: {node.state.last_applied}",
            f"  log_length: {node.state.log.last_index}",
            f"  peers: {', '.join(node.peers)}",
            f"  persistence: {wal}",
            f"  store keys: {len(node.store)}",
        ]
    )


async def _node_logs(manager: ClusterManager, args: list[str]) -> str:
    node_id, last = _parse_id_and_last(args, default_last=20)
    try:
        manager.node(node_id)  # just validates the id exists
    except ClusterError as exc:
        raise ReplCommandError(str(exc)) from exc

    events = manager.tracer.for_node(node_id)[-last:]
    return manager.tracer.story(events) if events else f"no trace events for {node_id}"


async def _cmd_node(manager: ClusterManager, args: list[str]) -> str:
    if not args:
        raise ReplCommandError("usage: node inspect <id> | node logs <id> [--last N]")
    sub, rest = args[0], args[1:]
    if sub == "inspect":
        return await _node_inspect(manager, rest)
    if sub == "logs":
        return await _node_logs(manager, rest)
    raise ReplCommandError(f"unknown 'node' subcommand: {sub!r}")


async def _cmd_restart(manager: ClusterManager, args: list[str]) -> str:
    if len(args) != 1:
        raise ReplCommandError("usage: restart <id>")
    (node_id,) = args
    try:
        await manager.restart_node(node_id)
    except ClusterError as exc:
        raise ReplCommandError(str(exc)) from exc
    except LifecycleTimeout as exc:
        raise ReplCommandError(f"restarted {node_id}, but no leader was re-elected: {exc}") from exc
    return f"restarted {node_id}"


async def _cmd_trace(manager: ClusterManager, args: list[str]) -> str:
    last = _parse_last(args, default=50)
    events = manager.tracer.events[-last:]
    return manager.tracer.story(events) if events else "(no events yet)"


async def _cmd_dashboard(manager: ClusterManager, args: list[str]) -> str:
    """Live view, refreshing in place. Prints its own frames via ``typer.echo`` as it
    goes rather than returning one string - the only handler that does, because a
    dashboard is several frames over time, not one answer. Returns "" so ``run_repl``
    doesn't print anything a second time."""
    refreshes, interval = _parse_dashboard_args(args)
    if interval <= 0:
        raise ReplCommandError("--interval must be positive")
    await run_dashboard(manager, refreshes=refreshes, interval=interval, echo=typer.echo)
    return ""


async def _cmd_help(manager: ClusterManager, args: list[str]) -> str:
    return HELP_TEXT


Handler = Callable[[ClusterManager, list[str]], Awaitable[str]]

_HANDLERS: dict[str, Handler] = {
    "put": _cmd_put,
    "get": _cmd_get,
    "delete": _cmd_delete,
    "cas": _cmd_cas,
    "status": _cmd_status,
    "topology": _cmd_topology,
    "show-log": _cmd_show_log,
    "node": _cmd_node,
    "restart": _cmd_restart,
    "trace": _cmd_trace,
    "dashboard": _cmd_dashboard,
    "help": _cmd_help,
}


async def _dispatch(manager: ClusterManager, verb: str, args: list[str]) -> str:
    handler = _HANDLERS.get(verb)
    if handler is None:
        raise ReplCommandError(f"unknown command: {verb!r} (try 'help')")
    return await handler(manager, args)


async def execute_command(manager: ClusterManager, line: str) -> str:
    """Run one REPL line against ``manager`` and return what it printed.

    The seam a caller that is not a terminal - the HTTP API's console - drives the
    session through, so a command typed in a browser and the same command typed at
    ``pyraft-lab cluster start`` go through one implementation rather than two.
    ``dashboard`` is excluded because it writes frames to a terminal over time rather
    than returning one answer.
    """
    verb, args = parse_command(line)
    if not verb:
        return ""
    if verb in ("exit", "quit"):
        raise ReplCommandError("the cluster is stopped from the API, not with 'exit'")
    if verb == "dashboard":
        raise ReplCommandError("'dashboard' is a terminal view; the live cluster pages replace it")
    return await _dispatch(manager, verb, args)


# --- the loop: the only piece that touches real I/O -----------------------------------


async def _next_line(prompt: str) -> str:
    return await asyncio.to_thread(input, prompt)


def _banner(manager: ClusterManager) -> str:
    snap = manager.status()
    return (
        f"{len(manager.nodes)}-node cluster running "
        f"(leader: {snap['leader'] or '(none)'}, term: {snap['term']}).\n"
        "Type 'help' for commands, 'exit' to stop the cluster and leave."
    )


async def run_repl(
    manager: ClusterManager, *, read_line: Callable[[str], Awaitable[str]] = _next_line
) -> None:
    """Drive the session until ``exit``/``quit``/EOF. Never raises for a bad command -
    only ``KeyboardInterrupt``/``EOFError`` on the read itself end the loop."""
    typer.echo(_banner(manager))
    while True:
        try:
            line = await read_line(PROMPT)
        except EOFError:
            break
        except KeyboardInterrupt:
            typer.echo("")
            break

        try:
            verb, args = parse_command(line)
        except ReplParseError as exc:
            typer.echo(f"error: {exc}")
            continue
        if not verb:
            continue
        if verb in ("exit", "quit"):
            break

        try:
            output = await _dispatch(manager, verb, args)
        except (ReplCommandError, ReplParseError, ClusterError, RequestFailed) as exc:
            typer.echo(f"error: {exc}")
            continue
        except Exception as exc:  # last-resort guard: one bad command must not end the session
            typer.echo(f"error: unexpected: {exc}")
            continue
        if output:
            typer.echo(output)
