"""Phase 9: the REPL layer (``cli/repl.py``) - the parser, command dispatch, and the
loop itself, each tested at the level that doesn't need the one below it.

``tests/test_cluster.py`` covers ``ClusterManager`` in isolation; this file drives a
real, started ``ClusterManager`` (with ``FAST_ELECTION``/``FAST_HEARTBEAT``, same as
that file) through the REPL's own seams instead of a real terminal.
"""

from __future__ import annotations

import pytest
from tests.test_node_election import FAST_ELECTION, FAST_HEARTBEAT

from pyraft_lab.cli.repl import ReplCommandError, ReplParseError, _dispatch, parse_command, run_repl
from pyraft_lab.cluster.config import ClusterConfig
from pyraft_lab.cluster.manager import ClusterManager


def a_config(**overrides: object) -> ClusterConfig:
    data = {
        "nodes": 3,
        "election_timeout_range": FAST_ELECTION,
        "heartbeat_interval": FAST_HEARTBEAT,
    }
    data.update(overrides)
    return ClusterConfig(**data)


# --- parse_command: pure, no I/O ---------------------------------------------------------


def test_parse_command_splits_verb_and_args() -> None:
    assert parse_command("put a b") == ("put", ["a", "b"])


def test_parse_command_keeps_a_quoted_value_together() -> None:
    assert parse_command('put a "b c"') == ("put", ["a", "b c"])


def test_parse_command_is_case_insensitive_on_the_verb() -> None:
    assert parse_command("STATUS") == ("status", [])


@pytest.mark.parametrize("line", ["", "   ", "# a comment", "  # also a comment"])
def test_parse_command_blank_and_comment_lines_are_a_no_op(line: str) -> None:
    assert parse_command(line) == ("", [])


def test_parse_command_rejects_unbalanced_quotes() -> None:
    with pytest.raises(ReplParseError):
        parse_command("put 'a")


# --- dispatch: real commands against a real, running cluster -----------------------------


async def test_dispatch_put_then_get_roundtrips() -> None:
    manager = ClusterManager(a_config())
    await manager.start()
    try:
        assert "ok" in (await _dispatch(manager, "put", ["a", "1"])).lower()
        assert (await _dispatch(manager, "get", ["a"])) == "a = 1"
    finally:
        await manager.stop()


async def test_dispatch_get_of_a_missing_key_does_not_raise() -> None:
    manager = ClusterManager(a_config())
    await manager.start()
    try:
        output = await _dispatch(manager, "get", ["missing"])
        assert "no such key" in output
    finally:
        await manager.stop()


async def test_dispatch_restart_with_wrong_arity_raises() -> None:
    manager = ClusterManager(a_config())
    await manager.start()
    try:
        with pytest.raises(ReplCommandError):
            await _dispatch(manager, "restart", [])
    finally:
        await manager.stop()


async def test_dispatch_unknown_verb_raises() -> None:
    manager = ClusterManager(a_config())
    await manager.start()
    try:
        with pytest.raises(ReplCommandError, match="unknown command"):
            await _dispatch(manager, "bogus", [])
    finally:
        await manager.stop()


async def test_dispatch_node_inspect_and_logs() -> None:
    manager = ClusterManager(a_config())
    await manager.start()
    try:
        node_id = manager.topology()[0].node_id
        inspected = await _dispatch(manager, "node", ["inspect", node_id])
        assert f"node: {node_id}" in inspected
        assert "role:" in inspected

        logs = await _dispatch(manager, "node", ["logs", node_id])
        assert logs  # at least the election traffic that got it started
    finally:
        await manager.stop()


# --- run_repl: a scripted session, no real stdin ------------------------------------------


async def test_a_scripted_session_puts_gets_and_exits(capsys: pytest.CaptureFixture[str]) -> None:
    manager = ClusterManager(a_config())
    await manager.start()

    lines = iter(["put a 1", "get a", "status", "exit"])

    async def fake_read_line(prompt: str) -> str:
        return next(lines)

    try:
        await run_repl(manager, read_line=fake_read_line)
    finally:
        # run_repl never stops the cluster itself - confirms the "caller owns
        # shutdown" contract.
        assert manager.client is not None
        await manager.stop()

    out = capsys.readouterr().out
    assert "a = 1" in out
    assert "status: HEALTHY" in out


async def test_a_scripted_session_survives_an_unknown_command(
    capsys: pytest.CaptureFixture[str],
) -> None:
    manager = ClusterManager(a_config())
    await manager.start()

    lines = iter(["nonsense", "exit"])

    async def fake_read_line(prompt: str) -> str:
        return next(lines)

    try:
        await run_repl(manager, read_line=fake_read_line)  # must not raise
    finally:
        await manager.stop()

    out = capsys.readouterr().out
    assert "error: unknown command" in out


async def test_a_scripted_session_ends_cleanly_on_eof(capsys: pytest.CaptureFixture[str]) -> None:
    manager = ClusterManager(a_config())
    await manager.start()

    async def fake_read_line(prompt: str) -> str:
        raise EOFError

    try:
        await run_repl(manager, read_line=fake_read_line)  # must not raise
    finally:
        await manager.stop()
