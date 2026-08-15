"""The install and entry point must work from a clean checkout."""

from __future__ import annotations

import importlib

import pytest
from typer.testing import CliRunner

from pyraft_lab import __version__
from pyraft_lab.cli.main import app

runner = CliRunner()


@pytest.mark.parametrize(
    "module",
    [
        "pyraft_lab",
        "pyraft_lab.raft",
        "pyraft_lab.kv",
        "pyraft_lab.network",
        "pyraft_lab.client",
        "pyraft_lab.experiments",
        "pyraft_lab.observability",
        "pyraft_lab.consistency",
        "pyraft_lab.cli",
    ],
)
def test_every_package_imports(module: str) -> None:
    assert importlib.import_module(module) is not None


def test_cli_reports_the_version() -> None:
    result = runner.invoke(app, ["version"])
    assert result.exit_code == 0
    assert __version__ in result.stdout


def test_cli_lists_message_kinds() -> None:
    result = runner.invoke(app, ["kinds"])
    assert result.exit_code == 0
    assert {"blob", "ping", "pong"} <= set(result.stdout.split())
