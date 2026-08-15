"""Commands must survive the trip into a log entry and back out again."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from pyraft_lab.kv.commands import (
    Cas,
    Command,
    Delete,
    Get,
    Put,
    UnknownCommandKind,
    command_from_wire,
    registered_command_kinds,
)
from pyraft_lab.network.codec import JsonCodec
from pyraft_lab.network.messages import Envelope
from pyraft_lab.raft.log import LogEntry
from pyraft_lab.raft.rpc import AppendEntries


@pytest.mark.parametrize(
    "command",
    [
        Put(key="a", value=1),
        Put(key="a", value={"nested": [1, 2, 3]}),
        Get(key="a"),
        Delete(key="a"),
        Cas(key="a", expected=None, new="created"),
        Cas(key="a", expected=1, new=2),
    ],
)
def test_a_command_round_trips_through_its_wire_form(command: Command) -> None:
    result = command_from_wire(command.to_wire())

    assert type(result) is type(command)
    assert result == command


def test_the_kind_tag_is_what_makes_the_round_trip_possible() -> None:
    assert Put(key="a", value=1).to_wire()["kind"] == "put"
    assert {"put", "get", "delete", "cas"} <= registered_command_kinds()


def test_commands_are_frozen() -> None:
    command = Put(key="a", value=1)
    with pytest.raises(ValidationError):
        command.key = "b"  # type: ignore[misc]


@pytest.mark.parametrize("wire", [{"kind": "nope"}, "not-a-dict", 42, {}])
def test_an_undecodable_command_is_rejected(wire: object) -> None:
    with pytest.raises(UnknownCommandKind):
        command_from_wire(wire)


def test_duplicate_kind_is_a_definition_error() -> None:
    with pytest.raises(TypeError, match="already registered"):

        class Impostor(Command):
            KIND = "put"

            key: str


def test_command_without_kind_is_a_definition_error() -> None:
    with pytest.raises(TypeError, match="KIND"):

        class Anonymous(Command):
            pass


def test_a_command_survives_replication_over_the_wire() -> None:
    """The real path: command -> log entry -> AppendEntries -> JSON -> back."""
    original = Cas(key="counter", expected=1, new=2)
    envelope = Envelope(
        src="n1",
        dst="n2",
        seq=0,
        body=AppendEntries(
            term=1,
            leader_id="n1",
            prev_log_index=0,
            prev_log_term=0,
            entries=[LogEntry(term=1, index=1, command=original.to_wire())],
            leader_commit=0,
        ),
    )

    codec = JsonCodec()
    decoded = codec.decode(codec.encode(envelope))

    assert isinstance(decoded.body, AppendEntries)
    delivered = command_from_wire(decoded.body.entries[0].command)
    assert delivered == original
