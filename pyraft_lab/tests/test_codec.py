"""Serialization must survive the round trip without losing the concrete message type."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from pyraft_lab.network import (
    Blob,
    DecodeError,
    Envelope,
    JsonCodec,
    Ping,
    Pong,
    message_class,
    registered_kinds,
)
from pyraft_lab.network.messages import Message, UnknownMessageKind

CODEC = JsonCodec()


def roundtrip(envelope: Envelope) -> Envelope:
    return CODEC.decode(CODEC.encode(envelope))


@pytest.mark.parametrize(
    "body",
    [
        Ping(nonce=1),
        Pong(nonce=99),
        Blob(),
        Blob(data={"key": "foo", "value": "bar", "nested": {"n": [1, 2, 3]}}),
    ],
)
def test_roundtrip_preserves_type_and_fields(body: Message) -> None:
    original = Envelope(src="node1", dst="node2", seq=7, body=body)
    result = roundtrip(original)

    assert type(result.body) is type(body)
    assert result.body == body
    assert (result.src, result.dst, result.seq) == ("node1", "node2", 7)


def test_encoding_is_deterministic() -> None:
    """Same envelope, same bytes - a precondition for reproducible runs."""
    envelope = Envelope(src="node1", dst="node2", seq=0, body=Blob(data={"b": 2, "a": 1}))
    assert CODEC.encode(envelope) == CODEC.encode(envelope)


def test_kind_travels_on_the_wire() -> None:
    raw = CODEC.encode(Envelope(src="a", dst="b", seq=0, body=Ping(nonce=5)))
    assert b'"kind":"ping"' in raw


def test_messages_are_frozen() -> None:
    ping = Ping(nonce=1)
    with pytest.raises(ValidationError):
        ping.nonce = 2  # type: ignore[misc]


def test_registry_maps_kind_to_class() -> None:
    assert message_class("ping") is Ping
    assert {"ping", "pong", "blob"} <= registered_kinds()


def test_unknown_kind_is_rejected() -> None:
    with pytest.raises(UnknownMessageKind):
        message_class("no-such-kind")


@pytest.mark.parametrize(
    "raw",
    [
        b"not json at all",
        b"[1, 2, 3]",
        b'{"src":"a","dst":"b","seq":0}',
        b'{"src":"a","dst":"b","seq":0,"kind":"nope","body":{}}',
        b'{"src":"a","dst":"b","seq":0,"kind":"ping","body":{}}',
        b'{"src":"a","dst":"b","seq":0,"kind":"ping","body":{"nonce":"not-an-int"}}',
    ],
)
def test_malformed_frames_raise_decode_error(raw: bytes) -> None:
    with pytest.raises(DecodeError):
        CODEC.decode(raw)


def test_duplicate_kind_is_a_definition_error() -> None:
    """Two message classes claiming one kind would make decoding ambiguous."""
    with pytest.raises(TypeError, match="already registered"):

        class Impostor(Message):
            KIND = "ping"

            nonce: int


def test_message_without_kind_is_a_definition_error() -> None:
    with pytest.raises(TypeError, match="KIND"):

        class Anonymous(Message):
            pass
