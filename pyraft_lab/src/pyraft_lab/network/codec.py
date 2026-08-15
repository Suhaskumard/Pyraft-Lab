"""Serialization for envelopes.

One encode/decode pair, behind a protocol. JSON is the Day 1 choice because it is
readable in test failures and needs no dependency; swapping in msgpack later means
adding one class here and nothing else.
"""

from __future__ import annotations

import json
from typing import Any, Protocol, runtime_checkable

from pyraft_lab.network.messages import Envelope, Message, message_class


class DecodeError(Exception):
    """Raised when bytes on the wire cannot be turned back into an envelope."""


@runtime_checkable
class Codec(Protocol):
    """Turns an :class:`Envelope` into bytes and back."""

    def encode(self, envelope: Envelope) -> bytes: ...

    def decode(self, raw: bytes) -> Envelope: ...


class JsonCodec:
    """UTF-8 JSON codec.

    The message body is serialized through its own concrete class and tagged with
    ``kind``, so the decoder rebuilds the exact subclass rather than the ``Message``
    base (which would silently drop every field).
    """

    def encode(self, envelope: Envelope) -> bytes:
        wire: dict[str, Any] = {
            "src": envelope.src,
            "dst": envelope.dst,
            "seq": envelope.seq,
            "kind": envelope.body.KIND,
            "body": envelope.body.model_dump(mode="json"),
        }
        return json.dumps(wire, separators=(",", ":"), sort_keys=True).encode("utf-8")

    def decode(self, raw: bytes) -> Envelope:
        try:
            wire = json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise DecodeError(f"malformed frame: {exc}") from exc

        if not isinstance(wire, dict):
            raise DecodeError(f"expected a JSON object, got {type(wire).__name__}")

        missing = {"src", "dst", "seq", "kind", "body"} - set(wire)
        if missing:
            raise DecodeError(f"frame missing fields: {sorted(missing)}")

        try:
            body_cls: type[Message] = message_class(wire["kind"])
            body = body_cls.model_validate(wire["body"])
            return Envelope(src=wire["src"], dst=wire["dst"], seq=wire["seq"], body=body)
        except DecodeError:
            raise
        except Exception as exc:
            raise DecodeError(f"invalid {wire['kind']!r} frame: {exc}") from exc


DEFAULT_CODEC: Codec = JsonCodec()
