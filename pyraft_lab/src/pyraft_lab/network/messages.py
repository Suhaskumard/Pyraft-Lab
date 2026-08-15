"""Wire message types and the envelope that carries them between nodes.

Every message subclasses :class:`Message` and declares a unique ``KIND``. Subclasses
register themselves on definition, so the decoder can rebuild the concrete type from
the ``kind`` field alone. The Raft RPCs land here on Day 2 without touching the codec
or the bus.
"""

from __future__ import annotations

from typing import Any, ClassVar

from pydantic import BaseModel, ConfigDict, Field

from pyraft_lab import NodeId

_REGISTRY: dict[str, type[Message]] = {}


class Message(BaseModel):
    """Base class for anything that can travel over a transport.

    Messages are frozen: once a node hands one to the bus it must not observe later
    mutation, which would be impossible across a real network.
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    KIND: ClassVar[str] = ""

    def __init_subclass__(cls, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)
        kind = cls.__dict__.get("KIND")
        if not kind:
            raise TypeError(f"{cls.__name__} must declare a non-empty KIND")
        if kind in _REGISTRY and _REGISTRY[kind] is not cls:
            raise TypeError(
                f"message kind {kind!r} already registered to {_REGISTRY[kind].__name__}"
            )
        _REGISTRY[kind] = cls


def message_class(kind: str) -> type[Message]:
    """Look up the message class registered under ``kind``."""
    try:
        return _REGISTRY[kind]
    except KeyError:
        raise UnknownMessageKind(kind) from None


def registered_kinds() -> frozenset[str]:
    """Every message kind known to this process."""
    return frozenset(_REGISTRY)


class UnknownMessageKind(Exception):
    """Raised when a decoded envelope names a message kind this process has never seen."""

    def __init__(self, kind: str) -> None:
        super().__init__(f"unknown message kind {kind!r}; known kinds: {sorted(_REGISTRY)}")
        self.kind = kind


class Envelope(BaseModel):
    """A message plus its addressing metadata.

    The envelope is what a transport actually queues, delays, drops or reorders. The
    body inside it is opaque to the network layer.
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    src: NodeId
    dst: NodeId
    seq: int = Field(ge=0, description="Per-sender monotonic counter, for tracing and reordering")
    body: Message

    @property
    def kind(self) -> str:
        return self.body.KIND

    def __str__(self) -> str:  # pragma: no cover - debugging aid
        return f"#{self.seq} {self.src}->{self.dst} {self.kind}"


# --- Day 1 message types -------------------------------------------------------------
# Enough to prove the transport carries arbitrary payloads. Raft RPCs arrive on Day 2.


class Ping(Message):
    """Liveness probe carrying a nonce the responder echoes back."""

    KIND: ClassVar[str] = "ping"

    nonce: int


class Pong(Message):
    """Reply to a :class:`Ping`, echoing its nonce."""

    KIND: ClassVar[str] = "pong"

    nonce: int


class Blob(Message):
    """An arbitrary JSON-serializable payload, used to exercise the transport."""

    KIND: ClassVar[str] = "blob"

    data: dict[str, Any] = Field(default_factory=dict)
