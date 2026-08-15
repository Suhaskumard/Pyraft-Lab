"""What a client asks the state machine to do.

Commands ride inside ``LogEntry.command``, which Raft types as ``Any`` and treats as
opaque - the consensus layer must never need to understand a payload to replicate it.
That opacity has a cost: a command put into a log entry is serialized as plain JSON by
the codec, so it comes back as a bare ``dict`` with its class forgotten, exactly the
type loss ``docs/architecture.md`` D1 describes for messages.

The fix is the same one: commands travel as ``kind``-tagged dicts (:meth:`Command.to_wire`)
and are rebuilt through a registry (:func:`command_from_wire`). Raft still sees nothing
but a dict; only ``kv/store.py`` ever decodes one.
"""

from __future__ import annotations

from typing import Any, ClassVar

from pydantic import BaseModel, ConfigDict

_REGISTRY: dict[str, type[Command]] = {}


class UnknownCommandKind(Exception):
    """Raised when a log entry names a command kind this process has never seen."""

    def __init__(self, kind: str) -> None:
        super().__init__(f"unknown command kind {kind!r}; known kinds: {sorted(_REGISTRY)}")
        self.kind = kind


class Command(BaseModel):
    """Base class for anything the state machine can apply.

    Frozen for the same reason messages are: a command sitting in a replicated log is
    history, and history does not get edited in place.
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
                f"command kind {kind!r} already registered to {_REGISTRY[kind].__name__}"
            )
        _REGISTRY[kind] = cls

    def to_wire(self) -> dict[str, Any]:
        """The dict form that goes into a log entry."""
        return {"kind": self.KIND, **self.model_dump(mode="json")}


def command_from_wire(wire: Any) -> Command:
    """Rebuild the concrete command from what came out of a log entry."""
    if not isinstance(wire, dict):
        raise UnknownCommandKind(str(wire))
    try:
        cls = _REGISTRY[wire["kind"]]
    except KeyError:
        raise UnknownCommandKind(str(wire.get("kind"))) from None
    return cls.model_validate({k: v for k, v in wire.items() if k != "kind"})


def registered_command_kinds() -> frozenset[str]:
    return frozenset(_REGISTRY)


class Put(Command):
    """Set ``key`` to ``value``, creating it if absent."""

    KIND: ClassVar[str] = "put"

    key: str
    value: Any


class Get(Command):
    """Read ``key``.

    A read changes nothing, so replicating it through the log is not what makes it
    correct - it is here so a read can be *ordered* against writes in a recorded
    history, which is what Phase 5's linearizability checker consumes. ``RaftNode``
    never actually appends a ``Get`` to the log: Day 10 serves it straight from
    :meth:`KVStore.read`, gated on a leader-lease check, once the applied index has
    caught up to the commit index the read arrived at.
    """

    KIND: ClassVar[str] = "get"

    key: str


class Delete(Command):
    """Remove ``key`` if it is present."""

    KIND: ClassVar[str] = "delete"

    key: str


class Cas(Command):
    """Compare-and-swap: set ``key`` to ``new`` only if it currently holds ``expected``.

    ``expected=None`` means "only if the key is absent", which makes CAS usable as an
    atomic create.
    """

    KIND: ClassVar[str] = "cas"

    key: str
    expected: Any
    new: Any
