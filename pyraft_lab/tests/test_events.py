"""Phase 4: the event vocabulary and the shape an event is written in.

The vocabulary is fixed by the specification, so these tests assert it exactly rather
than loosely - a kind quietly renamed is a trace nobody can read back, and a kind
quietly added is a vocabulary that has stopped being one.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from pyraft_lab.observability.events import (
    NETWORK_EVENTS,
    Event,
    EventKind,
    read_jsonl,
    to_jsonl,
    write_jsonl,
)

SPECIFIED_KINDS = {
    "ELECTION_STARTED",
    "VOTE_REQUESTED",
    "VOTE_GRANTED",
    "LEADER_ELECTED",
    "APPEND_ENTRIES_SENT",
    "APPEND_ENTRIES_REJECTED",
    "LOG_COMMITTED",
    "NODE_CRASHED",
    "NODE_RECOVERED",
    "PARTITION_CREATED",
    "PARTITION_HEALED",
    "TERM_CHANGED",
}


# --- the vocabulary ----------------------------------------------------------------


def test_the_vocabulary_is_exactly_the_specified_twelve() -> None:
    assert {kind.value for kind in EventKind} == SPECIFIED_KINDS


def test_a_kind_is_its_own_name() -> None:
    """So a trace read back from JSON needs no conversion to be compared or filtered."""
    for kind in EventKind:
        assert kind == kind.name
        assert str(kind) == kind.name


def test_an_unknown_kind_is_rejected_at_the_boundary() -> None:
    with pytest.raises(ValueError, match="NOPE"):
        EventKind("NOPE")


def test_network_narrated_events_are_a_subset_of_the_vocabulary() -> None:
    assert NETWORK_EVENTS.issubset(EventKind)


# --- the record ---------------------------------------------------------------------


def test_an_event_serializes_to_the_specified_shape() -> None:
    event = Event(
        kind=EventKind.LEADER_ELECTED,
        timestamp=4.210,
        seq=7,
        node="node-3",
        term=8,
        details={"votes": 3, "cluster_size": 5},
    )

    assert event.to_dict() == {
        "timestamp": 4.210,
        "seq": 7,
        "node": "node-3",
        "term": 8,
        "event": "LEADER_ELECTED",
        "details": {"votes": 3, "cluster_size": 5},
    }


def test_an_event_survives_a_round_trip() -> None:
    event = Event(
        kind=EventKind.LOG_COMMITTED,
        timestamp=1.5,
        seq=2,
        node="n1",
        term=3,
        details={"from": 4, "to": 9, "entries": 5},
    )
    assert Event.from_dict(event.to_dict()) == event


def test_a_network_event_round_trips_without_a_node_or_a_term() -> None:
    """PARTITION_CREATED belongs to no node and to no term - both must survive as null
    rather than being coerced into a value that would read as a real one."""
    event = Event(
        kind=EventKind.PARTITION_CREATED,
        timestamp=2.0,
        seq=1,
        details={"groups": [["n1", "n2"], ["n3"]]},
    )

    assert event.to_dict()["node"] is None
    assert event.to_dict()["term"] is None
    assert Event.from_dict(event.to_dict()) == event


def test_an_event_is_immutable() -> None:
    event = Event(kind=EventKind.TERM_CHANGED, timestamp=0.0, seq=0, node="n1", term=1)
    with pytest.raises(AttributeError):
        event.term = 2  # type: ignore[misc]


def test_events_order_by_timestamp_then_emission() -> None:
    """Under a virtual clock many events share one timestamp, so ``seq`` is what makes
    the order total - and a replay that reorders two same-instant events is not one."""
    first = Event(kind=EventKind.ELECTION_STARTED, timestamp=1.0, seq=0, node="n1")
    second = Event(kind=EventKind.VOTE_REQUESTED, timestamp=1.0, seq=1, node="n1")
    later = Event(kind=EventKind.LEADER_ELECTED, timestamp=2.0, seq=2, node="n1")

    assert sorted([later, second, first], key=lambda e: e.order) == [first, second, later]


def test_an_event_reads_as_a_line() -> None:
    event = Event(
        kind=EventKind.VOTE_GRANTED, timestamp=0.25, seq=3, node="n2", term=4, details={"c": "n1"}
    )
    rendered = str(event)

    assert "VOTE_GRANTED" in rendered
    assert "n2" in rendered
    assert "t4" in rendered
    assert "c=n1" in rendered


def test_a_network_event_reads_as_belonging_to_the_network() -> None:
    assert "network" in str(Event(kind=EventKind.PARTITION_HEALED, timestamp=0.0, seq=0))


# --- persistence --------------------------------------------------------------------


def test_a_trace_round_trips_through_jsonl(tmp_path: Path) -> None:
    events = [
        Event(kind=EventKind.ELECTION_STARTED, timestamp=0.1, seq=0, node="n1", term=1),
        Event(kind=EventKind.LEADER_ELECTED, timestamp=0.2, seq=1, node="n1", term=1),
        Event(kind=EventKind.PARTITION_CREATED, timestamp=0.3, seq=2, details={"groups": [["n1"]]}),
    ]
    path = tmp_path / "trace.jsonl"

    write_jsonl(path, events)

    assert read_jsonl(path) == events


def test_jsonl_is_one_event_per_line(tmp_path: Path) -> None:
    """Line-per-event so a run can be tailed while it happens and appended to without
    rewriting - a trace that is only valid once closed is no use during a hang."""
    events = [Event(kind=EventKind.TERM_CHANGED, timestamp=float(i), seq=i) for i in range(3)]

    rendered = to_jsonl(events)

    assert rendered.count("\n") == 3
    assert len(rendered.splitlines()) == 3


def test_reading_a_trace_tolerates_blank_lines(tmp_path: Path) -> None:
    path = tmp_path / "trace.jsonl"
    path.write_text('{"timestamp": 1.0, "seq": 0, "event": "NODE_CRASHED", "node": "n1"}\n\n')

    (event,) = read_jsonl(path)

    assert event.kind is EventKind.NODE_CRASHED
    assert event.details == {}


def test_an_empty_trace_writes_and_reads_as_empty(tmp_path: Path) -> None:
    path = tmp_path / "trace.jsonl"
    write_jsonl(path, [])
    assert read_jsonl(path) == []
