"""Phase 4: the recorder itself - ordering, subscription, and being free when off.

Driven against a ``VirtualClock`` throughout: a tracer's whole value is that the time
on an event is the time the run believed it was, so these assert exact timestamps
rather than plausible ones.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from pyraft_lab.network.clock import VirtualClock
from pyraft_lab.observability.events import Event, EventKind, read_jsonl
from pyraft_lab.observability.tracer import NULL_TRACER, Tracer


@pytest.fixture
def clock() -> VirtualClock:
    return VirtualClock()


@pytest.fixture
def tracer(clock: VirtualClock) -> Tracer:
    return Tracer(clock)


# --- recording ----------------------------------------------------------------------


def test_an_event_is_stamped_with_the_injected_clock(tracer: Tracer, clock: VirtualClock) -> None:
    event = tracer.emit(EventKind.ELECTION_STARTED, node="n1", term=1)

    assert event is not None
    assert event.timestamp == clock.now()
    assert event.node == "n1"
    assert event.term == 1


async def test_timestamps_follow_the_clock_rather_than_wall_time(
    tracer: Tracer, clock: VirtualClock
) -> None:
    tracer.emit(EventKind.ELECTION_STARTED, node="n1", term=1)
    await clock.advance(0.25)
    tracer.emit(EventKind.LEADER_ELECTED, node="n1", term=1)

    assert [e.timestamp for e in tracer.events] == [0.0, 0.25]


def test_keyword_arguments_become_details(tracer: Tracer) -> None:
    event = tracer.emit(EventKind.LEADER_ELECTED, node="n1", term=2, votes=3, cluster_size=5)

    assert event is not None
    assert event.details == {"votes": 3, "cluster_size": 5}


def test_sequence_numbers_order_events_sharing_one_instant(tracer: Tracer) -> None:
    """The virtual clock does not move on its own, so a burst of events all carry the
    same timestamp - ``seq`` is the only thing left that can order them."""
    for _ in range(3):
        tracer.emit(EventKind.VOTE_REQUESTED, node="n1", term=1)

    timestamps = {e.timestamp for e in tracer.events}
    assert len(timestamps) == 1
    assert [e.seq for e in tracer.events] == [0, 1, 2]


def test_the_trace_is_a_copy_not_the_tracer_s_own_list(tracer: Tracer) -> None:
    tracer.emit(EventKind.TERM_CHANGED, node="n1", term=1)

    tracer.events.clear()

    assert len(tracer) == 1


# --- being off ----------------------------------------------------------------------


def test_a_disabled_tracer_records_nothing() -> None:
    tracer = Tracer(VirtualClock(), enabled=False)

    assert tracer.emit(EventKind.ELECTION_STARTED, node="n1", term=1) is None
    assert len(tracer) == 0


def test_the_null_tracer_is_disabled_and_needs_no_clock() -> None:
    """It is constructed at import time, before any event loop exists - so it must not
    touch a clock to refuse an event."""
    assert NULL_TRACER.enabled is False
    assert NULL_TRACER.emit(EventKind.NODE_CRASHED, node="n1") is None
    assert len(NULL_TRACER) == 0


def test_a_disabled_tracer_does_not_notify_subscribers() -> None:
    tracer = Tracer(VirtualClock(), enabled=False)
    seen: list[Event] = []
    tracer.subscribe(seen.append)

    tracer.emit(EventKind.LEADER_ELECTED, node="n1", term=1)

    assert seen == []


# --- subscription -------------------------------------------------------------------


def test_a_subscriber_sees_events_as_they_happen(tracer: Tracer) -> None:
    seen: list[Event] = []
    tracer.subscribe(seen.append)

    tracer.emit(EventKind.ELECTION_STARTED, node="n1", term=1)
    tracer.emit(EventKind.LEADER_ELECTED, node="n1", term=1)

    assert [e.kind for e in seen] == [EventKind.ELECTION_STARTED, EventKind.LEADER_ELECTED]


def test_a_subscriber_is_not_replayed_the_backlog(tracer: Tracer) -> None:
    """A live consumer subscribes to what happens next; anything wanting the history
    reads it off the tracer, which still has all of it."""
    tracer.emit(EventKind.ELECTION_STARTED, node="n1", term=1)
    seen: list[Event] = []

    tracer.subscribe(seen.append)
    tracer.emit(EventKind.LEADER_ELECTED, node="n1", term=1)

    assert [e.kind for e in seen] == [EventKind.LEADER_ELECTED]
    assert len(tracer) == 2


def test_every_subscriber_is_notified(tracer: Tracer) -> None:
    first: list[Event] = []
    second: list[Event] = []
    tracer.subscribe(first.append)
    tracer.subscribe(second.append)

    tracer.emit(EventKind.TERM_CHANGED, node="n1", term=2)

    assert len(first) == len(second) == 1


# --- reading back -------------------------------------------------------------------


def populate(tracer: Tracer) -> None:
    tracer.emit(EventKind.ELECTION_STARTED, node="n1", term=1)
    tracer.emit(EventKind.VOTE_GRANTED, node="n2", term=1, candidate="n1")
    tracer.emit(EventKind.LEADER_ELECTED, node="n1", term=1, votes=2)
    tracer.emit(EventKind.ELECTION_STARTED, node="n2", term=2)


def test_filtering_by_kind(tracer: Tracer) -> None:
    populate(tracer)

    assert [e.term for e in tracer.of_kind(EventKind.ELECTION_STARTED)] == [1, 2]
    assert tracer.count(EventKind.ELECTION_STARTED) == 2
    assert tracer.count(EventKind.ELECTION_STARTED, EventKind.LEADER_ELECTED) == 3


def test_filtering_by_node_and_term(tracer: Tracer) -> None:
    populate(tracer)

    assert [e.kind for e in tracer.for_node("n1")] == [
        EventKind.ELECTION_STARTED,
        EventKind.LEADER_ELECTED,
    ]
    assert len(tracer.in_term(1)) == 3


def test_first_and_last_of_a_kind(tracer: Tracer) -> None:
    populate(tracer)

    first = tracer.first(EventKind.ELECTION_STARTED)
    last = tracer.last(EventKind.ELECTION_STARTED)

    assert first is not None and first.node == "n1"
    assert last is not None and last.node == "n2"
    assert tracer.first(EventKind.NODE_CRASHED) is None


def test_a_tracer_iterates_and_counts(tracer: Tracer) -> None:
    populate(tracer)

    assert len(tracer) == 4
    assert [e.kind for e in tracer][0] is EventKind.ELECTION_STARTED


def test_clearing_drops_the_trace_but_keeps_the_ordering(tracer: Tracer) -> None:
    """Sequence numbers keep counting across a clear, so events either side of one can
    still be ordered against each other - a reused counter would make them ambiguous."""
    populate(tracer)

    tracer.clear()
    event = tracer.emit(EventKind.NODE_CRASHED, node="n1")

    assert len(tracer) == 1
    assert event is not None
    assert event.seq == 4


def test_the_story_renders_one_line_per_event(tracer: Tracer) -> None:
    populate(tracer)

    assert len(tracer.story().splitlines()) == 4


# --- persistence --------------------------------------------------------------------


def test_a_tracer_writes_a_readable_trace(tracer: Tracer, tmp_path: Path) -> None:
    populate(tracer)
    path = tmp_path / "run.jsonl"

    tracer.write_jsonl(path)

    assert read_jsonl(path) == tracer.events
