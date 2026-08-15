"""Day 13: the linearizability checker, on histories built by hand.

Hand-built rather than recorded from a cluster, and deliberately: a checker validated
only against runs of the system it is checking can only ever confirm that the two agree.
These histories include ones a correct Raft cluster should never produce, which is the
only way to know the checker would catch them.
"""

from __future__ import annotations

import pytest

from pyraft_lab.consistency.history import History, OpKind
from pyraft_lab.consistency.linearizability import check
from pyraft_lab.observability.events import Event, EventKind


@pytest.fixture
def history() -> History:
    return History()


def put(history: History, key: str, value: object, start: float, end: float | None) -> None:
    op = history.invoke("c1", OpKind.PUT, key, start, value=value)
    if end is not None:
        history.complete(op, end, result=value, ok=True, target="n1")


def get(
    history: History,
    key: str,
    result: object,
    start: float,
    end: float,
    *,
    found: bool = True,
    target: str = "n1",
) -> None:
    op = history.invoke("c1", OpKind.GET, key, start)
    history.complete(op, end, result=result, ok=found, target=target)


def delete(history: History, key: str, existed: bool, start: float, end: float) -> None:
    op = history.invoke("c1", OpKind.DELETE, key, start)
    history.complete(op, end, result=existed, ok=existed, target="n1")


def cas(
    history: History,
    key: str,
    expected: object,
    new: object,
    won: bool,
    start: float,
    end: float,
) -> None:
    op = history.invoke("c1", OpKind.CAS, key, start, value=new, expected=expected)
    history.complete(op, end, result=won, ok=won, target="n1")


# --- histories that are linearizable --------------------------------------------------


def test_an_empty_history_is_linearizable(history: History) -> None:
    result = check(history)

    assert result.linearizable
    assert result.verdict == "PASS"
    assert result.keys_checked == 0


def test_a_write_then_a_read_of_it(history: History) -> None:
    put(history, "x", 10, 0.0, 1.0)
    get(history, "x", 10, 2.0, 3.0)

    assert check(history).linearizable


def test_reading_a_key_nobody_wrote(history: History) -> None:
    get(history, "x", None, 0.0, 1.0, found=False)

    assert check(history).linearizable


def test_concurrent_writes_may_be_ordered_either_way(history: History) -> None:
    """Both writes finish before the read begins, so both must precede it - but their
    order relative to each other is free, and only one of the two orders explains the
    value the read saw. Finding that order is the checker's whole job."""
    put(history, "x", 1, 0.0, 10.0)
    put(history, "x", 2, 1.0, 11.0)
    get(history, "x", 1, 12.0, 13.0)

    assert check(history).linearizable


def test_a_read_concurrent_with_the_write_it_missed(history: History) -> None:
    """The read overlaps the write, so it may be linearized before it. Returning the
    old value is correct, not stale."""
    put(history, "x", 1, 0.0, 1.0)
    op = history.invoke("c2", OpKind.PUT, "x", 2.0, value=2)
    get(history, "x", 1, 2.5, 3.0)
    history.complete(op, 4.0, result=2, ok=True, target="n1")

    assert check(history).linearizable


def test_delete_then_absent(history: History) -> None:
    put(history, "x", 1, 0.0, 1.0)
    delete(history, "x", True, 2.0, 3.0)
    get(history, "x", None, 4.0, 5.0, found=False)

    assert check(history).linearizable


def test_a_successful_cas_and_the_value_it_left(history: History) -> None:
    put(history, "x", 1, 0.0, 1.0)
    cas(history, "x", 1, 2, True, 2.0, 3.0)
    get(history, "x", 2, 4.0, 5.0)

    assert check(history).linearizable


def test_a_losing_cas_changes_nothing(history: History) -> None:
    put(history, "x", 1, 0.0, 1.0)
    cas(history, "x", 99, 2, False, 2.0, 3.0)
    get(history, "x", 1, 4.0, 5.0)

    assert check(history).linearizable


def test_a_null_value_is_not_the_same_as_an_absent_key(history: History) -> None:
    """A store that lost the difference would pass a history where a delete vanished."""
    put(history, "x", None, 0.0, 1.0)
    get(history, "x", None, 2.0, 3.0, found=True)

    assert check(history).linearizable


def test_keys_are_checked_independently(history: History) -> None:
    put(history, "x", 1, 0.0, 1.0)
    get(history, "x", 1, 2.0, 3.0)
    put(history, "y", 5, 0.5, 1.5)
    get(history, "y", 5, 2.5, 3.5)

    result = check(history)

    assert result.linearizable
    assert result.keys_checked == 2
    assert result.operations_checked == 4


# --- an operation that never came back --------------------------------------------------


def test_an_incomplete_write_may_have_happened(history: History) -> None:
    put(history, "x", 1, 0.0, None)
    get(history, "x", 1, 2.0, 3.0)

    assert check(history).linearizable


def test_an_incomplete_write_may_equally_not_have(history: History) -> None:
    """The same history, with the read seeing nothing. A client that timed out does not
    know whether its write took effect, and neither does the checker get to assume."""
    put(history, "x", 1, 0.0, None)
    get(history, "x", None, 2.0, 3.0, found=False)

    assert check(history).linearizable


def test_an_incomplete_write_cannot_excuse_an_impossible_value(history: History) -> None:
    put(history, "x", 1, 0.0, None)
    get(history, "x", 99, 2.0, 3.0)

    assert not check(history).linearizable


# --- histories that are not --------------------------------------------------------------


def test_a_value_that_was_never_written(history: History) -> None:
    put(history, "x", 10, 0.0, 1.0)
    get(history, "x", 5, 2.0, 3.0)

    result = check(history)

    assert not result.linearizable
    assert result.verdict == "FAIL"
    violation = result.violations[0]
    assert violation.key == "x"
    assert violation.certain
    assert "no write could have placed" in violation.cause


def test_a_stale_read_of_an_overwritten_value(history: History) -> None:
    put(history, "x", 1, 0.0, 1.0)
    put(history, "x", 2, 2.0, 3.0)
    get(history, "x", 1, 4.0, 5.0)

    result = check(history)

    assert not result.linearizable
    violation = result.violations[0]
    assert "stale read" in violation.cause
    assert [op.kind for op in violation.operations] == [OpKind.PUT, OpKind.GET]


def test_a_key_that_goes_missing_after_a_write(history: History) -> None:
    put(history, "x", 1, 0.0, 1.0)
    get(history, "x", None, 2.0, 3.0, found=False)

    result = check(history)

    assert not result.linearizable
    assert "missing after a write" in result.violations[0].cause


def test_a_delete_that_claims_to_have_found_nothing_there(history: History) -> None:
    put(history, "x", 1, 0.0, 1.0)
    delete(history, "x", False, 2.0, 3.0)

    assert not check(history).linearizable


def test_a_cas_that_claims_to_have_won_against_the_wrong_value(history: History) -> None:
    put(history, "x", 1, 0.0, 1.0)
    cas(history, "x", 99, 2, True, 2.0, 3.0)

    assert not check(history).linearizable


def test_a_resurrected_key(history: History) -> None:
    put(history, "x", 1, 0.0, 1.0)
    delete(history, "x", True, 2.0, 3.0)
    get(history, "x", 1, 4.0, 5.0)

    assert not check(history).linearizable


def test_only_the_offending_key_is_reported(history: History) -> None:
    put(history, "x", 1, 0.0, 1.0)
    get(history, "x", 1, 2.0, 3.0)
    put(history, "y", 1, 0.0, 1.0)
    get(history, "y", 7, 2.0, 3.0)

    result = check(history)

    assert [v.key for v in result.violations] == ["y"]


# --- the report ---------------------------------------------------------------------------


def test_a_failure_reports_the_operations_and_where(history: History) -> None:
    put(history, "x", 1, 0.0, 1.0)
    put(history, "x", 2, 2.0, 3.0)
    get(history, "x", 1, 4.0, 5.0, target="n4")

    report = check(history).explain()

    assert "Linearizability: FAIL" in report
    assert "PUT(x, 2) -> 2" in report
    assert "GET(x) -> 1" in report
    assert "Node: n4" in report
    assert "Time: 5.000s" in report
    assert "Possible cause:" in report


def test_a_pass_reports_only_the_verdict(history: History) -> None:
    put(history, "x", 1, 0.0, 1.0)

    assert check(history).explain() == "Linearizability: PASS"


def test_the_trace_sharpens_the_cause(history: History) -> None:
    """The verdict never depends on the trace; the explanation is allowed to."""
    put(history, "x", 1, 0.0, 1.0)
    put(history, "x", 2, 2.0, 3.0)
    get(history, "x", 1, 4.0, 5.0, target="n4")

    events = [
        Event(kind=EventKind.LEADER_ELECTED, timestamp=0.0, seq=0, node="n1", term=12),
        Event(kind=EventKind.PARTITION_CREATED, timestamp=3.5, seq=1, details={"groups": []}),
    ]
    result = check(history, events=events)

    violation = result.violations[0]
    assert violation.term == 12
    assert "partitioned" in violation.cause
    assert "n4" in violation.cause


def test_a_result_serializes(history: History) -> None:
    import json

    put(history, "x", 1, 0.0, 1.0)
    get(history, "x", 9, 2.0, 3.0)

    payload = check(history).to_dict()

    assert json.loads(json.dumps(payload)) == payload
    assert payload["verdict"] == "FAIL"
    assert payload["violations"][0]["key"] == "x"


# --- the budget is a third answer, not a quiet pass -----------------------------------------


def test_running_out_of_budget_is_reported_as_unproven(history: History) -> None:
    """A checker that answered PASS when it simply gave up would be worse than useless:
    every hard history - which is to say every interesting one - would pass."""
    for i in range(12):
        op = history.invoke("c1", OpKind.PUT, "x", 0.0, value=i)
        history.complete(op, 100.0, result=i, ok=True, target="n1")
    get(history, "x", 999, 200.0, 201.0)

    result = check(history, max_steps=50)

    assert result.verdict == "UNPROVEN"
    assert not result.conclusive
    assert result.exhausted_keys == ["x"]
    assert result.violations == []
    assert "not proven either way" in result.explain()


def test_a_conclusive_result_says_so(history: History) -> None:
    put(history, "x", 1, 0.0, 1.0)

    result = check(history)

    assert result.conclusive
    assert result.states_explored > 0
