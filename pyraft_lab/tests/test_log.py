"""The replicated log: sentinel indexing, contiguity, conflict repair."""

from __future__ import annotations

import pytest

from pyraft_lab.raft.log import LogEntry, RaftLog


@pytest.fixture
def log() -> RaftLog:
    return RaftLog()


def test_empty_log_has_a_term_zero_sentinel_at_index_zero(log: RaftLog) -> None:
    assert len(log) == 0
    assert log.last_index == 0
    assert log.last_term == 0
    assert log.term_at(0) == 0
    assert log.has_entry(0, 0)


def test_term_at_is_none_past_the_end(log: RaftLog) -> None:
    assert log.term_at(1) is None
    assert log.entry_at(1) is None


def test_append_extends_the_log_contiguously(log: RaftLog) -> None:
    log.append(LogEntry(term=1, index=1, command="a"))
    log.append(LogEntry(term=1, index=2, command="b"))

    assert len(log) == 2
    assert log.last_index == 2
    assert log.last_term == 1
    assert log.entry_at(1) == LogEntry(term=1, index=1, command="a")


def test_append_rejects_a_non_contiguous_index(log: RaftLog) -> None:
    with pytest.raises(ValueError, match="expected next index 1"):
        log.append(LogEntry(term=1, index=2, command="a"))

    log.append(LogEntry(term=1, index=1, command="a"))
    with pytest.raises(ValueError, match="expected next index 2"):
        log.append(LogEntry(term=1, index=3, command="b"))


def test_has_entry_checks_both_index_and_term(log: RaftLog) -> None:
    log.append(LogEntry(term=2, index=1, command="a"))

    assert log.has_entry(1, 2)
    assert not log.has_entry(1, 1)
    assert not log.has_entry(2, 2)


def test_truncate_from_removes_the_entry_and_everything_after(log: RaftLog) -> None:
    for i in range(1, 5):
        log.append(LogEntry(term=1, index=i, command=i))

    log.truncate_from(3)

    assert log.last_index == 2
    assert log.term_at(3) is None


def test_truncate_from_the_sentinel_is_rejected(log: RaftLog) -> None:
    with pytest.raises(ValueError, match="sentinel"):
        log.truncate_from(0)


def test_entries_from_returns_the_suffix_in_order(log: RaftLog) -> None:
    for i in range(1, 4):
        log.append(LogEntry(term=1, index=i, command=i))

    suffix = log.entries_from(2)
    assert [e.index for e in suffix] == [2, 3]


def test_entries_from_rejects_a_non_positive_index(log: RaftLog) -> None:
    with pytest.raises(ValueError, match="index must be >= 1"):
        log.entries_from(0)


# --- append_new_entries: paper §5.3 steps 2-4 ------------------------------------


def test_append_new_entries_extends_when_there_is_no_conflict(log: RaftLog) -> None:
    log.append_new_entries([LogEntry(term=1, index=1, command="a")])
    log.append_new_entries([LogEntry(term=1, index=2, command="b")])

    assert len(log) == 2
    assert [e.command for e in log.entries_from(1)] == ["a", "b"]


def test_append_new_entries_truncates_on_conflict(log: RaftLog) -> None:
    log.append(LogEntry(term=1, index=1, command="a"))
    log.append(LogEntry(term=1, index=2, command="stale"))
    log.append(LogEntry(term=1, index=3, command="also-stale"))

    # A new leader for term 2 disagrees on what's at index 2.
    log.append_new_entries([LogEntry(term=2, index=2, command="fresh")])

    assert log.last_index == 2
    assert log.entry_at(2) == LogEntry(term=2, index=2, command="fresh")


def test_append_new_entries_is_idempotent_on_replay(log: RaftLog) -> None:
    entries = [LogEntry(term=1, index=1, command="a"), LogEntry(term=1, index=2, command="b")]
    log.append_new_entries(entries)
    log.append_new_entries(entries)  # e.g. a retried AppendEntries after a lost reply

    assert len(log) == 2
    assert [e.command for e in log.entries_from(1)] == ["a", "b"]


# --- candidate_log_is_up_to_date: paper §5.4.1 -----------------------------------


def test_up_to_date_check_prefers_the_later_term(log: RaftLog) -> None:
    log.append(LogEntry(term=5, index=1, command="a"))

    assert log.candidate_log_is_up_to_date(candidate_last_term=6, candidate_last_index=1)
    assert not log.candidate_log_is_up_to_date(candidate_last_term=4, candidate_last_index=99)


def test_up_to_date_check_falls_back_to_length_on_a_tied_term(log: RaftLog) -> None:
    log.append(LogEntry(term=1, index=1, command="a"))
    log.append(LogEntry(term=1, index=2, command="b"))

    assert log.candidate_log_is_up_to_date(candidate_last_term=1, candidate_last_index=2)
    assert log.candidate_log_is_up_to_date(candidate_last_term=1, candidate_last_index=3)
    assert not log.candidate_log_is_up_to_date(candidate_last_term=1, candidate_last_index=1)


def test_up_to_date_check_on_two_empty_logs(log: RaftLog) -> None:
    assert log.candidate_log_is_up_to_date(candidate_last_term=0, candidate_last_index=0)
