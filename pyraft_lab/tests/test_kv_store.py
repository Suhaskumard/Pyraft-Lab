"""The state machine: PUT/GET/DELETE/CAS, and the apply-once ordering rules."""

from __future__ import annotations

import pytest

from pyraft_lab.kv.commands import Cas, Command, Delete, Get, Put
from pyraft_lab.kv.store import KVStore
from pyraft_lab.raft.log import LogEntry, RaftLog


def log_of(*commands: Command | None) -> RaftLog:
    """A log holding these commands at indexes 1..n, all in term 1."""
    log = RaftLog()
    for i, command in enumerate(commands, start=1):
        wire = command.to_wire() if command is not None else None
        log.append(LogEntry(term=1, index=i, command=wire))
    return log


# --- the commands -----------------------------------------------------------------


def test_put_then_read() -> None:
    store = KVStore()
    log = log_of(Put(key="a", value=1))

    results = store.apply_committed(log, commit_index=1)

    assert results[0].ok is True
    assert store.read("a") == 1


def test_put_overwrites() -> None:
    store = KVStore()
    store.apply_committed(log_of(Put(key="a", value=1), Put(key="a", value=2)), 2)

    assert store.read("a") == 2
    assert len(store) == 1


def test_get_reports_whether_the_key_exists() -> None:
    store = KVStore()
    log = log_of(Put(key="a", value=1), Get(key="a"), Get(key="missing"))

    results = store.apply_committed(log, commit_index=3)

    assert (results[1].ok, results[1].value) == (True, 1)
    assert (results[2].ok, results[2].value) == (False, None)


def test_delete_removes_the_key_and_returns_the_old_value() -> None:
    store = KVStore()
    log = log_of(Put(key="a", value=1), Delete(key="a"))

    results = store.apply_committed(log, commit_index=2)

    assert (results[1].ok, results[1].value) == (True, 1)
    assert store.read("a") is None
    assert "a" not in store


def test_deleting_an_absent_key_applies_but_reports_not_ok() -> None:
    """A no-op delete is a successful application with a false outcome, not an error."""
    store = KVStore()

    results = store.apply_committed(log_of(Delete(key="ghost")), 1)

    assert results[0].ok is False
    assert store.last_applied == 1  # it still counts as applied


def test_cas_swaps_when_the_expected_value_matches() -> None:
    store = KVStore()
    log = log_of(Put(key="a", value=1), Cas(key="a", expected=1, new=2))

    results = store.apply_committed(log, commit_index=2)

    assert results[1].ok is True
    assert store.read("a") == 2


def test_cas_refuses_and_reports_the_value_that_beat_it() -> None:
    store = KVStore()
    log = log_of(Put(key="a", value=99), Cas(key="a", expected=1, new=2))

    results = store.apply_committed(log, commit_index=2)

    assert results[1].ok is False
    assert results[1].value == 99  # what a retry loop needs to see
    assert store.read("a") == 99


def test_cas_against_none_is_an_atomic_create() -> None:
    store = KVStore()
    log = log_of(
        Cas(key="a", expected=None, new="first"),
        Cas(key="a", expected=None, new="second"),
    )

    results = store.apply_committed(log, commit_index=2)

    assert results[0].ok is True
    assert results[1].ok is False  # the key exists now, so the second create loses
    assert store.read("a") == "first"


def test_a_none_command_applies_harmlessly() -> None:
    """The log format permits a no-op entry; applying one must not break anything."""
    store = KVStore()

    results = store.apply_committed(log_of(None), 1)

    assert results[0].ok is True
    assert store.last_applied == 1


# --- the ordering rules -----------------------------------------------------------


def test_only_committed_entries_are_applied() -> None:
    """Entries past commit_index can still be overwritten, so they must not be applied."""
    store = KVStore()
    log = log_of(Put(key="a", value=1), Put(key="b", value=2), Put(key="c", value=3))

    store.apply_committed(log, commit_index=1)

    assert store.read("a") == 1
    assert store.read("b") is None
    assert store.last_applied == 1


def test_applying_resumes_where_it_left_off() -> None:
    store = KVStore()
    log = log_of(Put(key="a", value=1), Put(key="b", value=2), Put(key="c", value=3))

    first = store.apply_committed(log, commit_index=1)
    second = store.apply_committed(log, commit_index=3)

    assert len(first) == 1
    assert [r.index for r in second] == [2, 3]
    assert store.last_applied == 3


def test_re_applying_the_same_commit_index_is_a_no_op() -> None:
    """Exactly-once is what makes non-idempotent commands like CAS safe."""
    store = KVStore()
    log = log_of(Put(key="n", value=0), Cas(key="n", expected=0, new=1))

    store.apply_committed(log, commit_index=2)
    again = store.apply_committed(log, commit_index=2)

    assert again == []
    assert store.read("n") == 1  # not 2, and the CAS did not re-run


def test_commit_index_cannot_move_backward_through_apply() -> None:
    store = KVStore()
    log = log_of(Put(key="a", value=1), Put(key="b", value=2))
    store.apply_committed(log, commit_index=2)

    assert store.apply_committed(log, commit_index=1) == []
    assert store.last_applied == 2


def test_applying_past_the_end_of_the_log_is_a_bug() -> None:
    store = KVStore()

    with pytest.raises(ValueError, match="past the end"):
        store.apply_committed(log_of(Put(key="a", value=1)), commit_index=5)


def test_entries_apply_in_index_order() -> None:
    """Order is the whole point: the same log must produce the same final state."""
    store = KVStore()
    log = log_of(
        Put(key="k", value="first"),
        Put(key="k", value="second"),
        Put(key="k", value="third"),
    )

    results = store.apply_committed(log, commit_index=3)

    assert [r.index for r in results] == [1, 2, 3]
    assert store.read("k") == "third"


def test_two_replicas_of_the_same_log_reach_identical_state() -> None:
    """Determinism: the property every consistency check in Phase 5 rests on."""
    log = log_of(
        Put(key="a", value=1),
        Put(key="b", value=2),
        Cas(key="a", expected=1, new=10),
        Delete(key="b"),
        Cas(key="b", expected=None, new="recreated"),
    )

    one, two = KVStore(), KVStore()
    one.apply_committed(log, commit_index=5)
    # The second replica learns about commits in dribs and drabs, as a real one would.
    for index in range(1, 6):
        two.apply_committed(log, commit_index=index)

    assert one.snapshot() == two.snapshot()
    assert one.last_applied == two.last_applied == 5


# --- snapshot / restore -----------------------------------------------------------


def test_snapshot_is_a_copy_not_a_view() -> None:
    store = KVStore()
    store.apply_committed(log_of(Put(key="a", value=1)), 1)

    snapshot = store.snapshot()
    snapshot["a"] = "tampered"

    assert store.read("a") == 1


def test_restore_replaces_state_and_last_applied() -> None:
    store = KVStore()
    store.apply_committed(log_of(Put(key="old", value=1)), 1)

    store.restore({"new": 2}, last_applied=42)

    assert store.read("old") is None
    assert store.read("new") == 2
    assert store.last_applied == 42
