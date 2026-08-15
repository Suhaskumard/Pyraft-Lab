"""Leader-side replication: batching, nextIndex backoff, majority commit."""

from __future__ import annotations

import pytest

from pyraft_lab.raft.log import LogEntry
from pyraft_lab.raft.replication import (
    NotLeader,
    advance_commit_index,
    append_command,
    build_append_entries,
    handle_append_reply,
)
from pyraft_lab.raft.rpc import AppendEntriesReply
from pyraft_lab.raft.state import RaftState, Role


def leader(term: int = 1, peers: tuple[str, ...] = ("n2", "n3")) -> RaftState:
    state = RaftState(node_id="n1", current_term=term, role=Role.LEADER)
    state.next_index = dict.fromkeys(peers, 1)
    state.match_index = dict.fromkeys(peers, 0)
    return state


# --- append_command ---------------------------------------------------------------


def test_append_command_writes_at_the_next_index_in_the_current_term() -> None:
    state = leader(term=3)

    entry = append_command(state, {"op": "put", "key": "a"})

    assert (entry.index, entry.term) == (1, 3)
    assert state.log.last_index == 1
    assert state.commit_index == 0  # not committed merely by being appended


def test_append_command_is_rejected_on_a_follower() -> None:
    state = RaftState(node_id="n1", role=Role.FOLLOWER)

    with pytest.raises(NotLeader):
        append_command(state, "x")


# --- build_append_entries ---------------------------------------------------------


def test_a_caught_up_follower_gets_an_empty_heartbeat() -> None:
    state = leader()
    state.log.append(LogEntry(term=1, index=1, command="a"))
    state.next_index["n2"] = 2

    request = build_append_entries(state, "n2")

    assert request.entries == []
    assert (request.prev_log_index, request.prev_log_term) == (1, 1)


def test_a_lagging_follower_gets_everything_it_is_missing() -> None:
    state = leader()
    for i in range(1, 4):
        state.log.append(LogEntry(term=1, index=i, command=i))
    state.next_index["n2"] = 1

    request = build_append_entries(state, "n2")

    assert [e.index for e in request.entries] == [1, 2, 3]
    assert (request.prev_log_index, request.prev_log_term) == (0, 0)


def test_batches_are_capped() -> None:
    state = leader()
    for i in range(1, 11):
        state.log.append(LogEntry(term=1, index=i, command=i))
    state.next_index["n2"] = 1

    request = build_append_entries(state, "n2", max_batch=4)

    assert [e.index for e in request.entries] == [1, 2, 3, 4]


def test_the_request_carries_the_leaders_commit_index() -> None:
    state = leader()
    state.log.append(LogEntry(term=1, index=1, command="a"))
    state.commit_index = 1

    assert build_append_entries(state, "n2").leader_commit == 1


# --- handle_append_reply ----------------------------------------------------------


def test_success_advances_match_and_next_index() -> None:
    state = leader()
    for i in range(1, 4):
        state.log.append(LogEntry(term=1, index=i, command=i))

    handle_append_reply(state, "n2", AppendEntriesReply(term=1, success=True), last_index_sent=3)

    assert state.match_index["n2"] == 3
    assert state.next_index["n2"] == 4


def test_failure_walks_next_index_backward() -> None:
    state = leader()
    state.next_index["n2"] = 5

    handle_append_reply(state, "n2", AppendEntriesReply(term=1, success=False), last_index_sent=0)

    assert state.next_index["n2"] == 4
    assert state.match_index["n2"] == 0  # nothing is known to match yet


def test_next_index_never_walks_below_one() -> None:
    """Index 0 is the sentinel; a follower can always be brought back from index 1."""
    state = leader()
    state.next_index["n2"] = 1

    for _ in range(5):
        handle_append_reply(
            state, "n2", AppendEntriesReply(term=1, success=False), last_index_sent=0
        )

    assert state.next_index["n2"] == 1


def test_a_stale_success_cannot_drag_match_index_backward() -> None:
    """A delayed reply to an older, shorter request must not undo later progress."""
    state = leader()
    for i in range(1, 6):
        state.log.append(LogEntry(term=1, index=i, command=i))

    handle_append_reply(state, "n2", AppendEntriesReply(term=1, success=True), last_index_sent=5)
    handle_append_reply(state, "n2", AppendEntriesReply(term=1, success=True), last_index_sent=2)

    assert state.match_index["n2"] == 5


def test_a_reply_from_another_term_is_ignored() -> None:
    state = leader(term=5)
    state.next_index["n2"] = 3

    handle_append_reply(state, "n2", AppendEntriesReply(term=4, success=False), last_index_sent=0)

    assert state.next_index["n2"] == 3


def test_a_reply_to_a_node_that_is_no_longer_leader_is_ignored() -> None:
    state = leader()
    state.role = Role.FOLLOWER

    handle_append_reply(state, "n2", AppendEntriesReply(term=1, success=True), last_index_sent=3)

    assert state.match_index["n2"] == 0


# --- advance_commit_index ---------------------------------------------------------


def test_an_entry_commits_once_a_majority_stores_it() -> None:
    state = leader()
    state.log.append(LogEntry(term=1, index=1, command="a"))
    state.match_index["n2"] = 1  # leader + n2 = 2 of 3 = majority

    assert advance_commit_index(state, cluster_size=3) is True
    assert state.commit_index == 1


def test_an_entry_on_a_minority_does_not_commit() -> None:
    state = leader(peers=("n2", "n3", "n4", "n5"))
    state.log.append(LogEntry(term=1, index=1, command="a"))
    state.match_index["n2"] = 1  # leader + n2 = 2 of 5 - not a majority

    assert advance_commit_index(state, cluster_size=5) is False
    assert state.commit_index == 0


def test_commit_stops_at_the_index_the_majority_actually_reached() -> None:
    state = leader(peers=("n2", "n3", "n4", "n5"))
    for i in range(1, 6):
        state.log.append(LogEntry(term=1, index=i, command=i))
    state.match_index.update({"n2": 5, "n3": 3, "n4": 3, "n5": 1})

    advance_commit_index(state, cluster_size=5)

    # leader(5), n2(5), n3(3), n4(3) -> three of five have reached at least 3.
    assert state.commit_index == 3


def test_an_entry_from_an_earlier_term_is_not_committed_by_vote_count() -> None:
    """Paper §5.4.2 / Figure 8: committing an old-term entry on replication alone is
    unsafe - it can still be overwritten by a future leader."""
    state = leader(term=3)
    state.log.append(LogEntry(term=1, index=1, command="old"))
    state.match_index["n2"] = 1

    assert advance_commit_index(state, cluster_size=3) is False
    assert state.commit_index == 0


def test_a_current_term_commit_carries_earlier_entries_with_it() -> None:
    """The indirect path old entries commit by: once a current-term entry is safe,
    everything before it is too."""
    state = leader(term=3)
    state.log.append(LogEntry(term=1, index=1, command="old"))
    state.log.append(LogEntry(term=3, index=2, command="new"))
    state.match_index["n2"] = 2

    assert advance_commit_index(state, cluster_size=3) is True
    assert state.commit_index == 2  # index 1 is now committed as well


def test_commit_index_does_not_move_without_new_replication() -> None:
    state = leader()
    state.log.append(LogEntry(term=1, index=1, command="a"))
    state.match_index["n2"] = 1
    advance_commit_index(state, cluster_size=3)

    assert advance_commit_index(state, cluster_size=3) is False
    assert state.commit_index == 1


def test_a_single_node_cluster_commits_on_its_own() -> None:
    state = RaftState(node_id="solo", current_term=1, role=Role.LEADER)
    state.log.append(LogEntry(term=1, index=1, command="a"))

    assert advance_commit_index(state, cluster_size=1) is True
    assert state.commit_index == 1


def test_a_follower_never_advances_commit_index_this_way() -> None:
    state = leader()
    state.role = Role.FOLLOWER
    state.log.append(LogEntry(term=1, index=1, command="a"))
    state.match_index["n2"] = 1

    assert advance_commit_index(state, cluster_size=3) is False
