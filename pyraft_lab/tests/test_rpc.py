"""RequestVote and AppendEntries receiver logic, per the paper's Figure 2.

Handlers are pure functions of (state, message) -> reply, so every rule is tested
directly without any asyncio, timers, or a second node.
"""

from __future__ import annotations

from pyraft_lab.raft.log import LogEntry
from pyraft_lab.raft.rpc import (
    AppendEntries,
    RequestVote,
    handle_append_entries,
    handle_request_vote,
)
from pyraft_lab.raft.state import RaftState, Role

# --- RequestVote ------------------------------------------------------------------


def test_grants_a_vote_to_an_up_to_date_candidate_when_unvoted() -> None:
    state = RaftState(node_id="n1", current_term=1)

    reply = handle_request_vote(
        state, RequestVote(term=1, candidate_id="n2", last_log_index=0, last_log_term=0)
    )

    assert reply.vote_granted is True
    assert reply.term == 1
    assert state.voted_for == "n2"


def test_rejects_a_stale_term_request_vote() -> None:
    state = RaftState(node_id="n1", current_term=5)

    reply = handle_request_vote(
        state, RequestVote(term=3, candidate_id="n2", last_log_index=0, last_log_term=0)
    )

    assert reply.vote_granted is False
    assert reply.term == 5  # unchanged - the stale term never gets applied
    assert state.voted_for is None


def test_refuses_a_second_vote_in_the_same_term() -> None:
    state = RaftState(node_id="n1", current_term=1, voted_for="n2")

    reply = handle_request_vote(
        state, RequestVote(term=1, candidate_id="n3", last_log_index=0, last_log_term=0)
    )

    assert reply.vote_granted is False
    assert state.voted_for == "n2"


def test_re_grants_the_same_vote_to_a_retried_request() -> None:
    """A candidate may resend RequestVote after a lost reply; granting again must be safe."""
    state = RaftState(node_id="n1", current_term=1, voted_for="n2")

    reply = handle_request_vote(
        state, RequestVote(term=1, candidate_id="n2", last_log_index=0, last_log_term=0)
    )

    assert reply.vote_granted is True


def test_rejects_a_candidate_whose_log_has_an_older_last_term() -> None:
    state = RaftState(node_id="n1", current_term=2)
    state.log.append(LogEntry(term=2, index=1, command="a"))

    reply = handle_request_vote(
        state, RequestVote(term=2, candidate_id="n2", last_log_index=5, last_log_term=1)
    )

    assert reply.vote_granted is False
    assert state.voted_for is None


def test_rejects_a_candidate_with_a_shorter_log_at_the_same_term() -> None:
    state = RaftState(node_id="n1", current_term=1)
    state.log.append(LogEntry(term=1, index=1, command="a"))
    state.log.append(LogEntry(term=1, index=2, command="b"))

    reply = handle_request_vote(
        state, RequestVote(term=1, candidate_id="n2", last_log_index=1, last_log_term=1)
    )

    assert reply.vote_granted is False


def test_a_higher_term_request_vote_bumps_current_term_and_may_still_be_rejected() -> None:
    """Term always advances, even when the vote itself is refused on log grounds."""
    state = RaftState(node_id="n1", current_term=1)
    state.log.append(LogEntry(term=1, index=1, command="a"))
    state.log.append(LogEntry(term=1, index=2, command="b"))

    reply = handle_request_vote(
        state, RequestVote(term=5, candidate_id="n2", last_log_index=0, last_log_term=0)
    )

    assert state.current_term == 5
    assert reply.term == 5
    assert reply.vote_granted is False


def test_a_higher_term_request_vote_steps_a_leader_down() -> None:
    state = RaftState(node_id="n1", current_term=1, role=Role.LEADER, voted_for="n1")

    handle_request_vote(
        state, RequestVote(term=2, candidate_id="n2", last_log_index=0, last_log_term=0)
    )

    assert state.role is Role.FOLLOWER


# --- AppendEntries ------------------------------------------------------------------


def test_an_empty_heartbeat_succeeds_against_an_empty_log() -> None:
    state = RaftState(node_id="n1", current_term=1)

    reply = handle_append_entries(
        state,
        AppendEntries(
            term=1,
            leader_id="leader",
            prev_log_index=0,
            prev_log_term=0,
            entries=[],
            leader_commit=0,
        ),
    )

    assert reply.success is True
    assert reply.term == 1


def test_rejects_a_stale_term() -> None:
    state = RaftState(node_id="n1", current_term=5)

    reply = handle_append_entries(
        state,
        AppendEntries(
            term=3,
            leader_id="leader",
            prev_log_index=0,
            prev_log_term=0,
            entries=[],
            leader_commit=0,
        ),
    )

    assert reply.success is False
    assert reply.term == 5


def test_rejects_when_prev_log_index_is_missing() -> None:
    state = RaftState(node_id="n1", current_term=1)

    reply = handle_append_entries(
        state,
        AppendEntries(
            term=1,
            leader_id="leader",
            prev_log_index=5,
            prev_log_term=1,
            entries=[],
            leader_commit=0,
        ),
    )

    assert reply.success is False


def test_rejects_when_prev_log_term_mismatches() -> None:
    state = RaftState(node_id="n1", current_term=2)
    state.log.append(LogEntry(term=1, index=1, command="a"))

    reply = handle_append_entries(
        state,
        AppendEntries(
            term=2,
            leader_id="leader",
            prev_log_index=1,
            prev_log_term=2,
            entries=[],
            leader_commit=0,
        ),
    )

    assert reply.success is False
    assert state.log.last_index == 1  # untouched: rejection happens before any mutation


def test_appends_new_entries_when_prev_log_matches() -> None:
    state = RaftState(node_id="n1", current_term=1)

    reply = handle_append_entries(
        state,
        AppendEntries(
            term=1,
            leader_id="leader",
            prev_log_index=0,
            prev_log_term=0,
            entries=[
                LogEntry(term=1, index=1, command="a"),
                LogEntry(term=1, index=2, command="b"),
            ],
            leader_commit=0,
        ),
    )

    assert reply.success is True
    assert state.log.last_index == 2


def test_truncates_conflicting_entries_and_appends_the_leaders_version() -> None:
    state = RaftState(node_id="n1", current_term=2)
    state.log.append(LogEntry(term=1, index=1, command="a"))
    state.log.append(LogEntry(term=1, index=2, command="stale"))

    reply = handle_append_entries(
        state,
        AppendEntries(
            term=2,
            leader_id="leader",
            prev_log_index=1,
            prev_log_term=1,
            entries=[LogEntry(term=2, index=2, command="fresh")],
            leader_commit=0,
        ),
    )

    assert reply.success is True
    assert state.log.entry_at(2) == LogEntry(term=2, index=2, command="fresh")


def test_commit_index_advances_to_the_min_of_leader_commit_and_last_new_entry() -> None:
    state = RaftState(node_id="n1", current_term=1)

    handle_append_entries(
        state,
        AppendEntries(
            term=1,
            leader_id="leader",
            prev_log_index=0,
            prev_log_term=0,
            entries=[LogEntry(term=1, index=1, command="a")],
            leader_commit=99,  # leader is far ahead; we only have 1 entry
        ),
    )

    assert state.commit_index == 1


def test_commit_index_never_moves_backward() -> None:
    state = RaftState(node_id="n1", current_term=1, commit_index=3)
    for i in range(1, 5):
        state.log.append(LogEntry(term=1, index=i, command=i))

    handle_append_entries(
        state,
        AppendEntries(
            term=1,
            leader_id="leader",
            prev_log_index=4,
            prev_log_term=1,
            entries=[],
            leader_commit=1,
        ),
    )

    assert state.commit_index == 3


def test_a_valid_append_entries_returns_a_candidate_to_follower() -> None:
    state = RaftState(node_id="n1", current_term=1, role=Role.CANDIDATE, voted_for="n1")

    handle_append_entries(
        state,
        AppendEntries(
            term=1,
            leader_id="leader",
            prev_log_index=0,
            prev_log_term=0,
            entries=[],
            leader_commit=0,
        ),
    )

    assert state.role is Role.FOLLOWER


def test_a_higher_term_append_entries_bumps_the_term_and_clears_the_vote() -> None:
    state = RaftState(node_id="n1", current_term=1, voted_for="n1", role=Role.LEADER)

    reply = handle_append_entries(
        state,
        AppendEntries(
            term=4,
            leader_id="leader",
            prev_log_index=0,
            prev_log_term=0,
            entries=[],
            leader_commit=0,
        ),
    )

    assert reply.success is True
    assert state.current_term == 4
    assert state.voted_for is None
    assert state.role is Role.FOLLOWER


def test_append_entries_is_idempotent_under_retry() -> None:
    state = RaftState(node_id="n1", current_term=1)
    entries = [LogEntry(term=1, index=1, command="a")]
    msg = AppendEntries(
        term=1,
        leader_id="leader",
        prev_log_index=0,
        prev_log_term=0,
        entries=entries,
        leader_commit=1,
    )

    handle_append_entries(state, msg)
    reply = handle_append_entries(state, msg)  # simulate a retried RPC

    assert reply.success is True
    assert state.log.last_index == 1
