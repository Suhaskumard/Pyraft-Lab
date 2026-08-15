"""RaftState: term transitions and the step-down-on-higher-term rule."""

from __future__ import annotations

from pyraft_lab.raft.state import RaftState, Role


def test_a_new_node_starts_as_a_follower_at_term_zero() -> None:
    state = RaftState(node_id="n1")

    assert state.role is Role.FOLLOWER
    assert state.current_term == 0
    assert state.voted_for is None


def test_observe_term_ignores_an_equal_or_lower_term() -> None:
    state = RaftState(node_id="n1", current_term=5, voted_for="n2")

    assert state.observe_term(5) is False
    assert state.observe_term(4) is False
    assert state.current_term == 5
    assert state.voted_for == "n2"


def test_observe_term_steps_a_leader_down_to_follower() -> None:
    node_id = "n1"
    state = RaftState(
        node_id=node_id,
        current_term=3,
        voted_for=node_id,
        role=Role.LEADER,
        next_index={"n2": 5},
        match_index={"n2": 4},
    )

    assert state.observe_term(4) is True
    assert state.current_term == 4
    assert state.role is Role.FOLLOWER
    assert state.voted_for is None
    assert state.next_index == {}
    assert state.match_index == {}


def test_observe_term_clears_the_vote_for_the_new_term() -> None:
    state = RaftState(node_id="n1", current_term=1, voted_for="n2")

    state.observe_term(2)

    assert state.voted_for is None
