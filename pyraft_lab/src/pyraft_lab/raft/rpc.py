"""The two Raft RPCs: wire types and their receiver-side handlers.

Each handler implements exactly one row of the paper's Figure 2 ("Rules for
Servers" + the per-RPC "Receiver implementation" boxes) and nothing more: no timers,
no broadcasting, no vote tallying. Those are Day 3 (election) and Day 4 (replication)
concerns that drive these handlers from the outside. A handler's only job is "given my
current state and this request, what do I become and what do I reply?" - which makes
it deterministic and trivial to unit test without asyncio.
"""

from __future__ import annotations

from typing import ClassVar

from pydantic import Field

from pyraft_lab import NodeId
from pyraft_lab.network.messages import Message
from pyraft_lab.raft.log import LogEntry
from pyraft_lab.raft.state import RaftState, Role


class RequestVote(Message):
    """Sent by a candidate to solicit a vote (paper Figure 2)."""

    KIND: ClassVar[str] = "request_vote"

    term: int
    candidate_id: NodeId
    last_log_index: int
    last_log_term: int


class RequestVoteReply(Message):
    KIND: ClassVar[str] = "request_vote_reply"

    term: int
    vote_granted: bool


class AppendEntries(Message):
    """Sent by the leader to replicate entries and, with an empty ``entries``, as a
    heartbeat (paper Figure 2).
    """

    KIND: ClassVar[str] = "append_entries"

    term: int
    leader_id: NodeId
    prev_log_index: int
    prev_log_term: int
    entries: list[LogEntry] = Field(default_factory=list)
    leader_commit: int
    sent_at: float = 0.0
    """The leader's clock reading when this request went out, echoed back in the reply.

    Beyond Figure 2, and the companion to ``AppendEntriesReply.match_index``: it lets a
    leader date an acknowledgement by *the request it answers* rather than by when the
    reply happened to land. The read lease needs that - see ``RaftNode._has_lease``.
    """


class AppendEntriesReply(Message):
    KIND: ClassVar[str] = "append_entries_reply"

    term: int
    success: bool
    match_index: int = 0
    """The highest index this reply proves the follower's log matches the leader's.

    Beyond Figure 2, which carries only ``term`` and ``success``. That minimal reply
    cannot say *which* request it answers, so a leader with more than one outstanding
    to a peer has to guess - and guessing "the last one I sent" credits a slow reply
    with a later request's entries, advancing ``matchIndex`` past what the follower
    actually stored (docs/architecture.md P8's fifth bug). Carrying the index makes
    attribution exact and reordering harmless, which is what real implementations do
    (etcd's ``MsgAppResp`` carries ``Index``). Meaningful only when ``success``.
    """
    request_sent_at: float = 0.0
    """``AppendEntries.sent_at`` echoed straight back, success or not.

    Echoed rather than re-read from a clock so it means the same thing on both sides:
    the leader's own reading when it sent the request this reply answers.
    """


def handle_request_vote(state: RaftState, msg: RequestVote) -> RequestVoteReply:
    """RequestVote RPC, receiver implementation (paper Figure 2).

    1. Reply false if ``term < currentTerm``.
    2. If ``votedFor`` is null or ``candidateId``, and the candidate's log is at least
       as up-to-date as this node's, grant the vote.
    """
    state.observe_term(msg.term)

    if msg.term < state.current_term:
        return RequestVoteReply(term=state.current_term, vote_granted=False)

    already_committed_elsewhere = state.voted_for not in (None, msg.candidate_id)
    challenger_is_current = state.log.candidate_log_is_up_to_date(
        msg.last_log_term, msg.last_log_index
    )

    if already_committed_elsewhere or not challenger_is_current:
        return RequestVoteReply(term=state.current_term, vote_granted=False)

    state.voted_for = msg.candidate_id
    return RequestVoteReply(term=state.current_term, vote_granted=True)


def handle_append_entries(state: RaftState, msg: AppendEntries) -> AppendEntriesReply:
    """AppendEntries RPC, receiver implementation (paper Figure 2).

    1. Reply false if ``term < currentTerm``.
    2. Reply false if the log has no entry at ``prevLogIndex`` matching
       ``prevLogTerm``.
    3. Truncate on conflict and append the leader's entries.
    4. Advance ``commitIndex`` to ``min(leaderCommit, index of last new entry)``.
    """
    state.observe_term(msg.term)

    if msg.term < state.current_term:
        return AppendEntriesReply(
            term=state.current_term, success=False, request_sent_at=msg.sent_at
        )

    # A valid AppendEntries for the current term proves a leader exists for it: a
    # candidate must return to follower (paper §5.2), and a follower simply stays one.
    state.role = Role.FOLLOWER

    if not state.log.has_entry(msg.prev_log_index, msg.prev_log_term):
        return AppendEntriesReply(
            term=state.current_term, success=False, request_sent_at=msg.sent_at
        )

    state.log.append_new_entries(msg.entries)

    if msg.leader_commit > state.commit_index:
        state.commit_index = min(msg.leader_commit, state.log.last_index)

    # What this reply proves, stated by the node that actually stored it: the request's
    # prefix matched at prev_log_index and its entries are now appended. Not
    # ``log.last_index`` - entries beyond this request's reach may be a stale tail this
    # leader has not overwritten yet, which it must not be credited with.
    return AppendEntriesReply(
        term=state.current_term,
        success=True,
        match_index=msg.prev_log_index + len(msg.entries),
        request_sent_at=msg.sent_at,
    )
