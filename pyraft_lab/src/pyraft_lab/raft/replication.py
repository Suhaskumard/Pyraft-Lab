"""Log replication, leader side: what to send, and what a reply means.

The follower half of replication already exists - ``handle_append_entries`` in
``rpc.py`` does the consistency check, the conflict truncation and the commit-index
advance (Day 2). This module is the other end: deciding which entries a given follower
still needs, walking ``nextIndex`` backward when it rejects, and working out when a
majority has made an entry safe to commit.

Pure functions again, for the reasons in docs/architecture.md D2. ``node.py`` owns the
sending; the in-flight bookkeeping it needs is described under ``handle_append_reply``.
"""

from __future__ import annotations

from pyraft_lab import NodeId
from pyraft_lab.raft.election import majority
from pyraft_lab.raft.log import LogEntry
from pyraft_lab.raft.rpc import AppendEntries, AppendEntriesReply
from pyraft_lab.raft.state import RaftState, Role

MAX_BATCH = 64
"""Entries per AppendEntries. Caps the message size a lagging follower can provoke."""


def append_command(state: RaftState, command: object) -> LogEntry:
    """Append a client command to the leader's own log (paper §5.3).

    The entry is not committed here - it becomes committed only once
    :func:`advance_commit_index` sees it replicated on a majority.
    """
    if state.role is not Role.LEADER:
        raise NotLeader(f"{state.node_id} is {state.role.value}, not the leader")

    entry = LogEntry(
        term=state.current_term,
        index=state.log.last_index + 1,
        command=command,
    )
    state.log.append(entry)
    return entry


class NotLeader(Exception):
    """Raised when a command is submitted to a node that is not the leader."""


def build_append_entries(
    state: RaftState, peer: NodeId, *, max_batch: int = MAX_BATCH
) -> AppendEntries:
    """Build the next AppendEntries for ``peer`` from its ``nextIndex``.

    Sends everything the follower is missing, up to ``max_batch``. When it is already
    caught up this naturally produces an empty ``entries`` list - which is exactly a
    heartbeat, so a leader needs no separate heartbeat path.

    ``prevLogIndex`` never goes below the log's base (Phase 6): on a leader that came
    back from a compacted WAL, the entries below it are not there to send. Clamping is
    right rather than merely safe for every follower that still holds the committed
    prefix - which is all of them, since a snapshot only ever covers applied entries -
    and it is where a follower that has *lost* that prefix would need an InstallSnapshot
    we deliberately do not implement (see raft/snapshot.py and architecture P6).
    """
    next_index = state.next_index.get(peer, state.log.last_index + 1)
    prev_index = max(next_index - 1, state.log.base_index)
    prev_term = state.log.term_at(prev_index)
    if prev_term is None:
        # The follower's nextIndex points past our own log - only possible from stale
        # leader state. Fall back to the end of our log and let the check re-converge.
        prev_index = state.log.last_index
        prev_term = state.log.last_term

    entries: list[LogEntry] = []
    if prev_index < state.log.last_index:
        entries = state.log.entries_from(prev_index + 1)[:max_batch]

    return AppendEntries(
        term=state.current_term,
        leader_id=state.node_id,
        prev_log_index=prev_index,
        prev_log_term=prev_term,
        entries=entries,
        leader_commit=state.commit_index,
    )


def handle_append_reply(
    state: RaftState, peer: NodeId, reply: AppendEntriesReply, last_index_sent: int
) -> None:
    """Fold a follower's reply into the leader's per-follower tracking.

    ``last_index_sent`` is the highest index the leader put in the request this reply
    answers. Figure 2's reply carries only ``term`` and ``success``, so the leader must
    remember what it sent rather than learn it back - ``node.py`` keeps that in-flight
    number per peer.

    On success the follower's log is known to match up to that index. On failure the
    logs disagree at ``prevLogIndex``, so ``nextIndex`` walks back one and the next
    round retries from further behind (paper §5.3).
    """
    if state.role is not Role.LEADER or reply.term != state.current_term:
        return  # not ours to act on; a higher term is handled by observe_term upstream

    if reply.success:
        # max(): a delayed reply to an older, shorter request must never drag
        # matchIndex backward once a later one has already advanced it.
        state.match_index[peer] = max(state.match_index.get(peer, 0), last_index_sent)
        state.next_index[peer] = state.match_index[peer] + 1
    else:
        state.next_index[peer] = max(1, state.next_index.get(peer, 1) - 1)


def advance_commit_index(state: RaftState, cluster_size: int) -> bool:
    """Commit everything a majority has stored, subject to Raft's term restriction.

    Paper §5.4.2: a leader may only commit an entry from its *own* term this way.
    Counting an older-term entry as committed just because it is widely replicated is
    the classic Figure 8 bug - such an entry can still be overwritten. Older entries
    become committed indirectly, carried along when a current-term entry commits.

    Returns True if ``commitIndex`` moved.
    """
    if state.role is not Role.LEADER:
        return False

    # The leader stores everything in its own log, so it counts toward every majority.
    replicated = sorted([*state.match_index.values(), state.log.last_index], reverse=True)
    candidate = replicated[majority(cluster_size) - 1]

    if candidate > state.commit_index and state.log.term_at(candidate) == state.current_term:
        state.commit_index = candidate
        return True
    return False
