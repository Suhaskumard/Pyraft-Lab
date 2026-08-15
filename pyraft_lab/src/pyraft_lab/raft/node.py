"""The async driver: timers, the receive loop, and who to send what.

This is the only file in ``raft/`` that knows about asyncio. Every decision it makes is
delegated to the pure functions in ``rpc.py`` and ``election.py``; what lives here is
purely the *when* - when an election timer expires, when to heartbeat, what to do with
each arriving message.

Timing runs on the event loop's clock for now. Day 8 swaps that for the virtual clock
without touching the decision logic, which is exactly why the two are separated.
"""

from __future__ import annotations

import asyncio
import contextlib
import random

from pyraft_lab import NodeId
from pyraft_lab.client.client import ClientReply, ClientRequest
from pyraft_lab.kv.store import KVStore, Result
from pyraft_lab.network.bus import Inbox, Transport
from pyraft_lab.network.messages import Message
from pyraft_lab.raft.election import (
    ELECTION_TIMEOUT_RANGE,
    HEARTBEAT_INTERVAL,
    VoteTally,
    become_leader,
    begin_election,
    random_election_timeout,
)
from pyraft_lab.raft.replication import (
    advance_commit_index,
    append_command,
    build_append_entries,
    handle_append_reply,
)
from pyraft_lab.raft.rpc import (
    AppendEntries,
    AppendEntriesReply,
    RequestVote,
    RequestVoteReply,
    handle_append_entries,
    handle_request_vote,
)
from pyraft_lab.raft.state import RaftState, Role


class RaftNode:
    """One Raft server: state, a transport, and the loop that drives them."""

    def __init__(
        self,
        node_id: NodeId,
        peers: list[NodeId],
        transport: Transport,
        *,
        rng: random.Random | None = None,
        election_timeout_range: tuple[float, float] = ELECTION_TIMEOUT_RANGE,
        heartbeat_interval: float = HEARTBEAT_INTERVAL,
    ) -> None:
        self.node_id = node_id
        self.peers = [p for p in peers if p != node_id]
        self.transport = transport
        self.state = RaftState(node_id=node_id)
        self.store = KVStore()

        # Seeded per node so a cluster's election order is reproducible from one seed.
        self._rng = rng if rng is not None else random.Random(hash(node_id) & 0xFFFF)
        self._election_timeout_range = election_timeout_range
        self._heartbeat_interval = heartbeat_interval

        self._inbox: Inbox | None = None
        self._task: asyncio.Task[None] | None = None
        self._tally: VoteTally | None = None
        self.leader_id: NodeId | None = None

        # Highest log index currently in flight to each peer. Figure 2's reply carries
        # no index, so the leader has to remember what it sent to interpret a success.
        self._inflight: dict[NodeId, int] = {}

        # Log index -> the client waiting on it. A write is answered only once its entry
        # has committed and applied, never on being appended.
        self._pending: dict[int, tuple[NodeId, str]] = {}

    @property
    def cluster_size(self) -> int:
        return len(self.peers) + 1

    @property
    def role(self) -> Role:
        return self.state.role

    @property
    def current_term(self) -> int:
        return self.state.current_term

    # --- lifecycle ---------------------------------------------------------------

    def start(self) -> None:
        """Register on the transport and begin driving timers."""
        if self._task is not None:
            raise RuntimeError(f"{self.node_id} is already running")
        self._inbox = self.transport.register(self.node_id)
        self._task = asyncio.create_task(self._run(), name=f"raft-{self.node_id}")

    async def stop(self) -> None:
        """Stop the loop and leave the transport. Models a clean shutdown, not a crash."""
        if self._task is not None:
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._task
            self._task = None
        if self._inbox is not None:
            self.transport.unregister(self.node_id)
            self._inbox = None

    # --- the loop ----------------------------------------------------------------

    async def _run(self) -> None:
        assert self._inbox is not None
        deadline = self._next_election_deadline()

        while True:
            if self.state.role is Role.LEADER:
                # A leader never stands for election; it only needs to wake up often
                # enough to heartbeat.
                timeout = self._heartbeat_interval
            else:
                timeout = max(0.0, deadline - self._now())

            try:
                envelope = await asyncio.wait_for(self._inbox.recv_envelope(), timeout)
            except TimeoutError:
                if self.state.role is Role.LEADER:
                    self._broadcast_heartbeat()
                else:
                    self._start_election()
                    deadline = self._next_election_deadline()
                continue

            if self._dispatch(envelope.src, envelope.body):
                deadline = self._next_election_deadline()

            # Both sides of replication can move commitIndex - a follower learns it from
            # leaderCommit, a leader derives it from matchIndex - so draining here covers
            # both without either handler having to remember to.
            self._apply_committed()

            if self._pending and self.state.role is not Role.LEADER:
                # We were deposed with requests outstanding. Their entries may still
                # commit under the new leader, but answering is no longer ours to do:
                # drop them and let the client's timeout drive a retry.
                self._pending.clear()

    def _now(self) -> float:
        return asyncio.get_running_loop().time()

    def _next_election_deadline(self) -> float:
        return self._now() + random_election_timeout(self._rng, self._election_timeout_range)

    # --- message handling --------------------------------------------------------

    def _dispatch(self, src: NodeId, message: Message) -> bool:
        """Route one message. Returns True if the election timer should reset.

        Only two things earn a reset (paper §5.2): hearing from a current leader, and
        granting a vote. Anything else - a stale RPC, a reply to our own campaign -
        must leave the timer alone, or a node could be kept from ever standing for
        election by traffic that proves nothing about a leader being alive.
        """
        match message:
            case RequestVote():
                reply = handle_request_vote(self.state, message)
                self._send(src, reply)
                return reply.vote_granted

            case AppendEntries():
                reply = handle_append_entries(self.state, message)
                self._send(src, reply)
                if reply.success:
                    self.leader_id = message.leader_id
                    self._tally = None  # a leader exists; our campaign is over
                return reply.success

            case RequestVoteReply():
                self._on_vote_reply(src, message)
                return False

            case AppendEntriesReply():
                self._on_append_reply(src, message)
                return False

            case ClientRequest():
                self._on_client_request(src, message)
                return False

            case _:
                return False

    def _on_vote_reply(self, voter: NodeId, reply: RequestVoteReply) -> None:
        if self.state.observe_term(reply.term):
            self._tally = None
            return

        # Ignore replies to an election we are no longer running: a stale reply from a
        # previous term must never contribute to a current majority.
        if self._tally is None or self.state.role is not Role.CANDIDATE:
            return
        if reply.term != self.state.current_term:
            return

        self._tally.record(voter, reply.vote_granted)
        if self._tally.won:
            self._win_election()

    def _on_append_reply(self, peer: NodeId, reply: AppendEntriesReply) -> None:
        if self.state.observe_term(reply.term):
            self._inflight.clear()
            return

        handle_append_reply(self.state, peer, reply, self._inflight.get(peer, 0))

        if reply.success:
            # Still behind? Send the next batch straight away rather than waiting for
            # the heartbeat tick - this is what makes catch-up fast.
            if self.state.next_index.get(peer, 0) <= self.state.log.last_index:
                self._replicate_to(peer)
            advance_commit_index(self.state, self.cluster_size)
        else:
            # nextIndex just walked back; retry immediately from further behind.
            self._replicate_to(peer)

    def submit(self, command: object, *, reply_to: tuple[NodeId, str] | None = None) -> int:
        """Append a client command to the log and start replicating it.

        Returns the index it was written at. The entry is *not* committed yet; when
        ``reply_to`` is given, that client is answered once the entry commits and
        applies. Raises ``NotLeader`` if this node is not the leader.
        """
        entry = append_command(self.state, command)
        if reply_to is not None:
            self._pending[entry.index] = reply_to
        # A single-node cluster has no peers to hear from, so nothing would otherwise
        # ever move commitIndex.
        advance_commit_index(self.state, self.cluster_size)
        self._apply_committed()
        for peer in self.peers:
            self._replicate_to(peer)
        return entry.index

    def _on_client_request(self, src: NodeId, request: ClientRequest) -> None:
        """Serve a client, or tell it who can.

        A follower does not forward the request on the client's behalf. Redirecting
        keeps the retry budget and the timing under the client's control, and means a
        node never holds state for a request it cannot finish.
        """
        if self.state.role is not Role.LEADER:
            self._send(
                src,
                ClientReply(
                    request_id=request.request_id,
                    ok=False,
                    not_leader=True,
                    leader_hint=self.leader_id,
                ),
            )
            return

        self.submit(request.command, reply_to=(src, request.request_id))

    def _apply_committed(self) -> list[Result]:
        """Hand every newly committed entry to the state machine, in order."""
        results = self.store.apply_committed(self.state.log, self.state.commit_index)
        self.state.last_applied = self.store.last_applied

        for result in results:
            waiter = self._pending.pop(result.index, None)
            if waiter is not None:
                client, request_id = waiter
                self._send(
                    client,
                    ClientReply(request_id=request_id, ok=result.ok, value=result.value),
                )
        return results

    def _start_election(self) -> None:
        request, self._tally = begin_election(self.state, self._rng, self.cluster_size)
        self.leader_id = None

        # A single-node cluster is its own majority and wins immediately.
        if self._tally.won:
            self._win_election()
            return

        for peer in self.peers:
            self._send(peer, request)

    def _win_election(self) -> None:
        become_leader(self.state, self.peers)
        self.leader_id = self.node_id
        self._tally = None
        self._inflight.clear()
        self._broadcast_heartbeat()

    def _broadcast_heartbeat(self) -> None:
        """Push each follower whatever it is missing (paper §5.3).

        A caught-up follower produces an empty ``entries`` list, which is precisely a
        heartbeat - so replication and proof-of-life are the same code path.
        """
        for peer in self.peers:
            self._replicate_to(peer)

    def _replicate_to(self, peer: NodeId) -> None:
        request = build_append_entries(self.state, peer)
        self._inflight[peer] = request.prev_log_index + len(request.entries)
        self._send(peer, request)

    def _send(self, dst: NodeId, message: Message) -> None:
        self.transport.send(self.node_id, dst, message)
