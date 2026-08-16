"""The async driver: timers, the receive loop, and who to send what.

This is the only file in ``raft/`` that knows about asyncio. Every decision it makes is
delegated to the pure functions in ``rpc.py`` and ``election.py``; what lives here is
purely the *when* - when an election timer expires, when to heartbeat, what to do with
each arriving message.

Timing runs against an injected ``Clock`` (Day 8): the real event loop clock by
default, or a ``VirtualClock`` a test or the experiment runner drives by hand. Neither
swap touches the decision logic in ``rpc.py``/``election.py``/``replication.py``,
which is exactly why the two are separated.

Persistence (Phase 6) is injected the same way and is equally optional. Given a
``WriteAheadLog``, this node recovers its Figure 2 persistent state from it before it
runs at all, and syncs anything new to disk before any message leaves - so a crash can
never lose something a peer or a client was already told. Given a ``SnapshotStore`` as
well, it snapshots the state machine when the log has grown past the policy's threshold
and compacts the WAL behind it.
"""

from __future__ import annotations

import asyncio
import contextlib
import random

from pyraft_lab import NodeId
from pyraft_lab.client.client import ClientReply, ClientRequest
from pyraft_lab.kv.commands import Get
from pyraft_lab.kv.store import KVStore, Result
from pyraft_lab.network.bus import Inbox, Transport
from pyraft_lab.network.clock import Clock, WallClock, race_deadline
from pyraft_lab.network.messages import Envelope, Message
from pyraft_lab.observability.events import EventKind
from pyraft_lab.observability.tracer import NULL_TRACER, Tracer
from pyraft_lab.raft.election import (
    ELECTION_TIMEOUT_RANGE,
    HEARTBEAT_INTERVAL,
    VoteTally,
    become_leader,
    begin_election,
    majority,
    random_election_timeout,
)
from pyraft_lab.raft.persistence import WalError, WalRecovery, WriteAheadLog
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
from pyraft_lab.raft.snapshot import (
    Snapshot,
    SnapshotError,
    SnapshotMeta,
    SnapshotPolicy,
    SnapshotStore,
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
        clock: Clock | None = None,
        tracer: Tracer | None = None,
        wal: WriteAheadLog | None = None,
        snapshots: SnapshotStore | None = None,
        snapshot_policy: SnapshotPolicy | None = None,
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
        # WallClock by default (unchanged behaviour); a VirtualClock from Day 8 makes
        # timing - and therefore a whole run - reproducible from a seed.
        self._clock: Clock = clock if clock is not None else WallClock()
        # Phase 4. Timestamps come off the same clock as the timers above, so an event's
        # time and the deadline that produced it are the same reading - a trace taken
        # against a VirtualClock is therefore exactly as reproducible as the run itself.
        self._tracer = tracer if tracer is not None else NULL_TRACER
        self._election_timeout_range = election_timeout_range
        self._heartbeat_interval = heartbeat_interval

        self._inbox: Inbox | None = None
        self._task: asyncio.Task[None] | None = None
        self._tally: VoteTally | None = None
        self.leader_id: NodeId | None = None

        # When the next heartbeat is due, while leading. Set on becoming leader and
        # advanced only when a heartbeat actually fires - never by the mere arrival of
        # some other message. See ``_run``'s docstring note on why this cannot just be
        # recomputed as "now + heartbeat_interval" at the top of the loop.
        self._next_heartbeat_at = 0.0

        # Highest log index currently in flight to each peer. Figure 2's reply carries
        # no index, so the leader has to remember what it sent to interpret a success.
        self._inflight: dict[NodeId, int] = {}

        # When an AppendEntries to a peer was sent and has not yet been answered.
        # Because a reply carries no index (D4), this value is only interpretable while
        # at most one request to a given peer is outstanding - two in flight at once
        # makes it ambiguous which one a given reply is *for*, and blindly crediting
        # the later (larger) one to an earlier reply can advance matchIndex past what
        # that peer has actually durably appended (see docs/architecture.md P8). So
        # ``_replicate_to`` skips a peer that is already awaiting a reply, and this is
        # cleared only when that reply arrives - or after ``_reply_timeout`` with no
        # reply at all, since a genuinely lost request or reply must not stall this
        # peer's replication forever.
        self._awaiting_reply: dict[NodeId, float] = {}
        # Exactly one heartbeat_interval, not several: this only needs to be long
        # enough that the handful of reactive resends a single burst of client writes
        # can trigger (submit, then _on_append_reply's "still behind" follow-up) land
        # within one window and so are seen as "one request in flight", not several
        # overlapping ones - the ambiguity this guard exists to prevent. Any longer
        # actively hurts: retries would then happen less often than the heartbeat tick
        # that used to (harmlessly) resend unconditionally, which under real packet
        # loss means fewer chances to get a message through before a peer's own
        # election timeout expires - trading the correctness bug for a liveness one.
        # heartbeat_interval is already documented as "comfortably under the minimum
        # election timeout" (raft/election.py), so anchoring to it - rather than to
        # election_timeout_range, which fast-election configurations narrow right down
        # towards this same timescale - keeps this timeout safely clear of that floor
        # regardless of how the two ranges relate to each other in a given setup.
        self._reply_timeout = self._heartbeat_interval

        # Log index -> the client waiting on it. A write is answered only once its entry
        # has committed and applied, never on being appended.
        self._pending: dict[int, tuple[NodeId, str]] = {}

        # Reads pending a lease confirmation and/or commit-index catch-up (Day 10):
        # (client, request_id, key, read_index).
        self._pending_reads: list[tuple[NodeId, str, str, int]] = []

        # Per-peer clock time of its last successful AppendEntries reply in the
        # current term - the leader lease Day 10's reads are served against.
        self._last_heartbeat_ack: dict[NodeId, float] = {}

        # Phase 6. Both optional and independent: no WAL means this node forgets
        # everything on restart, exactly as it did before this phase; a WAL without a
        # snapshot store means the file grows for the life of the run.
        self.wal = wal
        self.snapshots = snapshots
        self.snapshot_policy = (
            snapshot_policy
            if snapshot_policy is not None
            else (SnapshotPolicy() if snapshots is not None else None)
        )
        self.recovery: WalRecovery | None = None if wal is None else self._restore(wal)
        self._snapshot_index = self.state.log.base_index

        # What the WAL has been told. Term/vote and commitIndex all move inside pure
        # functions with nothing to persist them, so - as with the tracer below - the
        # node watches the values rather than asking each call site to remember.
        self._persisted_vote = (self.state.current_term, self.state.voted_for)
        self._persisted_commit = self.state.commit_index

        # Last values the tracer was told about. Term and commit index both move inside
        # pure functions that have no tracer and should not grow one, so the node
        # watches them instead of asking every call site to remember to report.
        # Initialized after any recovery above, so a restart does not narrate the term
        # it woke up in as a term change.
        self._traced_term = self.state.current_term
        self._traced_commit = self.state.commit_index

    @property
    def cluster_size(self) -> int:
        return len(self.peers) + 1

    @property
    def role(self) -> Role:
        return self.state.role

    @property
    def current_term(self) -> int:
        return self.state.current_term

    # --- persistence (Phase 6) ---------------------------------------------------
    #
    # Three obligations, and they are all about *ordering* rather than about writing:
    #
    # 1. recover before running. A node that campaigned in term 7 and then crashed must
    #    come back in term 7 with the vote it cast, or it can vote twice in one term and
    #    two leaders can be elected. Recovery therefore happens in ``__init__``, not in
    #    ``start`` - so no code path can observe this node in a state it never had.
    # 2. sync before speaking. Everything ``_send`` puts on the wire is a claim about
    #    this node's state; if the state that produced it is not yet on disk, a crash can
    #    make the claim retroactively false. So the fsync sits in ``_send`` itself rather
    #    than at each of the call sites that would have to remember.
    # 3. snapshot behind the state machine, never ahead of it. A snapshot may only cover
    #    entries that have been applied, since those are the only ones whose effect is
    #    in the state it captures.

    def _restore(self, wal: WriteAheadLog) -> WalRecovery:
        """Rebuild this node's state from its WAL, and attach the WAL to its log.

        2.0 item 9's restart sequence: load, validate, recover state, replay committed
        entries. Validation and the fold live in ``persistence.py``; what is left here is
        the part that needs to know about a *node* - installing the snapshot into the
        state machine, clamping the recovered commit index to the log that actually came
        back, and replaying the committed prefix through ``KVStore``.
        """
        recovery = wal.recover()
        if recovery.node_id and recovery.node_id != self.node_id:
            raise WalError(f"{wal.path} belongs to {recovery.node_id}, not to {self.node_id}")

        self.state.current_term = recovery.term
        self.state.voted_for = recovery.voted_for

        if recovery.snapshot is not None:
            self._install_snapshot(recovery.snapshot)

        self.state.log.restore(
            recovery.entries, base_index=recovery.base_index, base_term=recovery.base_term
        )
        # Only now is the log allowed to journal: restoring through the journal would
        # re-write on every restart the very records it had just read.
        self.state.log.journal = wal

        # Clamped, because the recovered commit index is a claim about a log we may hold
        # less of than we did: a torn tail can take an entry with it that a commit record
        # written before the crash already counted.
        self.state.commit_index = min(recovery.commit_index, self.state.log.last_index)
        self.store.apply_committed(self.state.log, self.state.commit_index)
        self.state.last_applied = self.store.last_applied
        return recovery

    def _install_snapshot(self, meta: SnapshotMeta) -> None:
        """Load the snapshot the WAL points at into the state machine."""
        if self.snapshots is None:
            raise SnapshotError(
                f"{self.node_id}'s WAL was compacted against a snapshot at index "
                f"{meta.last_included_index}, but no snapshot store was given to read it from"
            )
        snapshot = self.snapshots.load(meta.last_included_index)
        self.store.restore(snapshot.data, snapshot.meta.last_included_index)

    def _persist(self) -> None:
        """Write anything that has changed, then fsync. Called before this node speaks.

        Log entries are already in the file by now - ``RaftLog``'s journal wrote them as
        they were appended - so what is left is the pair of scalars no journal watches,
        and the sync that makes all of it durable together.
        """
        wal = self.wal
        if wal is None:
            return

        vote = (self.state.current_term, self.state.voted_for)
        if vote != self._persisted_vote:
            wal.record_term(*vote)
            self._persisted_vote = vote
        if self.state.commit_index != self._persisted_commit:
            wal.record_commit(self.state.commit_index)
            self._persisted_commit = self.state.commit_index
        wal.sync()

    def take_snapshot(self) -> SnapshotMeta | None:
        """Snapshot the state machine and compact the WAL behind it (2.0 item 10).

        Returns the snapshot's metadata, or ``None`` if there was nothing new to capture.
        Public because ``pyraft-lab snapshot --node ...`` (Phase 9, once there is a
        cluster to address) and a test both want to ask for one by hand rather than wait
        for the threshold.

        The order is what makes this crash-safe: the snapshot file is written and synced
        *before* the WAL is compacted against it, so a crash in between leaves an
        uncompacted WAL that still holds every entry, and the orphaned snapshot is
        simply ignored.
        """
        if self.wal is None or self.snapshots is None:
            return None

        index = self.state.last_applied
        term = self.state.log.term_at(index)
        if index <= self._snapshot_index or term is None:
            return None

        snapshot = Snapshot.create(
            last_included_index=index,
            last_included_term=term,
            data=self.store.snapshot(),
            node_id=self.node_id,
            created_at=self._now(),
        )
        self.snapshots.save(snapshot)
        self.wal.compact(
            snapshot=snapshot.meta,
            term=self.state.current_term,
            voted_for=self.state.voted_for,
            entries=self.state.log.entries_from(index + 1),
            commit_index=self.state.commit_index,
        )
        # Compaction rewrote both scalars, so what the WAL holds is current by
        # definition; leaving the watchers stale would write them again for nothing.
        self._persisted_vote = (self.state.current_term, self.state.voted_for)
        self._persisted_commit = self.state.commit_index
        self._snapshot_index = index
        self.snapshots.prune()
        return snapshot.meta

    def _maybe_snapshot(self) -> None:
        """Take a snapshot if the policy says the log has grown enough."""
        policy = self.snapshot_policy
        if policy is None or self.wal is None or self.snapshots is None:
            return
        if policy.due(
            applied_index=self.state.last_applied,
            last_snapshot_index=self._snapshot_index,
            wal_bytes=self.wal.size,
        ):
            self.take_snapshot()

    # --- lifecycle ---------------------------------------------------------------

    def start(self) -> None:
        """Register on the transport and begin driving timers."""
        if self._task is not None:
            raise RuntimeError(f"{self.node_id} is already running")
        self._inbox = self.transport.register(self.node_id)
        self._task = asyncio.create_task(self._run(), name=f"raft-{self.node_id}")

    async def stop(self, *, crashed: bool = True) -> None:
        """Stop the loop and leave the transport.

        The default is to have the departure recorded as a crash, because that is what
        stopping a node means in an experiment - and from the network's side a clean
        stop and a crash are indistinguishable anyway (P4). ``crashed=False`` is for
        tearing a cluster down at the end of a run, where a crash event would put a
        fault in the trace that was really just the experiment ending.

        Cleanup runs in ``finally`` and stopping still re-raises whatever the loop
        died of: if ``_run`` exits on something other than ``CancelledError`` (an
        unexpected bug, not a deliberate stop), that is a real finding and must not be
        swallowed - but the transport registration and the WAL's file handle must be
        released regardless, or a crashed task's exception leaks both, which on
        Windows then turns a later ``TemporaryDirectory`` cleanup into a confusing
        second ``PermissionError`` that has nothing to do with the actual bug.
        """
        task, self._task = self._task, None
        try:
            if task is not None:
                task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await task
        finally:
            if self._inbox is not None:
                self.transport.unregister(self.node_id, crashed=crashed)
                self._inbox = None
            if self.wal is not None:
                # No fsync here on purpose: from the WAL's side stopping *is* the crash,
                # and everything this node ever told anyone was synced before it said
                # it. The file is only let go of so a stopped node holds no handle; the
                # next write reopens it.
                self.wal.close()

    # --- the loop ----------------------------------------------------------------

    async def _run(self) -> None:
        """The reactor loop: wait for the next message or the next deadline, whichever
        comes first.

        A leader's wait target is ``_next_heartbeat_at``, not a fresh ``now() +
        heartbeat_interval`` computed here every time round - that recompute was once
        the whole bug: it made the deadline recede every time *any* message arrived,
        from any source, for any reason. Under real traffic (client requests,
        replication acks flowing back from followers) some message is close to always
        arriving within one heartbeat_interval, so the timeout branch that calls
        ``_broadcast_heartbeat`` could go unreached indefinitely - starving a follower
        that happens to already be caught up (nothing new to reactively push to it) of
        heartbeats altogether, until it times out and starts an election nobody
        expected. ``_next_heartbeat_at`` only ever moves forward when a heartbeat
        actually goes out (here, and in ``_win_election``), so it is a real deadline
        rather than one perpetually pushed out of reach.
        """
        assert self._inbox is not None
        deadline = self._next_election_deadline()

        while True:
            if self.state.role is Role.LEADER:
                # A leader never stands for election; it only needs to wake up often
                # enough to heartbeat.
                deadline = self._next_heartbeat_at

            envelope = await self._wait_for_message(deadline)

            if envelope is None:
                if self.state.role is Role.LEADER:
                    self._broadcast_heartbeat()
                    self._next_heartbeat_at = self._now() + self._heartbeat_interval
                else:
                    self._start_election()
                    deadline = self._next_election_deadline()
                continue

            was_leader = self.state.role is Role.LEADER
            reset_timer = self._dispatch(envelope.src, envelope.body)
            # Catches a step-down that produced no event of its own - a *reply* carrying
            # a higher term. Every other path reports through ``_trace``, which checks
            # the term itself before emitting.
            self._check_term_change()

            # A reply (vote or append) never earns a reset by _dispatch's own rule -
            # replies aren't proof of a current leader, so a plain follower is right to
            # ignore them. But a node that *was* leader a moment ago and just stepped
            # down (observe_term, inside handling this very reply) is a different case:
            # its deadline is still the leader's ~heartbeat_interval-away recompute from
            # the top of this loop, not a real election timeout. Left alone, that stale,
            # far-too-short deadline would let a freshly-demoted leader race back into
            # candidacy well ahead of every other node's honestly-randomized timeout -
            # an unfair head start with nothing in the paper to justify it, and the
            # mechanism behind the runaway re-election storms this fixes (see
            # docs/architecture.md P8).
            stepped_down_from_leader = was_leader and self.state.role is not Role.LEADER
            if reset_timer or stepped_down_from_leader:
                deadline = self._next_election_deadline()

            # Both sides of replication can move commitIndex - a follower learns it from
            # leaderCommit, a leader derives it from matchIndex - so draining here covers
            # both without either handler having to remember to.
            self._apply_committed()
            self._try_serve_reads()

            if self.state.role is not Role.LEADER:
                # We were deposed with requests outstanding. Their entries may still
                # commit under the new leader, but answering is no longer ours to do:
                # drop them and let the client's timeout drive a retry.
                if self._pending:
                    self._pending.clear()
                if self._pending_reads:
                    self._pending_reads.clear()

    def _now(self) -> float:
        return self._clock.now()

    async def _wait_for_message(self, deadline: float) -> Envelope | None:
        """Block until the next message arrives or ``deadline`` passes on our clock.

        Both clocks are handled by ``race_deadline`` (see it for why they need
        different mechanisms); this node's only job is to say what it is waiting for.
        """
        assert self._inbox is not None
        return await race_deadline(self._clock, self._inbox.recv_envelope(), deadline)

    def _next_election_deadline(self) -> float:
        return self._now() + random_election_timeout(self._rng, self._election_timeout_range)

    # --- tracing (Phase 4) -------------------------------------------------------
    #
    # Nothing below changes a decision; it only reports one. The pure modules stay
    # pure - ``election.py``, ``rpc.py`` and ``replication.py`` never learn that a
    # tracer exists - so instrumentation cannot alter what the algorithm does, which
    # is the one property a debugging tool must not cost you.

    def _trace(self, kind: EventKind, **details: object) -> None:
        """Record an event attributed to this node, in its current term.

        Checks the term first so that a message which both raised our term and
        produced an event yields TERM_CHANGED *before* that event, rather than a trace
        that appears to answer an RPC from a term it had not reached yet.
        """
        self._check_term_change()
        self._tracer.emit(kind, node=self.node_id, term=self.state.current_term, **details)

    def _check_term_change(self) -> None:
        """Emit TERM_CHANGED if the term moved since we last looked.

        A watcher rather than a call at each site: ``RaftState.observe_term`` is
        already the single funnel every term increase goes through (D2), and it is a
        pure function that would have to be handed a tracer to report for itself.
        Emitting directly - not through ``_trace`` - is what keeps this from recursing.
        """
        if self.state.current_term == self._traced_term:
            return
        previous, self._traced_term = self._traced_term, self.state.current_term
        self._tracer.emit(
            EventKind.TERM_CHANGED,
            node=self.node_id,
            term=self.state.current_term,
            previous_term=previous,
            role=self.state.role.value,
        )

    def _check_commit_advance(self) -> None:
        """Emit LOG_COMMITTED if commitIndex moved.

        Both ends of replication move it - a follower from ``leaderCommit``, a leader
        from ``matchIndex`` - so watching the value covers both without either path
        having to remember to report. Called from ``_apply_committed``, which every
        path that can advance it already routes through.
        """
        if self.state.commit_index == self._traced_commit:
            return
        previous, self._traced_commit = self._traced_commit, self.state.commit_index
        self._trace(
            EventKind.LOG_COMMITTED,
            **{"from": previous},
            to=self.state.commit_index,
            entries=self.state.commit_index - previous,
        )

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
                if reply.vote_granted:
                    self._trace(
                        EventKind.VOTE_GRANTED,
                        candidate=message.candidate_id,
                        last_log_index=message.last_log_index,
                        last_log_term=message.last_log_term,
                    )
                return reply.vote_granted

            case AppendEntries():
                reply = handle_append_entries(self.state, message)
                self._send(src, reply)
                if reply.success:
                    self.leader_id = message.leader_id
                    self._tally = None  # a leader exists; our campaign is over
                else:
                    # Recorded here, on the follower, rather than on the leader that
                    # sees the rejection: *why* it rejected is knowable only here.
                    # Figure 2's reply carries term and success and nothing else, and
                    # D4 keeps it that way deliberately, so the reason would otherwise
                    # be lost. The leader's reaction is already visible as the next
                    # APPEND_ENTRIES_SENT with a lower prev_log_index.
                    stale = message.term < self.state.current_term
                    self._trace(
                        EventKind.APPEND_ENTRIES_REJECTED,
                        leader=message.leader_id,
                        reason="stale_term" if stale else "log_mismatch",
                        prev_log_index=message.prev_log_index,
                        prev_log_term=message.prev_log_term,
                    )
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
            self._awaiting_reply.clear()
            return

        # A reply to a request sent while we were still leader can arrive after we have
        # since stepped down (observed a higher term via some other message). handle_
        # append_reply already no-ops in that case, but without this check the retry
        # logic below would not: it would send more AppendEntries - carrying our
        # updated term - as if we were still leader, which a peer has no way to tell
        # apart from a genuine leader's traffic. Left unchecked this is what turns one
        # step-down into a cluster-wide election storm (docs/architecture.md P8).
        if self.state.role is not Role.LEADER:
            return

        # This reply is - by the single-in-flight invariant _replicate_to maintains -
        # unambiguously the answer to the one request we had outstanding to this peer,
        # so _inflight[peer] is safe to interpret as what it confirms. Cleared before
        # acting on it so a retry decided below is free to send immediately rather than
        # being skipped as "already awaiting a reply".
        self._awaiting_reply.pop(peer, None)
        handle_append_reply(self.state, peer, reply, self._inflight.get(peer, 0))

        if reply.success:
            # Proof of life for the lease (Day 10): this peer has just processed an
            # AppendEntries from us in our current term, which - per the follower's
            # own reset rule in ``_dispatch`` - reset its election timer.
            self._last_heartbeat_ack[peer] = self._clock.now()
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
        # Explicitly, rather than relying on the sends below: a single-node cluster has
        # no peer to replicate to, and a submission with no client waiting on it sends
        # nothing at all - but the entry is committed either way, so it has to be safe.
        self._persist()
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

        if request.command.get("kind") == Get.KIND:
            self._handle_read(src, request)
            return

        self.submit(request.command, reply_to=(src, request.request_id))

    # --- linearizable reads (Day 10) ----------------------------------------------
    #
    # A read never touches the log: ``KVStore.read`` is already local and instant, so
    # the only thing standing between it and linearizability is proving this node is
    # still the leader as of the read. Rather than paper §8's ReadIndex - which needs
    # a round trip tagged well enough to survive reordering, and Figure 2's
    # AppendEntriesReply deliberately carries nothing to tag it with (see
    # docs/architecture.md D4) - a read is instead served once a majority of peers
    # have acknowledged an AppendEntries in this term within the last
    # ``election_timeout_range[0]`` seconds: since a follower's own randomly chosen
    # timeout is never shorter than that minimum, nobody could have voted for a
    # challenger since. ``_win_election``'s no-op commit closes the other half of the
    # gap: it is what lets ``commit_index`` (and so ``read_index``) catch up to the
    # true committed state after a failover, rather than sitting frozen at whatever a
    # since-deposed leader last advertised.

    def _handle_read(self, src: NodeId, request: ClientRequest) -> None:
        key = request.command["key"]
        self._pending_reads.append((src, request.request_id, key, self.state.commit_index))
        self._try_serve_reads()

    def _try_serve_reads(self) -> None:
        if not self._pending_reads or not self._has_lease():
            return

        still_pending: list[tuple[NodeId, str, str, int]] = []
        for client, request_id, key, read_index in self._pending_reads:
            if self.state.last_applied >= read_index:
                self._send(
                    client,
                    ClientReply(
                        request_id=request_id, ok=key in self.store, value=self.store.read(key)
                    ),
                )
            else:
                still_pending.append((client, request_id, key, read_index))
        self._pending_reads = still_pending

    def _has_lease(self) -> bool:
        """Is a majority of the cluster still within the window paper §5.2 guarantees
        they cannot yet have voted for a challenger in?
        """
        if self.state.role is not Role.LEADER:
            return False
        now = self._clock.now()
        lease = self._election_timeout_range[0]
        live = 1 + sum(
            1
            for peer in self.peers
            if now - self._last_heartbeat_ack.get(peer, float("-inf")) <= lease
        )
        return live >= majority(self.cluster_size)

    def _apply_committed(self) -> list[Result]:
        """Hand every newly committed entry to the state machine, in order."""
        # Before applying, not after: commitIndex moving is the event, and an entry is
        # committed - unlosable - whether or not this node has run it yet.
        self._check_commit_advance()
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

        # After the clients have their answers, not before: a snapshot rewrites the WAL,
        # and there is no reason to make a waiting client's reply queue behind that.
        if results:
            self._maybe_snapshot()
        return results

    def _start_election(self) -> None:
        request, self._tally = begin_election(self.state, self._rng, self.cluster_size)
        self.leader_id = None
        self._trace(
            EventKind.ELECTION_STARTED,
            last_log_index=request.last_log_index,
            last_log_term=request.last_log_term,
            cluster_size=self.cluster_size,
        )

        # A single-node cluster is its own majority and wins immediately.
        if self._tally.won:
            self._win_election()
            return

        for peer in self.peers:
            self._trace(EventKind.VOTE_REQUESTED, peer=peer)
            self._send(peer, request)

    def _win_election(self) -> None:
        # Read before the tally is dropped below: the winning count is the one thing
        # about an election that exists nowhere in the resulting state.
        votes = self._tally.votes if self._tally is not None else 1

        become_leader(self.state, self.peers)
        self.leader_id = self.node_id
        self._tally = None
        self._inflight.clear()
        self._awaiting_reply.clear()
        self._last_heartbeat_ack.clear()
        self._trace(EventKind.LEADER_ELECTED, votes=votes, cluster_size=self.cluster_size)

        # A no-op entry in the new term (paper §8). Without it, commit_index can sit
        # frozen at whatever this node knew as a follower: advance_commit_index's
        # current-term restriction (§5.4.2) blocks it from counting a majority on an
        # *older*-term entry no matter how many peers already match it, even though
        # Leader Completeness guarantees this log already holds it. One entry from our
        # own term unfreezes all of that in a single jump the next time it commits -
        # which matters most exactly when nothing else would ever commit: an idle
        # cluster whose new leader is only ever asked to serve reads.
        append_command(self.state, None)
        advance_commit_index(self.state, self.cluster_size)
        self._apply_committed()

        self._broadcast_heartbeat()
        self._next_heartbeat_at = self._now() + self._heartbeat_interval

    def _broadcast_heartbeat(self) -> None:
        """Push each follower whatever it is missing (paper §5.3).

        A caught-up follower produces an empty ``entries`` list, which is precisely a
        heartbeat - so replication and proof-of-life are the same code path.
        """
        for peer in self.peers:
            self._replicate_to(peer)

    def _replicate_to(self, peer: NodeId) -> None:
        """Send ``peer`` whatever it is missing - unless one is already in flight.

        At most one outstanding request per peer, always: with two in flight, a reply
        carrying no index (Figure 2, D4) cannot say which of them it is answering, and
        crediting it to whichever was sent *last* - the only thing ``_inflight`` can
        remember - can advance ``matchIndex`` past what the peer has actually durably
        appended if that later request (or its reply) is the one that gets lost. A
        timeout, not a reply, is what releases a peer stuck here: a genuinely lost
        request or reply must not stall this peer's replication forever, and it is
        fine to skip ahead to whatever the log looks like by the time we retry - the
        next reply is what will correctly advance matchIndex to wherever it now
        applies.
        """
        pending_since = self._awaiting_reply.get(peer)
        if pending_since is not None and self._now() - pending_since < self._reply_timeout:
            return

        request = build_append_entries(self.state, peer)
        self._inflight[peer] = request.prev_log_index + len(request.entries)
        self._awaiting_reply[peer] = self._now()
        self._trace(
            EventKind.APPEND_ENTRIES_SENT,
            peer=peer,
            prev_log_index=request.prev_log_index,
            entries=len(request.entries),
            leader_commit=request.leader_commit,
        )
        self._send(peer, request)

    def _send(self, dst: NodeId, message: Message) -> None:
        # Durability before disclosure (Phase 6): a vote, an entry or a commit index
        # this node is about to tell somebody about has to be on disk first, or a crash
        # can make what it said untrue. Cheap in practice - ``sync`` is a no-op unless
        # something was actually written, so an idle heartbeat costs nothing.
        self._persist()
        self.transport.send(self.node_id, dst, message)
