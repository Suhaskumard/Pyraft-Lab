"""The client: find the leader, survive it changing, and report what happened.

A Raft client has no privileged view of the cluster. It knows a member list, it guesses
a leader, and it learns it guessed wrong by being told so. Everything here follows from
that: a redirect is normal traffic rather than an error, and a request that fails is
retried against a different node rather than abandoned.

Every outcome is counted. Phase 5's experiment runner reads those counters to compute
availability under fault injection, so "how many requests were redirected" is data, not
debug noise - which is why redirects are tracked separately from failures.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from typing import Any, ClassVar

from pyraft_lab import NodeId
from pyraft_lab.kv.commands import Cas, Command, Delete, Get, Put
from pyraft_lab.network.messages import Message


class ClientRequest(Message):
    """A command submitted to a node for replication."""

    KIND: ClassVar[str] = "client_request"

    request_id: str
    command: dict[str, Any]


class ClientReply(Message):
    """The answer, or a redirect to whoever should have been asked.

    ``leader_hint`` is what turns a rejection into progress: a follower that knows the
    current leader names it, so the client's next attempt is informed rather than
    another guess.
    """

    KIND: ClassVar[str] = "client_reply"

    request_id: str
    ok: bool
    value: Any = None
    not_leader: bool = False
    leader_hint: NodeId | None = None
    error: str | None = None


@dataclass
class ClientMetrics:
    """Per-client request accounting, read by the experiment layer."""

    attempts: int = 0
    successes: int = 0
    failures: int = 0
    redirects: int = 0
    timeouts: int = 0
    latencies: list[float] = field(default_factory=list)

    @property
    def availability(self) -> float:
        """Fraction of *requests* that eventually succeeded.

        Deliberately counted per request, not per attempt: a request that succeeded on
        its third try was still served, and a system that retries into success is
        available. Retry cost shows up in the latency series instead.
        """
        served = self.successes + self.failures
        return self.successes / served if served else 0.0

    def as_dict(self) -> dict[str, Any]:
        return {
            "attempts": self.attempts,
            "successes": self.successes,
            "failures": self.failures,
            "redirects": self.redirects,
            "timeouts": self.timeouts,
            "availability": self.availability,
        }


class RequestFailed(Exception):
    """Raised when a request could not be served within its retry budget."""


@dataclass
class RetryPolicy:
    """Exponential backoff with a ceiling.

    Backing off matters most in exactly the case that provokes it: a cluster mid-election
    has no leader to answer anyone, and a client hammering it at full rate delays the
    election it is waiting for.
    """

    max_attempts: int = 5
    initial_backoff: float = 0.01
    multiplier: float = 2.0
    max_backoff: float = 0.5

    def backoff_for(self, attempt: int) -> float:
        """Delay before ``attempt`` (0-based), capped at ``max_backoff``."""
        return min(self.initial_backoff * (self.multiplier**attempt), self.max_backoff)


class KVClient:
    """Talks to the cluster over the same transport the nodes use.

    ``send`` is injected rather than reached for, so the same client drives a real
    ``InMemoryBus`` today and Phase 3's fault-injecting simulator unchanged.
    """

    def __init__(
        self,
        client_id: NodeId,
        members: list[NodeId],
        submit: Any,
        *,
        policy: RetryPolicy | None = None,
    ) -> None:
        self.client_id = client_id
        self.members = list(members)
        self._submit = submit
        self.policy = policy or RetryPolicy()
        self.metrics = ClientMetrics()
        self.leader_hint: NodeId | None = None
        self._next_request = 0

    # --- the four operations ------------------------------------------------------

    async def put(self, key: str, value: Any) -> Any:
        return await self._call(Put(key=key, value=value))

    async def get(self, key: str) -> Any:
        return await self._call(Get(key=key))

    async def delete(self, key: str) -> bool:
        result = await self._call(Delete(key=key), want_value=False)
        return bool(result)

    async def cas(self, key: str, expected: Any, new: Any) -> bool:
        result = await self._call(Cas(key=key, expected=expected, new=new), want_value=False)
        return bool(result)

    # --- the retry loop -----------------------------------------------------------

    async def _call(self, command: Command, *, want_value: bool = True) -> Any:
        request_id = f"{self.client_id}-{self._next_request}"
        self._next_request += 1

        last_error = "no attempt was made"

        for attempt in range(self.policy.max_attempts):
            if attempt:
                await asyncio.sleep(self.policy.backoff_for(attempt - 1))

            target = self._pick_target(attempt)
            self.metrics.attempts += 1

            started = asyncio.get_running_loop().time()
            reply = await self._submit(
                target, ClientRequest(request_id=request_id, command=command.to_wire())
            )
            elapsed = asyncio.get_running_loop().time() - started

            if reply is None:
                # No answer at all: a crashed or unreachable node. Try someone else.
                self.metrics.timeouts += 1
                last_error = f"no reply from {target}"
                self._forget_leader(target)
                continue

            if reply.not_leader:
                # Not a failure - the cluster told us where to go.
                self.metrics.redirects += 1
                self.leader_hint = reply.leader_hint
                last_error = f"{target} is not the leader"
                continue

            if not reply.ok and reply.error:
                self.metrics.failures += 1
                raise RequestFailed(reply.error)

            self.leader_hint = target
            self.metrics.successes += 1
            self.metrics.latencies.append(elapsed)
            return reply.value if want_value else reply.ok

        self.metrics.failures += 1
        raise RequestFailed(
            f"{request_id} gave up after {self.policy.max_attempts} attempts: {last_error}"
        )

    def _pick_target(self, attempt: int) -> NodeId:
        """Prefer the known leader; otherwise walk the member list.

        Rotating rather than re-asking the same node matters when the hint is stale: a
        client that keeps retrying one dead node never discovers the live majority.
        """
        if self.leader_hint is not None:
            return self.leader_hint
        return self.members[attempt % len(self.members)]

    def _forget_leader(self, target: NodeId) -> None:
        if self.leader_hint == target:
            self.leader_hint = None


class BusChannel:
    """Sends a request over a transport and waits for its matching reply.

    Matching on ``request_id`` rather than taking the next message off the inbox is what
    makes this safe once the network can delay and duplicate: a reply to an attempt the
    client already gave up on must be discarded, not mistaken for the current answer.
    """

    def __init__(self, transport: Any, client_id: NodeId, *, timeout: float = 0.2) -> None:
        self.transport = transport
        self.client_id = client_id
        self.timeout = timeout
        self.inbox = transport.register(client_id)

    def close(self) -> None:
        self.transport.unregister(self.client_id)

    async def __call__(self, target: NodeId, request: ClientRequest) -> ClientReply | None:
        self.transport.send(self.client_id, target, request)

        loop = asyncio.get_running_loop()
        deadline = loop.time() + self.timeout

        while True:
            remaining = deadline - loop.time()
            if remaining <= 0:
                return None
            try:
                _, message = await asyncio.wait_for(self.inbox.recv(), remaining)
            except TimeoutError:
                return None

            if isinstance(message, ClientReply) and message.request_id == request.request_id:
                return message
            # Anything else is a straggler from an abandoned attempt: drop it.
