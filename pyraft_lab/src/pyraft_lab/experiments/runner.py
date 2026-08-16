"""Running a scenario: build the cluster, break it on schedule, and keep the receipts.

The whole experiment runs on a :class:`~pyraft_lab.network.clock.VirtualClock`, and
that is what makes the laboratory usable at all. Timers keep their real, paper-shaped
values - a 150-300 ms election timeout, a 50 ms heartbeat - so what is measured is the
behaviour of a realistic cluster, but thirty simulated seconds cost no real seconds to
watch. It is also what makes a run reproducible: the same seed produces the same
elections, the same delivery order and the same operations.

Time advances by jumping to whatever is scheduled next - the next timer, the next fault,
or the end - rather than by ticking. Empty time costs nothing, which is why a 30-second
scenario runs in milliseconds instead of in 6000 pointless steps.

Nothing here decides whether a run was good. The runner produces a :class:`RunResult` -
the trace, the history, the network counters, the final state of every node - and
``metrics.py`` judges it. Keeping "what happened" apart from "was that acceptable" means
a scenario can be re-scored later without being re-run.
"""

from __future__ import annotations

import asyncio
import random
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from pyraft_lab import NodeId
from pyraft_lab.client.client import BusChannel, ClientMetrics, KVClient, RetryPolicy
from pyraft_lab.client.workload import Workload
from pyraft_lab.consistency.history import History
from pyraft_lab.experiments.scenario import (
    CRASH,
    HEAL_PARTITION,
    LATENCY,
    PACKET_LOSS,
    PARTITION,
    RECOVER,
    RESET_FAULTS,
    FaultStep,
    Scenario,
    ScenarioError,
)
from pyraft_lab.network.clock import VirtualClock
from pyraft_lab.network.simulator import SimulatedNetwork, SimulatorStats
from pyraft_lab.observability.events import Event
from pyraft_lab.observability.metrics import LiveMetrics
from pyraft_lab.observability.tracer import Tracer
from pyraft_lab.raft.election import ELECTION_TIMEOUT_RANGE, HEARTBEAT_INTERVAL
from pyraft_lab.raft.node import RaftNode
from pyraft_lab.raft.persistence import WriteAheadLog
from pyraft_lab.raft.state import Role

CLIENT_TIMEOUT = 0.2
"""Seconds a client waits for a reply before trying somebody else."""

PUMP_LIMIT = 200
"""Event-loop turns allowed between clock jumps before we stop waiting for quiet."""


@dataclass
class NodeSnapshot:
    """What one node believed at the end of the run."""

    node_id: NodeId
    alive: bool
    role: str
    term: int
    commit_index: int
    last_applied: int
    log_length: int
    committed: list[Any] = field(default_factory=list)
    state: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "node_id": self.node_id,
            "alive": self.alive,
            "role": self.role,
            "term": self.term,
            "commit_index": self.commit_index,
            "last_applied": self.last_applied,
            "log_length": self.log_length,
        }


@dataclass
class AppliedFault:
    """A timeline step and what it actually did.

    Recorded separately from the scenario's own timeline because the two can differ: a
    ``crash leader`` step at a moment when nobody leads has nothing to crash. That is a
    real outcome of the run and the report says so, rather than the step silently
    vanishing.
    """

    at: float
    action: str
    target: NodeId | None
    applied: bool
    note: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "at": self.at,
            "action": self.action,
            "target": self.target,
            "applied": self.applied,
            "note": self.note,
        }


@dataclass
class RunResult:
    """Everything one run produced. Judged by ``metrics.py``, not here.

    ``election_timeout_range``/``heartbeat_interval``/``client_timeout`` are carried
    along even though they never change during a run: Phase 7's replay reconstructs an
    ``ExperimentRunner`` from a manifest built off this result, and those three are the
    only constructor knobs a :class:`Scenario` does not itself encode - see
    ``experiments/manifest.py``.
    """

    scenario: Scenario
    run_id: str
    seed: int
    duration: float
    history: History
    events: list[Event]
    live: LiveMetrics
    network: SimulatorStats
    clients: dict[NodeId, ClientMetrics]
    nodes: list[NodeSnapshot]
    faults: list[AppliedFault]
    election_timeout_range: tuple[float, float] = ELECTION_TIMEOUT_RANGE
    heartbeat_interval: float = HEARTBEAT_INTERVAL
    client_timeout: float = CLIENT_TIMEOUT
    persist: bool = False
    """Whether this run built its nodes with a WAL (Phase 8's chaos campaign): a crash
    then truly discards in-memory state and recovery rebuilds only from disk. Recorded
    so a persisted run's manifest knows to reconstruct the same way on replay."""

    @property
    def surviving_log(self) -> list[Any]:
        """The longest committed prefix any node still holds.

        What "the cluster's data" means at the end of a run. Raft guarantees a committed
        entry is on a majority, so the longest committed prefix among the survivors
        contains every entry that was ever acknowledged - if one is missing from here,
        it is genuinely gone.
        """
        alive = [n for n in self.nodes if n.alive] or self.nodes
        return max((n.committed for n in alive), key=len, default=[])


class ExperimentRunner:
    """Runs one :class:`Scenario` and returns what happened."""

    def __init__(
        self,
        scenario: Scenario,
        *,
        seed: int | None = None,
        run_id: str | None = None,
        election_timeout_range: tuple[float, float] = ELECTION_TIMEOUT_RANGE,
        heartbeat_interval: float = HEARTBEAT_INTERVAL,
        client_timeout: float = CLIENT_TIMEOUT,
        wal_dir: str | Path | None = None,
    ) -> None:
        self.scenario = scenario
        self.seed = scenario.seed if seed is None else seed
        self.run_id = run_id or uuid.uuid4().hex[:12]
        self._election_timeout_range = election_timeout_range
        self._heartbeat_interval = heartbeat_interval
        self._client_timeout = client_timeout
        self._wal_dir = Path(wal_dir) if wal_dir is not None else None
        if self._wal_dir is not None:
            self._wal_dir.mkdir(parents=True, exist_ok=True)

        self.clock = VirtualClock()
        self.tracer = Tracer(self.clock)
        self.live = LiveMetrics(self.tracer)
        # Every source of randomness is derived from the one seed, so a run is
        # reproducible end to end - Phase 7 replays it by rebuilding exactly this.
        self.network = SimulatedNetwork(
            clock=self.clock, rng=random.Random(self.seed + 977), tracer=self.tracer
        )
        self.history = History()

        self.nodes: dict[NodeId, RaftNode] = {}
        self.clients: dict[NodeId, KVClient] = {}
        self._channels: list[BusChannel] = []
        self._down: list[NodeId] = []
        self._applied: list[AppliedFault] = []

    # --- setup --------------------------------------------------------------------

    def _build_cluster(self) -> None:
        ids = self.scenario.node_ids
        for index, node_id in enumerate(ids):
            self.nodes[node_id] = self._new_node(node_id, ids, index)

    def _new_node(self, node_id: NodeId, ids: list[NodeId], index: int) -> RaftNode:
        """A node as it would be built fresh - used both at startup and, when this run
        is persistent, to rebuild one from nothing but its WAL on recovery (see
        ``_recover``). The ``rng`` is reseeded by the same ``seed + index`` formula
        every time rather than continuing whatever stream the previous instance was
        partway through, so a rebuild is a pure function of ``(seed, node_id)`` and
        Phase 7 replay reproduces it exactly.
        """
        wal = (
            WriteAheadLog(self._wal_dir / f"{node_id}.wal", node_id=node_id, sync=False)
            if self._wal_dir is not None
            else None
        )
        return RaftNode(
            node_id=node_id,
            peers=ids,
            transport=self.network,
            rng=random.Random(self.seed + index),
            clock=self.clock,
            tracer=self.tracer,
            wal=wal,
            election_timeout_range=self._election_timeout_range,
            heartbeat_interval=self._heartbeat_interval,
        )

    def _build_clients(self) -> list[Workload]:
        ids = self.scenario.node_ids
        workloads: list[Workload] = []

        for index in range(max(1, self.scenario.workload.clients)):
            client_id = f"client{index + 1}"
            channel = BusChannel(
                self.network, client_id, timeout=self._client_timeout, clock=self.clock
            )
            self._channels.append(channel)
            client = KVClient(client_id, ids, channel, policy=RetryPolicy(), clock=self.clock)
            self.clients[client_id] = client
            workloads.append(
                Workload(
                    self.scenario.workload,
                    client,
                    self.history,
                    clock=self.clock,
                    rng=random.Random(self.seed + 5000 + index),
                    index=index,
                )
            )
        return workloads

    # --- the run ------------------------------------------------------------------

    async def run(self) -> RunResult:
        self._build_cluster()
        for node in self.nodes.values():
            node.start()

        # Let the cluster elect a leader before the first client operation. Without
        # this every scenario would spend its first operations measuring startup, which
        # no scenario is about.
        await self._settle()

        workloads = self._build_clients()
        tasks = [asyncio.create_task(w.run(self.scenario.duration)) for w in workloads]

        try:
            await self._drive(tasks)
        finally:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            result = await self._teardown()
        return result

    async def _drive(self, tasks: list[asyncio.Task[None]]) -> None:
        """Advance time to the next thing that is due, until the scenario is over."""
        pending = list(self.scenario.faults)

        while self.clock.now() < self.scenario.duration:
            await self._pump()

            while pending and pending[0].at <= self.clock.now():
                await self._apply(pending.pop(0))
                await self._pump()

            candidates = [self.scenario.duration]
            candidates += [
                when
                for when in (self.clock.next_deadline(), _next_fault(pending))
                if when is not None
            ]
            target = min(candidates)

            if target <= self.clock.now():
                # Nothing is scheduled before the horizon we are already at; step to it
                # so a run with no timers left still reaches its end.
                target = min(self.scenario.duration, self.clock.now() + self._heartbeat_interval)

            await self.clock.advance_to(target)

            # The workload is finished and nothing is left to break; running the clock
            # to the end would only add idle heartbeats to the trace.
            if (
                all(task.done() for task in tasks)
                and not pending
                and self.clock.now() >= self.scenario.duration
            ):
                break

        await self._pump()

    async def _pump(self) -> None:
        """Let the cluster react until the loop goes quiet.

        A clock jump only fires timers; the tasks they wake still have to run, send,
        and be delivered to. Quiet is defined as "no message waiting in any inbox",
        checked a few times over, because a node that has just taken one message off
        its queue may be about to put two more on somebody else's.
        """
        quiet = 0
        for _ in range(PUMP_LIMIT):
            await asyncio.sleep(0)
            quiet = quiet + 1 if self.network.pending() == 0 else 0
            if quiet >= 3:
                return

    async def _settle(self) -> None:
        """Advance until somebody is leading, or the election timeout has had its chance."""
        deadline = self.clock.now() + self._election_timeout_range[1] * 4
        while self.clock.now() < deadline:
            await self._pump()
            if any(node.role is Role.LEADER for node in self.nodes.values()):
                return
            nxt = self.clock.next_deadline()
            await self.clock.advance_to(min(deadline, nxt if nxt is not None else deadline))
        await self._pump()

    # --- faults -------------------------------------------------------------------

    async def _apply(self, step: FaultStep) -> None:
        at = self.clock.now()
        try:
            target, note = await self._perform(step)
        except ScenarioError as exc:
            self._applied.append(AppliedFault(at, step.action, None, False, str(exc)))
            return
        self._applied.append(AppliedFault(at, step.action, target, note == "", note))

    async def _perform(self, step: FaultStep) -> tuple[NodeId | None, str]:
        if step.action == CRASH:
            return await self._crash(step)
        if step.action == RECOVER:
            return self._recover(step)
        if step.action == PARTITION:
            return self._partition(step)
        if step.action == HEAL_PARTITION:
            self.network.heal()
            return None, ""
        if step.action == LATENCY:
            low, high = _latency_range(step.params)
            self.network.set_latency(low, high)
            return None, ""
        if step.action == PACKET_LOSS:
            self.network.set_loss(_loss_rate(step.params))
            return None, ""
        if step.action == RESET_FAULTS:
            self.network.reset_faults()
            return None, ""
        # pragma: no cover - FaultStep validates the action when it is constructed
        raise ScenarioError(f"no runner support for action {step.action!r}")

    async def _crash(self, step: FaultStep) -> tuple[NodeId | None, str]:
        target = self._resolve(step.target)
        if target is None:
            return None, "nothing to crash: no node matched the target"
        if target in self._down:
            return target, f"{target} was already down"

        await self.nodes[target].stop()
        self._down.append(target)
        return target, ""

    def _recover(self, step: FaultStep) -> tuple[NodeId | None, str]:
        """Bring a crashed node back.

        A ``recover`` step is paired with an earlier ``crash`` by the *runner*, not by
        the scenario: a flat ``node_crash target: leader`` with a duration expands into
        crash-then-recover, and by the time the recover runs, "leader" names somebody
        else entirely. What the author meant is "put back the one you took down", so
        that is what happens - oldest outage first.
        """
        if not self._down:
            return None, "nothing to recover: no node is down"

        named = self._resolve(step.target, allow_missing=True)
        target = named if named in self._down else self._down[0]
        self._down.remove(target)

        if self._wal_dir is not None:
            # A persistent run's crash is a real one: this node's in-memory object is
            # discarded here, and the replacement is built by ``_new_node`` from
            # nothing but what its WAL actually persisted - see docs/architecture.md P8.
            ids = self.scenario.node_ids
            self.nodes[target] = self._new_node(target, ids, ids.index(target))

        self.nodes[target].start()
        return target, ""

    def _partition(self, step: FaultStep) -> tuple[NodeId | None, str]:
        raw = step.params.get("groups")
        if not raw:
            return None, "partition step named no groups"

        # Members go through the same resolver a ``target:`` does, so a group may name
        # ``leader`` and mean whoever is leading at the moment the split happens. A
        # scenario about isolating the leader cannot be written with fixed node ids -
        # which node leads is decided by the run, not by the file.
        groups: list[list[NodeId]] = []
        claimed: set[NodeId] = set()
        for group in raw:
            members = [self._resolve(member) for member in group]
            if any(member is None for member in members):
                return None, "partition step named a node that could not be resolved"
            # First group to name a node keeps it. That is what lets a scenario write
            # "[[leader], [1, 2, 3, 4, 5]]" and mean "the leader, alone, against the
            # rest" without having to know which node will be leading.
            groups.append([m for m in members if m is not None and m not in claimed])
            claimed.update(m for m in members if m is not None)

        self.network.partition(*groups)
        return None, ""

    def _resolve(self, target: str | int | None, *, allow_missing: bool = False) -> NodeId | None:
        """Turn a step's target into a node id, resolving ``leader`` as of right now."""
        if target is None:
            return None
        if str(target).lower() == "leader":
            leaders = [
                node
                for node in self.nodes.values()
                if node.role is Role.LEADER and node.node_id not in self._down
            ]
            if not leaders:
                return None
            # A split brain can leave two nodes believing they lead; the one with the
            # higher term is the one whose loss the scenario is actually about.
            return max(leaders, key=lambda node: node.current_term).node_id

        if allow_missing:
            try:
                return self.scenario.resolve_node(target)
            except ScenarioError:
                return None
        return self.scenario.resolve_node(target)

    # --- results ------------------------------------------------------------------

    async def _teardown(self) -> RunResult:
        snapshots = [self._snapshot(node_id) for node_id in self.scenario.node_ids]

        for channel in self._channels:
            channel.close()
        for node_id, node in self.nodes.items():
            if node_id not in self._down:
                await node.stop(crashed=False)

        return RunResult(
            scenario=self.scenario,
            run_id=self.run_id,
            seed=self.seed,
            duration=self.clock.now(),
            history=self.history,
            events=self.tracer.events,
            live=self.live,
            network=self.network.stats,
            clients={cid: client.metrics for cid, client in self.clients.items()},
            nodes=snapshots,
            faults=self._applied,
            election_timeout_range=self._election_timeout_range,
            heartbeat_interval=self._heartbeat_interval,
            client_timeout=self._client_timeout,
            persist=self._wal_dir is not None,
        )

    def _snapshot(self, node_id: NodeId) -> NodeSnapshot:
        node = self.nodes[node_id]
        committed = [
            entry.command
            for index in range(1, node.state.commit_index + 1)
            if (entry := node.state.log.entry_at(index)) is not None and entry.command is not None
        ]
        return NodeSnapshot(
            node_id=node_id,
            alive=node_id not in self._down,
            role=node.role.value,
            term=node.current_term,
            commit_index=node.state.commit_index,
            last_applied=node.state.last_applied,
            log_length=node.state.log.last_index,
            committed=committed,
            state=node.store.snapshot(),
        )


def _next_fault(pending: list[FaultStep]) -> float | None:
    return pending[0].at if pending else None


def _latency_range(params: dict[str, Any]) -> tuple[float, float]:
    """Read a latency setting in any of the units the two documents use."""
    if "min_seconds" in params or "max_seconds" in params:
        return float(params.get("min_seconds", 0.0)), float(params.get("max_seconds", 0.0))

    base = float(params.get("latency_ms", params.get("mean_ms", 0.0)))
    jitter = float(params.get("jitter_ms", 0.0))
    low = max(0.0, base - jitter) / 1000.0
    high = (base + jitter) / 1000.0
    return low, high


def _loss_rate(params: dict[str, Any]) -> float:
    for key in ("rate", "drop_rate", "probability", "loss"):
        if key in params:
            return float(params[key])
    return 0.0


async def run_scenario(scenario: Scenario, **kwargs: Any) -> RunResult:
    """Convenience wrapper: build a runner for ``scenario`` and run it."""
    return await ExperimentRunner(scenario, **kwargs).run()
