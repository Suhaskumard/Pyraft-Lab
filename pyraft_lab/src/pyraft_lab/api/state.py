"""``LabState``: the one live cluster this process owns, and everything around it.

The same one-process model ``cluster start`` chose (docs/architecture.md P9), moved
behind HTTP: there is still exactly one cluster, it still lives for as long as the
process, and there is still no daemon behind it. What changes is only who drives it -
an HTTP client rather than a terminal - so ``POST /api/cluster/start`` and
``pyraft-lab cluster start`` build the identical ``ClusterManager`` from the identical
``ClusterConfig``.

The broadcast side is what a terminal did not need. Trace events arrive from node code
on this loop at whatever rate the cluster is running (a 5-node cluster heartbeating at
20 Hz emits ~80 events a second before any client traffic), so they are batched and
flushed on a timer rather than written to every socket as they happen.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
from collections.abc import Iterator
from dataclasses import replace
from pathlib import Path
from typing import Any

from pyraft_lab.api.jobs import Job, JobRegistry
from pyraft_lab.api.serializers import cluster_status, event_row
from pyraft_lab.cluster.config import ClusterConfig
from pyraft_lab.cluster.manager import ClusterManager
from pyraft_lab.observability.events import Event

FLUSH_INTERVAL = 0.1
"""Seconds between event flushes to connected sockets."""

STATUS_INTERVAL = 0.5
"""Seconds between status pushes. Slower than the event flush: a header does not need
to move at trace rate, and re-serializing the whole cluster is not free."""

QUEUE_LIMIT = 64
"""Batches, not events. A socket this far behind is dropped from - the UI re-reads
``GET /api/cluster`` on reconnect, so a slow client loses tail events, not its view."""


class LabError(Exception):
    """An API-level operation could not be carried out."""


class LabState:
    """Owns the live cluster, the workspace directories, and the job registry."""

    def __init__(self, workspace: Path) -> None:
        self.workspace = workspace
        self.config_path = workspace / "cluster.yaml"
        self.scenarios_dir = workspace / "scenarios"
        self.results_dir = workspace / "results"
        self.data_dir = workspace / "data"

        self.manager: ClusterManager | None = None
        self.started_at: float | None = None
        self.last_error: str | None = None

        self.jobs = JobRegistry(on_change=self._on_job_change)
        self._subscribers: set[asyncio.Queue[str]] = set()
        self._pending: list[dict[str, Any]] = []
        self._pumps: list[asyncio.Task[None]] = []
        self._lock = asyncio.Lock()

    # --- lifecycle ---------------------------------------------------------------------

    async def startup(self) -> None:
        loop = asyncio.get_running_loop()
        self.jobs.bind(loop)
        self.scenarios_dir.mkdir(parents=True, exist_ok=True)
        self.results_dir.mkdir(parents=True, exist_ok=True)
        self._pumps = [
            asyncio.create_task(self._flush_events()),
            asyncio.create_task(self._push_status()),
        ]

    async def shutdown(self) -> None:
        for pump in self._pumps:
            pump.cancel()
        for pump in self._pumps:
            with contextlib.suppress(asyncio.CancelledError):
                await pump
        self._pumps = []
        await self.jobs.shutdown()
        await self.stop_cluster()

    # --- the cluster -------------------------------------------------------------------

    def require_cluster(self) -> ClusterManager:
        if self.manager is None:
            raise LabError("no cluster is running - start one first")
        return self.manager

    def base_config(self) -> ClusterConfig:
        """The saved ``cluster.yaml`` if there is one, otherwise the built-in default -
        exactly what ``cluster start --config`` resolves."""
        if self.config_path.exists():
            return ClusterConfig.load(self.config_path)
        return ClusterConfig()

    async def start_cluster(
        self,
        *,
        nodes: int | None = None,
        seed: int | None = None,
        persistent: bool | None = None,
        election_timeout_range: tuple[float, float] | None = None,
        heartbeat_interval: float | None = None,
        client_timeout: float | None = None,
    ) -> ClusterManager:
        async with self._lock:
            if self.manager is not None:
                raise LabError("a cluster is already running - stop it first")

            base = self.base_config()
            overrides: dict[str, Any] = {}
            if nodes is not None:
                overrides["nodes"] = nodes
            if seed is not None:
                overrides["seed"] = seed
            if persistent is not None:
                overrides["data_dir"] = self.data_dir if persistent else None
            if election_timeout_range is not None:
                overrides["election_timeout_range"] = election_timeout_range
            if heartbeat_interval is not None:
                overrides["heartbeat_interval"] = heartbeat_interval
            if client_timeout is not None:
                overrides["client_timeout"] = client_timeout

            config = replace(base, **overrides) if overrides else base
            manager = ClusterManager(config)
            manager.tracer.subscribe(self._on_event)
            try:
                await manager.start()
            except Exception:
                # A cluster that never elected a leader still registered its nodes on
                # the network; leaving it half-built would make the next start fail on
                # a duplicate id it cannot see.
                with contextlib.suppress(Exception):
                    await manager.stop()
                raise

            self.manager = manager
            self.started_at = manager.clock.now()
            self.broadcast("cluster", {"running": True, "config": config.to_dict()})
            return manager

    async def stop_cluster(self) -> None:
        async with self._lock:
            if self.manager is None:
                return
            manager, self.manager = self.manager, None
            self.started_at = None
            await manager.stop()
            self.broadcast("cluster", {"running": False})

    def status_payload(self) -> dict[str, Any]:
        if self.manager is None:
            return {"running": False, "config": self.base_config().to_dict()}
        return {
            "running": True,
            "config": self.manager.config.to_dict(),
            "status": cluster_status(self.manager),
        }

    # --- broadcasting -------------------------------------------------------------------

    @contextlib.contextmanager
    def subscribe(self) -> Iterator[asyncio.Queue[str]]:
        queue: asyncio.Queue[str] = asyncio.Queue(maxsize=QUEUE_LIMIT)
        self._subscribers.add(queue)
        try:
            yield queue
        finally:
            self._subscribers.discard(queue)

    def broadcast(self, channel: str, payload: Any) -> None:
        if not self._subscribers:
            return
        message = json.dumps({"channel": channel, "payload": payload})
        for queue in list(self._subscribers):
            try:
                queue.put_nowait(message)
            except asyncio.QueueFull:
                self._subscribers.discard(queue)

    def _on_event(self, event: Event) -> None:
        """A tracer subscription. Buffers only - the flush task does the writing."""
        self._pending.append(event_row(event))

    def _on_job_change(self, job: Job) -> None:
        self.broadcast("job", job.to_dict())

    async def _flush_events(self) -> None:
        while True:
            await asyncio.sleep(FLUSH_INTERVAL)
            if not self._pending:
                continue
            batch, self._pending = self._pending, []
            self.broadcast("events", batch)

    async def _push_status(self) -> None:
        while True:
            await asyncio.sleep(STATUS_INTERVAL)
            if self.manager is not None and self._subscribers:
                self.broadcast("status", cluster_status(self.manager))
