"""Long-running work, run off the live cluster's event loop.

Every campaign in this project - a scenario run, stress, chaos, benchmark, a replay -
drives an ``ExperimentRunner`` over a ``VirtualClock``, which advances as fast as the
CPU allows and never yields to real time. Awaiting one on the API's own event loop
would therefore starve the *live* cluster sharing that loop: its heartbeat and election
timers run on a ``WallClock`` and need the loop back on a 50 ms cadence, so a
thirty-second benchmark would look to it like a thirty-second freeze and depose its
leader repeatedly.

So each job gets a worker thread with an event loop of its own (``asyncio.run`` inside
``asyncio.to_thread``). The two loops share nothing: a virtual-clock run holds no
reference to the live cluster, and the live cluster's nodes are never touched from the
worker. What crosses back is a plain progress record, guarded by a lock.
"""

from __future__ import annotations

import asyncio
import threading
import traceback
import uuid
from collections.abc import Callable, Coroutine
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

MAX_JOBS = 50
"""How many finished jobs to keep. Old ones are dropped oldest-first."""

MAX_LINES = 400
"""Per job. A campaign narrates one line per trial, and a long one should not be able
to grow the API's memory without bound."""


def utcnow() -> str:
    return datetime.now(UTC).isoformat(timespec="milliseconds")


class JobCancelled(Exception):
    """Raised inside a job's progress callback once cancellation is requested.

    Campaigns are cancelled *between* trials rather than mid-trial: a trial holds an
    in-flight virtual-clock run whose teardown is not interruptible, and abandoning one
    halfway would leave a half-written run directory behind.
    """


@dataclass
class Job:
    """One unit of background work, and everything the UI shows about it."""

    id: str
    kind: str
    params: dict[str, Any]
    status: str = "running"
    """``running`` | ``succeeded`` | ``failed`` | ``cancelled``."""
    completed: int = 0
    total: int = 0
    label: str = ""
    lines: list[str] = field(default_factory=list)
    result: Any = None
    error: str | None = None
    created_at: str = field(default_factory=utcnow)
    finished_at: str | None = None

    _lock: threading.Lock = field(default_factory=threading.Lock, repr=False)
    _cancel: threading.Event = field(default_factory=threading.Event, repr=False)
    _notify: Callable[[Job], None] | None = field(default=None, repr=False)

    # --- called from the worker thread ------------------------------------------------

    def progress(self, completed: int, total: int, label: str = "") -> None:
        """Record how far along this job is, and abort it if it has been cancelled."""
        with self._lock:
            self.completed = completed
            self.total = total
            if label:
                self.label = label
        self._changed()
        self.raise_if_cancelled()

    def log(self, line: str) -> None:
        with self._lock:
            self.lines.append(line)
            if len(self.lines) > MAX_LINES:
                del self.lines[: len(self.lines) - MAX_LINES]
        self._changed()

    def raise_if_cancelled(self) -> None:
        if self._cancel.is_set():
            raise JobCancelled(f"{self.kind} job {self.id} was cancelled")

    # --- called from the event loop ---------------------------------------------------

    def cancel(self) -> None:
        self._cancel.set()

    @property
    def cancelling(self) -> bool:
        return self._cancel.is_set() and self.status == "running"

    def to_dict(self) -> dict[str, Any]:
        with self._lock:
            return {
                "id": self.id,
                "kind": self.kind,
                "params": self.params,
                "status": self.status,
                "completed": self.completed,
                "total": self.total,
                "label": self.label,
                "lines": list(self.lines),
                "result": self.result,
                "error": self.error,
                "created_at": self.created_at,
                "finished_at": self.finished_at,
                "cancelling": self._cancel.is_set() and self.status == "running",
            }

    def _changed(self) -> None:
        if self._notify is not None:
            self._notify(self)


class JobRegistry:
    """Starts jobs, keeps the recent ones, and reports every change once."""

    def __init__(self, on_change: Callable[[Job], None] | None = None) -> None:
        self._jobs: dict[str, Job] = {}
        self._tasks: dict[str, asyncio.Task[None]] = {}
        self._on_change = on_change
        self._loop: asyncio.AbstractEventLoop | None = None

    def bind(self, loop: asyncio.AbstractEventLoop) -> None:
        """Remember the loop a worker thread must hop back to in order to notify."""
        self._loop = loop

    def start(
        self,
        kind: str,
        params: dict[str, Any],
        work: Callable[[Job], Coroutine[Any, Any, Any]],
    ) -> Job:
        job = Job(id=f"{kind}-{uuid.uuid4().hex[:8]}", kind=kind, params=params)
        job._notify = self._notify
        self._jobs[job.id] = job
        self._prune()
        self._tasks[job.id] = asyncio.create_task(self._run(job, work))
        self._notify(job)
        return job

    async def _run(self, job: Job, work: Callable[[Job], Coroutine[Any, Any, Any]]) -> None:
        try:
            result = await asyncio.to_thread(lambda: asyncio.run(work(job)))
        except JobCancelled:
            job.status = "cancelled"
            job.error = "cancelled"
        except Exception as exc:
            job.status = "failed"
            job.error = f"{type(exc).__name__}: {exc}"
            job.log(traceback.format_exc().strip().splitlines()[-1])
        else:
            job.status = "succeeded"
            job.result = result
        finally:
            job.finished_at = utcnow()
            self._tasks.pop(job.id, None)
            self._notify(job)

    def get(self, job_id: str) -> Job | None:
        return self._jobs.get(job_id)

    def all(self) -> list[Job]:
        return sorted(self._jobs.values(), key=lambda j: j.created_at, reverse=True)

    def latest(self, kind: str) -> Job | None:
        return next((job for job in self.all() if job.kind == kind), None)

    def running(self) -> list[Job]:
        return [job for job in self._jobs.values() if job.status == "running"]

    async def shutdown(self) -> None:
        """Ask every running job to stop, then wait briefly for the threads to notice.

        A worker cannot be killed - it owns a thread - so this only cancels between
        trials. A job still mid-trial at the end of the wait is left to finish into a
        process that is going away anyway.
        """
        for job in self.running():
            job.cancel()
        tasks = list(self._tasks.values())
        if tasks:
            await asyncio.wait(tasks, timeout=5.0)

    def _prune(self) -> None:
        finished = [j for j in self.all() if j.status != "running"]
        for job in finished[MAX_JOBS:]:
            self._jobs.pop(job.id, None)

    def _notify(self, job: Job) -> None:
        if self._on_change is None:
            return
        loop = self._loop
        if loop is None or not loop.is_running():
            return
        # Worker threads call this too, so the hop is not optional - and scheduling one
        # from the loop's own thread is legal, so this needs no "am I the loop" branch.
        loop.call_soon_threadsafe(self._on_change, job)
