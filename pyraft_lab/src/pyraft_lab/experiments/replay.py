"""Deterministic replay (2.0 item 5): persist a run, then reproduce it from what was
persisted.

Persisting is the easy half: :func:`persist_run` writes a :class:`RunManifest
<pyraft_lab.experiments.manifest.RunManifest>` alongside the event trace, the client
history and the metrics report Phase 5 already knows how to compute - one directory per
run, everything a person or a script needs to look at what happened.

Replay is the half that is actually a claim: :func:`replay_run` reconstructs a fresh
``ExperimentRunner`` from nothing but the manifest, runs it, and checks the new event
trace against the one that was recorded the first time - event for event, in order,
including the ``seq`` that makes same-instant events comparable at all (docs/
architecture.md P4). This works at all only because Phase 5's determinism was already
real: every random source in ``ExperimentRunner`` derives from one seed, and time only
ever moves because the runner advances a ``VirtualClock`` - see docs/architecture.md's
Phase 5 note on why the laboratory needed that in the first place. Replay does not add
determinism; it is the thing that goes looking for proof the determinism holds.

A mismatch is not treated as an error to raise past the caller. It is the answer to the
question "does this reproduce", and a ``ReplayReport`` says so precisely: not just
*that* the traces diverge, but the first event where they do, printed on both sides.
"""

from __future__ import annotations

import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from pyraft_lab.experiments.manifest import RunManifest
from pyraft_lab.experiments.metrics import report, write_report
from pyraft_lab.experiments.runner import ExperimentRunner, RunResult
from pyraft_lab.experiments.scenario import Scenario
from pyraft_lab.observability.events import Event, read_jsonl, write_jsonl

MANIFEST_FILE = "manifest.json"
EVENTS_FILE = "events.jsonl"
HISTORY_FILE = "history.json"
REPORT_FILE = "report.json"


class ReplayError(Exception):
    """A run directory could not be replayed as one."""


# --- persisting --------------------------------------------------------------------


def manifest_of(result: RunResult) -> RunManifest:
    """The manifest that describes how ``result`` was produced.

    Built from the result rather than the runner: everything a replay needs to
    reconstruct the run is either the scenario (which already carries the fault
    timeline, the network configuration and the workload - see the module docstring)
    or one of the three constructor knobs a scenario file does not itself encode.
    """
    scenario = result.scenario
    return RunManifest(
        run_id=result.run_id,
        seed=result.seed,
        scenario=scenario.to_dict(),
        election_timeout_range=result.election_timeout_range,
        heartbeat_interval=result.heartbeat_interval,
        client_timeout=result.client_timeout,
        duration=result.duration,
        scenario_source=str(scenario.source) if scenario.source else None,
        persist=result.persist,
    )


def persist_run(result: RunResult, results_dir: str | Path) -> Path:
    """Write everything Phase 7 needs to replay ``result``, plus what Phase 5 already
    knows how to compute about it.

    One directory per run, named by ``run_id`` - manifests are addressed by run, not by
    scenario, since the same scenario run twice (different seeds, or the same seed
    replayed) produces two runs worth keeping side by side.
    """
    run_dir = Path(results_dir) / result.run_id
    run_dir.mkdir(parents=True, exist_ok=True)

    manifest_of(result).save(run_dir / MANIFEST_FILE)
    write_jsonl(run_dir / EVENTS_FILE, result.events)
    result.history.write_json(run_dir / HISTORY_FILE)
    write_report(report(result), run_dir / REPORT_FILE)
    return run_dir


def load_manifest(results_dir: str | Path, run_id: str) -> RunManifest:
    return RunManifest.load(Path(results_dir) / run_id / MANIFEST_FILE)


def load_events(results_dir: str | Path, run_id: str) -> list[Event]:
    return read_jsonl(Path(results_dir) / run_id / EVENTS_FILE)


# --- replaying ---------------------------------------------------------------------


@dataclass(frozen=True)
class Divergence:
    """Where two traces stopped agreeing."""

    index: int
    original: dict[str, Any] | None
    "``None`` when the original trace ended here and the replay kept going."
    replayed: dict[str, Any] | None
    "``None`` when the replay ended here and the original trace kept going."

    def describe(self) -> str:
        lines = [f"first divergence at event #{self.index}:"]
        lines.append(f"  original:  {self.original if self.original else '(no event)'}")
        lines.append(f"  replayed:  {self.replayed if self.replayed else '(no event)'}")
        return "\n".join(lines)


@dataclass(frozen=True)
class ReplayReport:
    """Does the trace this run just produced match the one that was recorded?"""

    run_id: str
    matched: bool
    original_event_count: int
    replayed_event_count: int
    divergence: Divergence | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "run_id": self.run_id,
            "matched": self.matched,
            "original_event_count": self.original_event_count,
            "replayed_event_count": self.replayed_event_count,
            "divergence": (
                {
                    "index": self.divergence.index,
                    "original": self.divergence.original,
                    "replayed": self.divergence.replayed,
                }
                if self.divergence is not None
                else None
            ),
        }

    def explain(self) -> str:
        if self.matched:
            return (
                f"Replay {self.run_id}: MATCH ({self.original_event_count} events, bit-identical)"
            )
        lines = [
            f"Replay {self.run_id}: MISMATCH",
            f"  original events: {self.original_event_count}",
            f"  replayed events: {self.replayed_event_count}",
        ]
        if self.divergence is not None:
            lines += [f"  {line}" for line in self.divergence.describe().splitlines()]
        return "\n".join(lines)


def _compare(run_id: str, original: list[Event], replayed: list[Event]) -> ReplayReport:
    """Event-for-event comparison, in order.

    ``Event`` is a frozen dataclass, so ``==`` already compares every field including
    ``seq`` - which is exactly what has to match, since under a ``VirtualClock`` many
    events share one timestamp and ``seq`` is the only thing that makes their order
    total (docs/architecture.md P4). A replay that reordered two same-instant events
    would not be a replay, and this comparison is what would catch it.
    """
    shorter = min(len(original), len(replayed))
    divergence: Divergence | None = None

    for index in range(shorter):
        if original[index] != replayed[index]:
            divergence = Divergence(
                index=index,
                original=original[index].to_dict(),
                replayed=replayed[index].to_dict(),
            )
            break

    if divergence is None and len(original) != len(replayed):
        divergence = Divergence(
            index=shorter,
            original=original[shorter].to_dict() if shorter < len(original) else None,
            replayed=replayed[shorter].to_dict() if shorter < len(replayed) else None,
        )

    return ReplayReport(
        run_id=run_id,
        matched=divergence is None,
        original_event_count=len(original),
        replayed_event_count=len(replayed),
        divergence=divergence,
    )


async def replay_run(results_dir: str | Path, run_id: str) -> tuple[ReplayReport, RunResult]:
    """Reconstruct ``run_id`` from its manifest, run it again, and compare traces.

    Returns the comparison and the fresh :class:`RunResult`, so a caller that wants more
    than the trace comparison - the reproduced report, the reproduced history - has it
    without a second run. Raises :class:`ReplayError` if there is nothing to replay.
    """
    results_dir = Path(results_dir)
    run_dir = results_dir / run_id
    if not run_dir.exists():
        raise ReplayError(f"no run {run_id!r} in {results_dir}")

    manifest = load_manifest(results_dir, run_id)
    original_events = load_events(results_dir, run_id)

    scenario = Scenario.from_dict(manifest.scenario)

    if manifest.persist:
        # A persisted run's crashes really discarded state and recovered from a WAL
        # (Phase 8's chaos campaign); replaying it in cheap in-memory mode would skip
        # exactly the mechanism a failing trial exists to exercise, and could "match"
        # for the wrong reason. A fresh temp directory reproduces the same rebuild
        # deterministically - the WAL's *content*, not the directory, is what has to
        # match, and content is rederived from the identical seed and fault timeline.
        with tempfile.TemporaryDirectory(prefix=f"replay-{run_id}-wal-") as wal_dir:
            runner = ExperimentRunner(
                scenario,
                seed=manifest.seed,
                run_id=manifest.run_id,
                election_timeout_range=manifest.election_timeout_range,
                heartbeat_interval=manifest.heartbeat_interval,
                client_timeout=manifest.client_timeout,
                wal_dir=wal_dir,
            )
            result = await runner.run()
    else:
        runner = ExperimentRunner(
            scenario,
            seed=manifest.seed,
            run_id=manifest.run_id,
            election_timeout_range=manifest.election_timeout_range,
            heartbeat_interval=manifest.heartbeat_interval,
            client_timeout=manifest.client_timeout,
        )
        result = await runner.run()

    comparison = _compare(run_id, original_events, result.events)
    return comparison, result
