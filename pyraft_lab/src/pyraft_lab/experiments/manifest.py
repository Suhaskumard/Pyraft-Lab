"""What a run needs to remember about itself, so it can be run again.

2.0 item 5's list, verbatim: "Every experiment should generate: run_id, random_seed,
scenario, network configuration, fault timeline, workload, event trace." The last of
those - the event trace - is already Phase 4's job (``Tracer.write_jsonl``). Everything
before it is the *inputs* a run was built from, and this module is where they are named
and written down as one file.

Network configuration and the fault timeline turn out not to need separate fields: a
scenario's ``faults:`` list already *is* the fault timeline, and every network setting
(latency, loss) that is not the ideal default arrives as a timeline step too
(``LATENCY``/``PACKET_LOSS`` actions) - see ``experiments/scenario.py``. So
``Scenario.to_dict()`` already carries both, and a manifest is the scenario plus the
handful of constructor knobs ``ExperimentRunner`` does not read from the scenario itself.

Nothing here runs anything. A manifest is a value, like ``Scenario`` - built after a run
finishes, read back before one is replayed, and comparable independent of either.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

MANIFEST_VERSION = 1


class ManifestError(Exception):
    """A manifest file could not be read as one."""


@dataclass(frozen=True)
class RunManifest:
    """Everything about a run's *inputs*, independent of what it produced.

    ``created_at`` is wall-clock and purely informational - a human reading the file
    later, never anything a replay computes from. Every field that actually has to
    reproduce the run is deterministic input: the seed, the scenario, and the three
    constructor knobs (``ExperimentRunner``'s timeout/heartbeat/client-timeout) that a
    scenario file does not itself carry.
    """

    run_id: str
    seed: int
    scenario: dict[str, Any]
    election_timeout_range: tuple[float, float]
    heartbeat_interval: float
    client_timeout: float
    duration: float
    created_at: str = field(default_factory=lambda: datetime.now(UTC).isoformat())
    scenario_source: str | None = None
    persist: bool = False
    """Whether the run was built with a WAL per node (Phase 8's chaos campaign), so a
    crash truly discarded state and recovery rebuilt from disk alone. A replay has to
    reconstruct the same way, or it would "pass" by skipping the exact mechanism a
    failing chaos trial exists to exercise."""
    version: int = MANIFEST_VERSION

    @property
    def scenario_name(self) -> str:
        return str(self.scenario.get("name", "?"))

    def to_dict(self) -> dict[str, Any]:
        return {
            "version": self.version,
            "run_id": self.run_id,
            "seed": self.seed,
            "created_at": self.created_at,
            "scenario": self.scenario,
            "scenario_source": self.scenario_source,
            "election_timeout_range": list(self.election_timeout_range),
            "heartbeat_interval": self.heartbeat_interval,
            "client_timeout": self.client_timeout,
            "duration": self.duration,
            "persist": self.persist,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> RunManifest:
        try:
            version = int(data.get("version", MANIFEST_VERSION))
            if version != MANIFEST_VERSION:
                raise ManifestError(
                    f"manifest format version {version}, expected {MANIFEST_VERSION}"
                )
            low, high = data["election_timeout_range"]
            return cls(
                run_id=str(data["run_id"]),
                seed=int(data["seed"]),
                scenario=dict(data["scenario"]),
                election_timeout_range=(float(low), float(high)),
                heartbeat_interval=float(data["heartbeat_interval"]),
                client_timeout=float(data["client_timeout"]),
                duration=float(data["duration"]),
                created_at=str(data.get("created_at", "")),
                scenario_source=data.get("scenario_source"),
                persist=bool(data.get("persist", False)),
                version=version,
            )
        except ManifestError:
            raise
        except (KeyError, TypeError, ValueError) as exc:
            raise ManifestError(f"unreadable manifest: {exc}") from exc

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), indent=2, sort_keys=True)

    @classmethod
    def from_json(cls, text: str) -> RunManifest:
        try:
            data = json.loads(text)
        except json.JSONDecodeError as exc:
            raise ManifestError(f"manifest is not valid JSON: {exc}") from exc
        if not isinstance(data, dict):
            raise ManifestError(f"manifest must be an object, got {type(data).__name__}")
        return cls.from_dict(data)

    def save(self, path: str | Path) -> Path:
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(self.to_json(), encoding="utf-8")
        return path

    @classmethod
    def load(cls, path: str | Path) -> RunManifest:
        path = Path(path)
        if not path.exists():
            raise ManifestError(f"no manifest at {path}")
        return cls.from_json(path.read_text(encoding="utf-8"))
