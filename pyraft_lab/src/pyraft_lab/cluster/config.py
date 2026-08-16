"""``ClusterConfig``: the value a live cluster is built from.

The same shape ``experiments/manifest.py``'s ``RunManifest`` already uses (a frozen
dataclass with ``to_dict``/``from_dict``/``save``/``load``), except this one is meant
to be hand-authored and hand-edited - ``pyraft-lab init`` writes a starter copy, a
person can open it in an editor, and ``cluster start --config`` reads it back - so it
round-trips through YAML (``experiments/scenario.py``'s dialect) rather than JSON.

Real (not test-fast) election/heartbeat defaults are deliberate: this describes a
*live* cluster a human interacts with in real time over a ``WallClock``, not a fixture
a test wants to run in milliseconds.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

from pyraft_lab import NodeId
from pyraft_lab.raft.election import ELECTION_TIMEOUT_RANGE, HEARTBEAT_INTERVAL

DEFAULT_CLIENT_TIMEOUT = 0.2
"""Seconds. Matches ``experiments/runner.py``'s ``CLIENT_TIMEOUT`` default."""


class ClusterConfigError(Exception):
    """A cluster config file could not be read as one."""


@dataclass(frozen=True)
class ClusterConfig:
    """How many nodes, where (if anywhere) they persist, and how fast their timers
    run. Everything ``ClusterManager`` needs to build a cluster and nothing it
    produces - the same "value, not a runner" split ``Scenario`` and ``RunManifest``
    already use.
    """

    nodes: int = 3
    data_dir: Path | None = None
    """``None`` means an ephemeral, in-memory cluster - no WAL, nothing survives a
    node restart. Set to persist each node's state under ``data_dir/<node_id>.wal``.
    """
    election_timeout_range: tuple[float, float] = ELECTION_TIMEOUT_RANGE
    heartbeat_interval: float = HEARTBEAT_INTERVAL
    client_timeout: float = DEFAULT_CLIENT_TIMEOUT
    seed: int = 0

    def __post_init__(self) -> None:
        """Reject configs that cannot describe a working cluster.

        Only structural impossibilities are refused here, not merely unwise timers. A
        heartbeat at or above the minimum election timeout is the notable case left
        legal: it produces a cluster in permanent election churn, which is a thing this
        laboratory exists to let someone watch on purpose. The UI warns about it
        instead, and ``heartbeat_is_too_slow`` below is what it asks.
        """
        if self.nodes < 1:
            raise ClusterConfigError(f"a cluster needs at least one node, got {self.nodes}")

        low, high = self.election_timeout_range
        if low <= 0 or high <= 0:
            raise ClusterConfigError(
                f"election timeouts must be positive, got ({low}, {high}) seconds"
            )
        if low >= high:
            raise ClusterConfigError(
                f"the minimum election timeout must be below the maximum, got ({low}, {high}) "
                "seconds - a node picks its timeout at random from inside that range"
            )
        if self.heartbeat_interval <= 0:
            raise ClusterConfigError(
                f"the heartbeat interval must be positive, got {self.heartbeat_interval} seconds"
            )
        if self.client_timeout <= 0:
            raise ClusterConfigError(
                f"the client timeout must be positive, got {self.client_timeout} seconds"
            )

    @property
    def heartbeat_is_too_slow(self) -> bool:
        """Whether followers will time out against a healthy leader.

        A leader proves it is alive only by heartbeating, so a heartbeat that is not
        comfortably inside the shortest election timeout guarantees some follower
        campaigns against a leader that is doing its job. Legal, but never what someone
        configuring a cluster for real work intends.
        """
        return self.heartbeat_interval >= self.election_timeout_range[0]

    @property
    def node_ids(self) -> list[NodeId]:
        """Generated, not configured - the same reasoning ``Scenario.node_ids`` uses:
        a config says how many nodes it wants, and the ids follow."""
        return [f"n{i}" for i in range(1, self.nodes + 1)]

    def to_dict(self) -> dict[str, Any]:
        return {
            "nodes": self.nodes,
            "data_dir": str(self.data_dir) if self.data_dir is not None else None,
            "election_timeout_range": list(self.election_timeout_range),
            "heartbeat_interval": self.heartbeat_interval,
            "client_timeout": self.client_timeout,
            "seed": self.seed,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> ClusterConfig:
        try:
            low, high = data.get("election_timeout_range", ELECTION_TIMEOUT_RANGE)
            raw_dir = data.get("data_dir")
            return cls(
                nodes=int(data.get("nodes", 3)),
                data_dir=Path(raw_dir) if raw_dir else None,
                election_timeout_range=(float(low), float(high)),
                heartbeat_interval=float(data.get("heartbeat_interval", HEARTBEAT_INTERVAL)),
                client_timeout=float(data.get("client_timeout", DEFAULT_CLIENT_TIMEOUT)),
                seed=int(data.get("seed", 0)),
            )
        except ClusterConfigError:
            raise
        except (TypeError, ValueError) as exc:
            raise ClusterConfigError(f"unreadable cluster config: {exc}") from exc

    def to_yaml(self) -> str:
        return yaml.safe_dump(self.to_dict(), sort_keys=False, default_flow_style=False)

    @classmethod
    def from_yaml(cls, text: str) -> ClusterConfig:
        try:
            data = yaml.safe_load(text)
        except yaml.YAMLError as exc:
            raise ClusterConfigError(f"could not parse YAML: {exc}") from exc
        if not isinstance(data, dict):
            raise ClusterConfigError(
                f"a cluster config must be a mapping, got {type(data).__name__}"
            )
        return cls.from_dict(data)

    def save(self, path: str | Path) -> Path:
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(self.to_yaml(), encoding="utf-8")
        return path

    @classmethod
    def load(cls, path: str | Path) -> ClusterConfig:
        path = Path(path)
        if not path.exists():
            raise ClusterConfigError(f"no cluster config at {path}")
        return cls.from_yaml(path.read_text(encoding="utf-8"))
