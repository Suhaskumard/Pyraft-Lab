"""Per-directed-link fault characteristics: how long a message takes, and whether it
arrives at all.

Deliberately just data plus two sampling methods - :mod:`pyraft_lab.network.faults`
is what turns these into named, composable fault descriptions, and
:mod:`pyraft_lab.network.simulator` is what applies them to real traffic.
"""

from __future__ import annotations

import random
from dataclasses import dataclass


@dataclass(frozen=True)
class LinkConfig:
    """One direction of one link. The default is an ideal link: instant, lossless."""

    min_latency: float = 0.0
    max_latency: float = 0.0
    loss_probability: float = 0.0

    def __post_init__(self) -> None:
        if self.min_latency < 0 or self.max_latency < self.min_latency:
            raise ValueError("expected 0 <= min_latency <= max_latency")
        if not 0.0 <= self.loss_probability <= 1.0:
            raise ValueError("loss_probability must be within [0, 1]")

    def sample_delay(self, rng: random.Random) -> float:
        """A delivery delay drawn from ``[min_latency, max_latency]``."""
        if self.max_latency == self.min_latency:
            return self.min_latency
        return rng.uniform(self.min_latency, self.max_latency)

    def should_drop(self, rng: random.Random) -> bool:
        """Whether one message on this link is lost, independently of any other."""
        return self.loss_probability > 0.0 and rng.random() < self.loss_probability


IDEAL_LINK = LinkConfig()
"""No latency, no jitter, no loss - what :class:`~pyraft_lab.network.bus.InMemoryBus`
implicitly is, and what every link starts as before a fault is injected."""
