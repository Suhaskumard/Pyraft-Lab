"""Day 8: ``LinkConfig`` sampling in isolation, before ``SimulatedNetwork`` uses it."""

from __future__ import annotations

import random

import pytest

from pyraft_lab.network.link import IDEAL_LINK, LinkConfig


def test_ideal_link_has_no_latency_and_no_loss() -> None:
    assert IDEAL_LINK.min_latency == 0.0
    assert IDEAL_LINK.max_latency == 0.0
    assert IDEAL_LINK.loss_probability == 0.0


def test_ideal_link_never_drops() -> None:
    rng = random.Random(0)
    assert all(not IDEAL_LINK.should_drop(rng) for _ in range(1000))


def test_ideal_link_delay_is_always_zero() -> None:
    rng = random.Random(0)
    assert all(IDEAL_LINK.sample_delay(rng) == 0.0 for _ in range(100))


def test_a_fixed_latency_range_samples_a_constant() -> None:
    link = LinkConfig(min_latency=0.05, max_latency=0.05)
    rng = random.Random(0)
    assert all(link.sample_delay(rng) == 0.05 for _ in range(50))


def test_a_latency_range_samples_within_bounds() -> None:
    link = LinkConfig(min_latency=0.01, max_latency=0.05)
    rng = random.Random(0)
    delays = [link.sample_delay(rng) for _ in range(500)]
    assert all(0.01 <= d <= 0.05 for d in delays)
    assert len(set(delays)) > 1  # actually varies, not degenerate


def test_negative_min_latency_is_rejected() -> None:
    with pytest.raises(ValueError, match="min_latency"):
        LinkConfig(min_latency=-0.01)


def test_max_below_min_is_rejected() -> None:
    with pytest.raises(ValueError, match="min_latency"):
        LinkConfig(min_latency=0.05, max_latency=0.01)


def test_loss_probability_out_of_range_is_rejected() -> None:
    with pytest.raises(ValueError, match="loss_probability"):
        LinkConfig(loss_probability=1.5)
    with pytest.raises(ValueError, match="loss_probability"):
        LinkConfig(loss_probability=-0.1)


def test_a_loss_probability_of_one_always_drops() -> None:
    link = LinkConfig(loss_probability=1.0)
    rng = random.Random(0)
    assert all(link.should_drop(rng) for _ in range(100))


def test_a_loss_probability_of_zero_never_drops() -> None:
    link = LinkConfig(loss_probability=0.0)
    rng = random.Random(0)
    assert not any(link.should_drop(rng) for _ in range(1000))


def test_loss_is_seeded_and_reproducible() -> None:
    link = LinkConfig(loss_probability=0.5)
    first = [link.should_drop(random.Random(42)) for _ in range(200)]
    second = [link.should_drop(random.Random(42)) for _ in range(200)]
    assert first == second


def test_loss_at_half_probability_drops_some_and_passes_some() -> None:
    link = LinkConfig(loss_probability=0.5)
    rng = random.Random(42)
    results = [link.should_drop(rng) for _ in range(200)]
    assert any(results)
    assert not all(results)
