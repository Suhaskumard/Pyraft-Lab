"""Phase 7, 2.0 item 5: persist a run, then prove it reproduces.

Two things are under test, and the second is the one that matters:

1. a manifest round-trips (``RunManifest``, ``persist_run``'s files land where
   ``replay_run`` expects them);
2. replaying a persisted run produces a *bit-identical* event trace - not "close",
   not "the same shape" - because that identity is the entire claim 2.0 item 5 makes.
   The determinism this leans on (one seed feeds every random source, time only moves
   because the runner advances a ``VirtualClock``) was already Phase 5's job; what is
   new here is proving it by actually doing the replay and diffing the trace, rather
   than asserting it by argument.

Scenarios here are built directly rather than loaded from ``scenarios/*.yaml`` and kept
short, so the suite stays fast; nothing about replay depends on scenario length.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from pyraft_lab.experiments.manifest import ManifestError, RunManifest
from pyraft_lab.experiments.replay import (
    EVENTS_FILE,
    HISTORY_FILE,
    MANIFEST_FILE,
    REPORT_FILE,
    ReplayError,
    load_events,
    load_manifest,
    manifest_of,
    persist_run,
    replay_run,
)
from pyraft_lab.experiments.runner import ExperimentRunner
from pyraft_lab.experiments.scenario import Scenario
from pyraft_lab.observability.events import Event, EventKind


def a_scenario(**overrides: object) -> Scenario:
    data = {
        "name": "replay-smoke",
        "duration": 3.0,
        "nodes": 3,
        "seed": 7,
        "faults": [{"at": 1.0, "action": "crash", "target": "leader", "duration": 0.5}],
        "workload": {
            "type": "put_get_mix",
            "puts_per_sec": 20,
            "get_ratio": 0.5,
            "keys": 3,
            "clients": 2,
        },
    }
    data.update(overrides)
    return Scenario.from_dict(data)


# --- the manifest --------------------------------------------------------------------


def test_a_manifest_round_trips_through_json() -> None:
    manifest = RunManifest(
        run_id="abc123",
        seed=7,
        scenario=a_scenario().to_dict(),
        election_timeout_range=(0.05, 0.1),
        heartbeat_interval=0.02,
        client_timeout=0.2,
        duration=3.0,
        scenario_source="scenarios/replay-smoke.yaml",
    )

    restored = RunManifest.from_dict(manifest.to_dict())

    assert restored == manifest
    assert restored.scenario_name == "replay-smoke"


def test_a_manifest_saves_and_loads_from_disk(tmp_path: Path) -> None:
    manifest = RunManifest(
        run_id="abc123",
        seed=7,
        scenario=a_scenario().to_dict(),
        election_timeout_range=(0.05, 0.1),
        heartbeat_interval=0.02,
        client_timeout=0.2,
        duration=3.0,
    )
    path = manifest.save(tmp_path / "sub" / "manifest.json")

    assert RunManifest.load(path) == manifest


def test_loading_a_manifest_that_is_not_there_says_so(tmp_path: Path) -> None:
    with pytest.raises(ManifestError, match="no manifest at"):
        RunManifest.load(tmp_path / "nope.json")


def test_a_manifest_from_a_future_format_version_is_refused() -> None:
    manifest = RunManifest(
        run_id="x",
        seed=1,
        scenario=a_scenario().to_dict(),
        election_timeout_range=(0.05, 0.1),
        heartbeat_interval=0.02,
        client_timeout=0.2,
        duration=1.0,
    )
    payload = manifest.to_dict()
    payload["version"] = 99

    with pytest.raises(ManifestError, match="format version 99"):
        RunManifest.from_dict(payload)


def test_a_malformed_manifest_is_refused_cleanly() -> None:
    with pytest.raises(ManifestError, match="unreadable manifest"):
        RunManifest.from_dict({"run_id": "x"})
    with pytest.raises(ManifestError, match="not valid JSON"):
        RunManifest.from_json("{not json")
    with pytest.raises(ManifestError, match="must be an object"):
        RunManifest.from_json("[1, 2, 3]")


# --- persisting ------------------------------------------------------------------------


async def test_persist_run_writes_every_file_replay_needs(tmp_path: Path) -> None:
    result = await ExperimentRunner(a_scenario()).run()

    run_dir = persist_run(result, tmp_path)

    assert run_dir == tmp_path / result.run_id
    for name in (MANIFEST_FILE, EVENTS_FILE, HISTORY_FILE, REPORT_FILE):
        assert (run_dir / name).exists(), name

    manifest = load_manifest(tmp_path, result.run_id)
    assert manifest.run_id == result.run_id
    assert manifest.seed == result.seed
    assert manifest.scenario == result.scenario.to_dict()
    assert manifest.election_timeout_range == result.election_timeout_range
    assert manifest.heartbeat_interval == result.heartbeat_interval
    assert manifest.client_timeout == result.client_timeout

    events = load_events(tmp_path, result.run_id)
    assert [e.to_dict() for e in events] == [e.to_dict() for e in result.events]


async def test_manifest_of_carries_the_scenarios_own_source(tmp_path: Path) -> None:
    path = tmp_path / "s.yaml"
    path.write_text("name: from-file\nduration: 1s\nnodes: 1\n", encoding="utf-8")
    scenario = Scenario.load(path)

    result = await ExperimentRunner(scenario).run()

    assert manifest_of(result).scenario_source == str(path)


# --- replay: the property that matters --------------------------------------------------


async def test_a_replayed_run_reproduces_the_trace_bit_for_bit(tmp_path: Path) -> None:
    result = await ExperimentRunner(a_scenario()).run()
    persist_run(result, tmp_path)

    comparison, replayed = await replay_run(tmp_path, result.run_id)

    assert comparison.matched
    assert comparison.divergence is None
    assert comparison.original_event_count == comparison.replayed_event_count == len(result.events)
    assert [e.to_dict() for e in replayed.events] == [e.to_dict() for e in result.events]
    # Not just the trace: the same seed reconstructs the same operations too.
    assert [op.to_dict() for op in replayed.history] == [op.to_dict() for op in result.history]
    assert "MATCH" in comparison.explain()


async def test_replay_reproduces_a_run_with_partitions_and_packet_loss(tmp_path: Path) -> None:
    """A busier timeline than the crash/recover smoke test, so the comparison is
    actually exercising partition/latency/loss faults and not just the quiet path.
    """
    scenario = a_scenario(
        seed=42,
        duration=4.0,
        faults=[
            {"at": 0.5, "action": "packet_loss", "rate": 0.1},
            {"at": 1.5, "action": "partition", "groups": [["n1"], ["n2", "n3"]]},
            {"at": 2.5, "action": "heal_partition"},
        ],
    )
    result = await ExperimentRunner(scenario).run()
    persist_run(result, tmp_path)

    comparison, _ = await replay_run(tmp_path, result.run_id)

    assert comparison.matched


async def test_replaying_two_different_seeds_of_the_same_scenario_stay_independent(
    tmp_path: Path,
) -> None:
    """Guards against a shared-state bug where replaying run B could pick up state left
    behind by run A - each run_id is its own directory and its own reconstruction.
    """
    first = await ExperimentRunner(a_scenario(seed=1)).run()
    second = await ExperimentRunner(a_scenario(seed=2)).run()
    persist_run(first, tmp_path)
    persist_run(second, tmp_path)

    first_replay, _ = await replay_run(tmp_path, first.run_id)
    second_replay, _ = await replay_run(tmp_path, second.run_id)

    assert first_replay.matched
    assert second_replay.matched
    assert [e.to_dict() for e in load_events(tmp_path, first.run_id)] != [
        e.to_dict() for e in load_events(tmp_path, second.run_id)
    ]


# --- replay: when it does not match, or cannot run at all ------------------------------


async def test_replaying_a_run_that_does_not_exist_is_refused(tmp_path: Path) -> None:
    with pytest.raises(ReplayError, match="no run"):
        await replay_run(tmp_path, "never-existed")


async def test_a_tampered_trace_is_caught_and_pinpointed(tmp_path: Path) -> None:
    """The comparison is what a real regression would trip: something in the recorded
    trace no longer matches what the same inputs produce.
    """
    result = await ExperimentRunner(a_scenario()).run()
    run_dir = persist_run(result, tmp_path)

    from pyraft_lab.observability.events import read_jsonl, write_jsonl

    events = read_jsonl(run_dir / "events.jsonl")
    tampered = list(events)
    tampered[3] = Event(
        kind=EventKind.TERM_CHANGED,
        timestamp=tampered[3].timestamp,
        seq=tampered[3].seq,
        node=tampered[3].node,
        term=999,
        details={},
    )
    write_jsonl(run_dir / "events.jsonl", tampered)

    comparison, _ = await replay_run(tmp_path, result.run_id)

    assert not comparison.matched
    assert comparison.divergence is not None
    assert comparison.divergence.index == 3
    assert comparison.divergence.original is not None
    assert comparison.divergence.original["term"] == 999
    assert "MISMATCH" in comparison.explain()
    assert "#3" in comparison.explain()


async def test_a_trace_with_extra_events_at_the_end_is_caught(tmp_path: Path) -> None:
    result = await ExperimentRunner(a_scenario()).run()
    run_dir = persist_run(result, tmp_path)

    from pyraft_lab.observability.events import read_jsonl, write_jsonl

    events = read_jsonl(run_dir / "events.jsonl")
    extra = Event(
        kind=EventKind.NODE_CRASHED, timestamp=events[-1].timestamp + 1, seq=999999, node="n1"
    )
    write_jsonl(run_dir / "events.jsonl", [*events, extra])

    comparison, _ = await replay_run(tmp_path, result.run_id)

    assert not comparison.matched
    assert comparison.original_event_count == comparison.replayed_event_count + 1
    assert comparison.divergence is not None
    assert comparison.divergence.index == len(events)
    assert comparison.divergence.replayed is None


async def test_a_manifest_naming_an_unresolvable_scenario_target_fails_at_replay(
    tmp_path: Path,
) -> None:
    """A manifest embeds the whole scenario, so a scenario that could not itself have
    been loaded also cannot be replayed - the same ``ScenarioError`` either way, not a
    replay-specific failure mode.
    """
    from pyraft_lab.experiments.scenario import ScenarioError

    result = await ExperimentRunner(a_scenario()).run()
    run_dir = persist_run(result, tmp_path)

    manifest = load_manifest(tmp_path, result.run_id)
    broken = manifest.to_dict()
    broken["scenario"]["nodes"] = 0
    (run_dir / "manifest.json").write_text(json.dumps(broken), encoding="utf-8")

    with pytest.raises(ScenarioError, match="at least one node"):
        await replay_run(tmp_path, result.run_id)
