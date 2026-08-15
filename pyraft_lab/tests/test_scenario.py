"""Scenario parsing: both fault dialects folding into one timeline, node resolution,
and the load errors a malformed file should fail with.

There was no coverage of this module before Phase 7 needed a scenario it could
round-trip through a manifest with confidence - see ``test_replay.py``.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from pyraft_lab.experiments.scenario import (
    CRASH,
    HEAL_PARTITION,
    LATENCY,
    PACKET_LOSS,
    PARTITION,
    RECOVER,
    Scenario,
    ScenarioError,
    parse_duration,
)


def test_duration_parses_units() -> None:
    assert parse_duration("30s") == 30.0
    assert parse_duration("500ms") == 0.5
    assert parse_duration("2m") == 120.0
    assert parse_duration(5) == 5.0
    assert parse_duration(2.5) == 2.5


def test_duration_rejects_garbage() -> None:
    with pytest.raises(ScenarioError, match="not a duration"):
        parse_duration("soon")


def test_a_minimal_scenario_gets_sensible_defaults() -> None:
    scenario = Scenario.from_dict({"name": "min", "duration": "10s"})

    assert scenario.nodes == 3
    assert scenario.node_ids == ["n1", "n2", "n3"]
    assert scenario.faults == ()
    assert scenario.seed == 0
    assert scenario.workload.type == "put_get_mix"


def test_a_scenario_needs_a_name_and_a_duration() -> None:
    with pytest.raises(ScenarioError, match="needs a name"):
        Scenario.from_dict({"duration": "10s"})
    with pytest.raises(ScenarioError, match="needs a duration"):
        Scenario.from_dict({"name": "x"})


def test_a_cluster_needs_at_least_one_node() -> None:
    with pytest.raises(ScenarioError, match="at least one node"):
        Scenario.from_dict({"name": "x", "duration": "1s", "nodes": 0})


def test_faults_as_empty_dict_list_or_absent_all_mean_none() -> None:
    for faults in (None, {}, []):
        data = {"name": "x", "duration": "1s"}
        if faults is not None:
            data["faults"] = faults
        assert Scenario.from_dict(data).faults == ()


# --- the two dialects --------------------------------------------------------------


def test_the_flat_dialect_with_a_duration_becomes_a_crash_recover_pair() -> None:
    scenario = Scenario.from_dict(
        {
            "name": "x",
            "duration": "30s",
            "faults": [
                {"type": "node_crash", "target": "leader", "start": "10s", "duration": "5s"}
            ],
        }
    )

    assert [s.action for s in scenario.faults] == [CRASH, RECOVER]
    assert scenario.faults[0].at == 10.0
    assert scenario.faults[0].target == "leader"
    assert scenario.faults[1].at == 15.0


def test_the_chained_dialect_is_read_directly() -> None:
    scenario = Scenario.from_dict(
        {
            "name": "x",
            "duration": "30s",
            "faults": [
                {"at": "10s", "action": "crash", "target": "leader"},
                {"at": "15s", "action": "packet_loss", "rate": 0.2},
                {
                    "at": "20s",
                    "action": "partition",
                    "groups": [["n1", "n2"], ["n3"]],
                },
                {"at": "30s", "action": "heal_partition"},
            ],
        }
    )

    assert [s.action for s in scenario.faults] == [CRASH, PACKET_LOSS, PARTITION, HEAL_PARTITION]
    assert scenario.faults[1].params == {"rate": 0.2}


def test_a_chained_step_with_a_duration_gets_its_own_undo() -> None:
    scenario = Scenario.from_dict(
        {
            "name": "x",
            "duration": "10s",
            "faults": [{"at": "1s", "action": "crash", "target": "n1", "duration": "2s"}],
        }
    )

    assert [s.action for s in scenario.faults] == [CRASH, RECOVER]
    assert scenario.faults[1].at == 3.0


def test_link_config_can_mean_latency_and_loss_at_once() -> None:
    scenario = Scenario.from_dict(
        {
            "name": "x",
            "duration": "5s",
            "faults": [{"type": "link_config", "start": "0s", "latency_ms": 100, "drop_rate": 0.1}],
        }
    )

    assert {s.action for s in scenario.faults} == {LATENCY, PACKET_LOSS}


def test_faults_at_the_same_instant_keep_file_order() -> None:
    scenario = Scenario.from_dict(
        {
            "name": "x",
            "duration": "5s",
            "faults": [
                {"at": "1s", "action": "partition", "groups": [["n1"], ["n2", "n3"]]},
                {"at": "1s", "action": "crash", "target": "n2"},
            ],
        }
    )

    assert [s.action for s in scenario.faults] == [PARTITION, CRASH]


def test_an_unknown_fault_type_or_action_is_rejected() -> None:
    with pytest.raises(ScenarioError, match="unknown fault type"):
        Scenario.from_dict(
            {"name": "x", "duration": "1s", "faults": [{"type": "meteor", "start": "0s"}]}
        )
    with pytest.raises(ScenarioError, match="unknown fault action"):
        Scenario.from_dict(
            {"name": "x", "duration": "1s", "faults": [{"at": "0s", "action": "meteor"}]}
        )


def test_a_fault_entry_that_is_neither_dialect_is_rejected() -> None:
    with pytest.raises(ScenarioError, match="neither an 'action'"):
        Scenario.from_dict({"name": "x", "duration": "1s", "faults": [{"start": "0s"}]})


# --- node resolution -----------------------------------------------------------------


def test_resolve_node_understands_every_spelling() -> None:
    scenario = Scenario.from_dict({"name": "x", "duration": "1s", "nodes": 3})

    for spelling in (2, "2", "n2", "node-2", "node_2", "NODE-2"):
        assert scenario.resolve_node(spelling) == "n2"
    assert scenario.resolve_node(None) is None


def test_resolve_node_rejects_out_of_range_or_unrecognized() -> None:
    scenario = Scenario.from_dict({"name": "x", "duration": "1s", "nodes": 3})

    with pytest.raises(ScenarioError, match="outside a 3-node cluster"):
        scenario.resolve_node(9)
    with pytest.raises(ScenarioError, match="does not name a node"):
        scenario.resolve_node("gremlin")


# --- workload validation -------------------------------------------------------------


def test_an_unknown_workload_setting_is_rejected() -> None:
    with pytest.raises(ScenarioError, match="unknown workload settings"):
        Scenario.from_dict(
            {"name": "x", "duration": "1s", "workload": {"puts_per_sec": 5, "warp_speed": True}}
        )


# --- round-tripping and loading -------------------------------------------------------


def test_to_dict_and_from_dict_round_trip() -> None:
    scenario = Scenario.from_dict(
        {
            "name": "roundtrip",
            "duration": "12s",
            "nodes": 5,
            "seed": 3,
            "faults": [{"at": "1s", "action": "crash", "target": "leader"}],
            "workload": {"puts_per_sec": 5, "clients": 3},
            "expected": {"data_loss": False},
        }
    )

    restored = Scenario.from_dict(scenario.to_dict())
    assert restored.to_dict() == scenario.to_dict()


def test_loading_a_yaml_file(tmp_path: Path) -> None:
    path = tmp_path / "s.yaml"
    path.write_text("name: from-file\nduration: 5s\nnodes: 3\n", encoding="utf-8")

    scenario = Scenario.load(path)

    assert scenario.name == "from-file"
    assert scenario.source == path


def test_loading_a_missing_file_or_bad_yaml_fails_clearly(tmp_path: Path) -> None:
    with pytest.raises(ScenarioError, match="no scenario file"):
        Scenario.load(tmp_path / "nope.yaml")

    bad = tmp_path / "bad.yaml"
    bad.write_text("name: [unterminated\n", encoding="utf-8")
    with pytest.raises(ScenarioError, match="could not parse YAML"):
        Scenario.load(bad)


def test_shipped_scenarios_all_load_and_have_the_required_names() -> None:
    from pyraft_lab.experiments.scenario import load_all

    repo_root = Path(__file__).resolve().parents[1]
    scenarios = load_all(repo_root / "scenarios")
    names = {s.name for s in scenarios}

    required = {
        "baseline",
        "leader_crash",
        "minority_partition",
        "majority_partition",
        "network_latency",
        "packet_loss",
        "churn",
    }
    assert required <= names
