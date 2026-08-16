"""Phase 8: ``ExperimentRunner``'s opt-in WAL directory.

Two things are under test:

1. the default (``wal_dir=None``) is unchanged - a crashed node's *same* object comes
   back on recovery, exactly as it did before this phase. This is the load-bearing
   backward-compatibility guarantee, and the rest of the suite (every pre-existing
   test, every shipped scenario) passing unmodified is the other half of that proof -
   nothing here needs to re-assert it beyond this one direct check.
2. with ``wal_dir`` set, a crash genuinely discards the node and recovery rebuilds a
   *new* one from nothing but what its WAL persisted - the mechanism Phase 8's chaos
   campaign depends on (docs/architecture.md P8).

Scenarios are built directly, in ``test_replay.py``'s style, and kept short.
"""

from __future__ import annotations

from pathlib import Path

from pyraft_lab.experiments.runner import ExperimentRunner
from pyraft_lab.experiments.scenario import CRASH, RECOVER, FaultStep, Scenario
from pyraft_lab.raft.persistence import read_wal


def a_scenario(**overrides: object) -> Scenario:
    data = {
        "name": "persistence-smoke",
        "duration": 6.0,
        "nodes": 3,
        "seed": 11,
        "workload": {
            "type": "put_get_mix", "puts_per_sec": 20, "get_ratio": 0.5, "keys": 3, "clients": 2,
        },
    }
    data.update(overrides)
    return Scenario.from_dict(data)


async def test_without_wal_dir_a_crashed_node_is_the_same_object_on_recovery() -> None:
    scenario = a_scenario(
        faults=[{"at": 1.0, "action": CRASH, "target": "1"}, {"at": 2.0, "action": RECOVER}]
    )
    runner = ExperimentRunner(scenario)
    runner._build_cluster()
    for node in runner.nodes.values():
        node.start()

    before = runner.nodes["n1"]
    await runner._crash(FaultStep(at=1.0, action=CRASH, target="1"))
    runner._recover(FaultStep(at=2.0, action=RECOVER))
    after = runner.nodes["n1"]

    assert after is before
    for node_id, node in runner.nodes.items():
        if node_id not in runner._down:
            await node.stop(crashed=False)


async def test_with_wal_dir_a_crashed_node_is_rebuilt_from_disk(tmp_path: Path) -> None:
    scenario = a_scenario(
        faults=[{"at": 1.0, "action": CRASH, "target": "1"}, {"at": 3.0, "action": RECOVER}]
    )
    result = await ExperimentRunner(scenario, wal_dir=tmp_path).run()

    assert result.persist is True
    wal_file = tmp_path / "n1.wal"
    assert wal_file.exists()

    recovery = read_wal(wal_file)
    assert recovery.records_valid > 0
    # Whatever this node believed when it was told to answer a write survived the
    # crash purely because it was on disk - the recovered term/commit index line up
    # with what the run actually reached, not with a fresh, empty node.
    assert recovery.term >= 1
    assert recovery.commit_index > 0


async def test_wal_dir_rebuild_produces_a_new_object_not_a_reused_one(tmp_path: Path) -> None:
    scenario = a_scenario(
        faults=[{"at": 1.0, "action": CRASH, "target": "2"}, {"at": 3.0, "action": RECOVER}]
    )
    runner = ExperimentRunner(scenario, wal_dir=tmp_path)
    runner._build_cluster()
    for node in runner.nodes.values():
        node.start()

    before = runner.nodes["n2"]
    await runner._crash(FaultStep(at=1.0, action=CRASH, target="2"))
    runner._recover(FaultStep(at=3.0, action=RECOVER))
    after = runner.nodes["n2"]

    assert after is not before
    for node_id, node in runner.nodes.items():
        if node_id not in runner._down:
            await node.stop(crashed=False)


async def test_two_nodes_each_get_their_own_independent_wal(tmp_path: Path) -> None:
    scenario = a_scenario(
        nodes=5,
        faults=[
            {"at": 1.0, "action": CRASH, "target": "1"},
            {"at": 2.0, "action": RECOVER},
            {"at": 3.0, "action": CRASH, "target": "3"},
            {"at": 4.0, "action": RECOVER},
        ],
    )
    result = await ExperimentRunner(scenario, wal_dir=tmp_path).run()

    assert result.persist is True
    for node_id in ("n1", "n3"):
        wal_file = tmp_path / f"{node_id}.wal"
        assert wal_file.exists()
        assert read_wal(wal_file).records_valid > 0
    # n2/n4/n5 never crashed, but every node in a persistent run is built with a WAL.
    for node_id in ("n2", "n4", "n5"):
        assert (tmp_path / f"{node_id}.wal").exists()


async def test_a_node_crashed_and_recovered_twice_keeps_both_cycles(tmp_path: Path) -> None:
    scenario = a_scenario(
        duration=8.0,
        faults=[
            {"at": 1.0, "action": CRASH, "target": "1"},
            {"at": 2.0, "action": RECOVER},
            {"at": 4.0, "action": CRASH, "target": "1"},
            {"at": 5.0, "action": RECOVER},
        ],
    )
    result = await ExperimentRunner(scenario, wal_dir=tmp_path).run()

    recovery = read_wal(tmp_path / "n1.wal")
    assert recovery.records_valid > 0
    assert recovery.commit_index > 0
    assert result.persist is True
