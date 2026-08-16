"""Phase 9: ``ClusterManager``/``NodeManager``/``LifecycleManager``, driven directly -
no REPL, no CLI, no stdin. ``tests/test_cli_repl.py`` covers the REPL layer on top of
this; this file is about the cluster controller itself.

Uses the same ``FAST_ELECTION``/``FAST_HEARTBEAT`` constants ``tests/test_node_election.py``
already defines, imported from there rather than redefined - ``ClusterManager`` is a
``WallClock`` construct, same as the nodes those tests build directly, so the same
"compress the timers, keep the ratio" reasoning applies here too.
"""

from __future__ import annotations

import asyncio
from pathlib import Path

import pytest
from tests.test_node_election import FAST_ELECTION, FAST_HEARTBEAT

from pyraft_lab.cluster.config import ClusterConfig, ClusterConfigError
from pyraft_lab.cluster.lifecycle import LifecycleManager, LifecycleTimeout
from pyraft_lab.cluster.manager import ClusterError, ClusterManager
from pyraft_lab.network import InMemoryBus
from pyraft_lab.network.clock import WallClock
from pyraft_lab.observability.events import EventKind
from pyraft_lab.raft.node import RaftNode


def a_config(**overrides: object) -> ClusterConfig:
    data = {
        "nodes": 3,
        "election_timeout_range": FAST_ELECTION,
        "heartbeat_interval": FAST_HEARTBEAT,
    }
    data.update(overrides)
    return ClusterConfig(**data)


def leader_id(manager: ClusterManager) -> str:
    return next(row.node_id for row in manager.topology() if row.role == "leader")


def follower_id(manager: ClusterManager) -> str:
    return next(row.node_id for row in manager.topology() if row.role != "leader")


async def wait_for_replication(
    manager: ClusterManager, node_id: str, key: str, timeout: float = 1.0
) -> None:
    """Poll until ``node_id``'s own store has ``key`` - a follower's local state can
    lag the leader's reply by one heartbeat round, and a test that restarts a follower
    before it has actually caught up is testing a race, not persistence."""
    deadline = asyncio.get_running_loop().time() + timeout
    while asyncio.get_running_loop().time() < deadline:
        if key in manager.node(node_id).store:
            return
        await asyncio.sleep(0.01)
    raise AssertionError(f"{node_id} never replicated {key!r} within {timeout}s")


# --- ClusterConfig ---------------------------------------------------------------------


def test_cluster_config_yaml_roundtrip(tmp_path: Path) -> None:
    config = ClusterConfig(nodes=5, data_dir=Path("d"), seed=3)
    path = config.save(tmp_path / "c.yaml")

    assert ClusterConfig.load(path) == config


def test_cluster_config_rejects_zero_nodes() -> None:
    with pytest.raises(ClusterConfigError, match="at least one node"):
        ClusterConfig(nodes=0)


def test_cluster_config_from_dict_rejects_zero_nodes() -> None:
    with pytest.raises(ClusterConfigError):
        ClusterConfig.from_dict({"nodes": 0})


def test_loading_a_cluster_config_that_is_not_there_says_so(tmp_path: Path) -> None:
    with pytest.raises(ClusterConfigError, match="no cluster config at"):
        ClusterConfig.load(tmp_path / "nope.yaml")


def test_a_malformed_cluster_config_is_refused_cleanly(tmp_path: Path) -> None:
    path = tmp_path / "bad.yaml"
    path.write_text("- just\n- a\n- list\n", encoding="utf-8")
    with pytest.raises(ClusterConfigError, match="must be a mapping"):
        ClusterConfig.load(path)


# --- ClusterManager: the core lifecycle -------------------------------------------------


async def test_three_node_cluster_start_serves_put_get_and_stops_cleanly() -> None:
    manager = ClusterManager(a_config())
    await manager.start()

    try:
        rows = manager.topology()
        assert sum(1 for row in rows if row.role == "leader") == 1

        assert manager.client is not None
        await manager.client.put("k", "v")
        assert await manager.client.get("k") == "v"
    finally:
        await manager.stop()


async def test_restart_without_wal_reuses_the_same_object_and_keeps_state() -> None:
    manager = ClusterManager(a_config())
    await manager.start()

    try:
        await manager.client.put("k", "v")
        target = follower_id(manager)
        await wait_for_replication(manager, target, "k")

        before = manager.node(target)
        await manager.restart_node(target)
        after = manager.node(target)

        assert after is before  # stop() never clears in-memory state
        assert after.store.read("k") == "v"
    finally:
        await manager.stop()


async def test_restart_with_wal_rebuilds_purely_from_disk(tmp_path: Path) -> None:
    manager = ClusterManager(a_config(data_dir=tmp_path))
    await manager.start()

    try:
        await manager.client.put("k", "v")
        target = follower_id(manager)
        await wait_for_replication(manager, target, "k")

        before = manager.node(target)
        await manager.restart_node(target)
        after = manager.node(target)

        assert after is not before  # a fresh RaftNode, not the crashed one
        assert after.store.read("k") == "v"
        assert after.state.commit_index > 0
    finally:
        await manager.stop()


async def test_restart_of_the_leader_reelects_and_the_cluster_stays_healthy() -> None:
    manager = ClusterManager(a_config())
    await manager.start()

    try:
        target = leader_id(manager)
        await manager.restart_node(target)

        rows = manager.topology()
        assert sum(1 for row in rows if row.role == "leader") == 1

        crashed = manager.tracer.of_kind(EventKind.NODE_CRASHED)
        recovered = manager.tracer.of_kind(EventKind.NODE_RECOVERED)
        assert any(e.node == target for e in crashed)
        assert any(e.node == target for e in recovered)
    finally:
        await manager.stop()


async def test_restarting_an_unknown_node_raises() -> None:
    manager = ClusterManager(a_config())
    await manager.start()
    try:
        with pytest.raises(ClusterError, match="no such node"):
            await manager.restart_node("nope")
    finally:
        await manager.stop()


async def test_node_and_status_and_topology_reflect_a_down_node() -> None:
    manager = ClusterManager(a_config())
    await manager.start()

    try:
        target = follower_id(manager)
        await manager.nodes[target].stop(crashed=True)

        rows = {row.node_id: row for row in manager.topology()}
        assert rows[target].alive is False

        snap = manager.status()
        assert target in snap["down"]
    finally:
        # the down node is already stopped; stop() tolerates that via LifecycleManager
        await manager.stop()


# --- LifecycleManager, reusable standalone ----------------------------------------------


async def test_lifecycle_wait_for_leader_reusable_standalone() -> None:
    bus = InMemoryBus()
    ids = ["n1", "n2", "n3"]
    nodes = [
        RaftNode(
            node_id=node_id, peers=ids, transport=bus,
            election_timeout_range=FAST_ELECTION, heartbeat_interval=FAST_HEARTBEAT,
        )
        for node_id in ids
    ]
    for node in nodes:
        node.start()

    try:
        leader = await LifecycleManager(WallClock()).wait_for_leader(nodes, timeout=0.6)
        assert leader in nodes
    finally:
        for node in nodes:
            await node.stop()


async def test_lifecycle_wait_for_leader_times_out() -> None:
    bus = InMemoryBus()
    # A lone node whose peer list names someone who never starts: it can never see a
    # majority of a 2-node cluster, so it campaigns forever without winning.
    node = RaftNode(
        node_id="n1", peers=["n1", "n2"], transport=bus,
        election_timeout_range=FAST_ELECTION, heartbeat_interval=FAST_HEARTBEAT,
    )
    node.start()

    try:
        with pytest.raises(LifecycleTimeout):
            await LifecycleManager(WallClock()).wait_for_leader([node], timeout=0.2)
    finally:
        await node.stop()
