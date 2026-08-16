"""The HTTP API, driven the way the front end drives it.

``TestClient`` runs the app's real lifespan, so the cluster these tests start is a real
``ClusterManager`` on a ``WallClock`` with real election timers - the same one
``pyraft-lab cluster start`` builds. Nothing here is mocked; a failure means a browser
would see the same thing.
"""

from __future__ import annotations

import pytest

fastapi = pytest.importorskip("fastapi")
from fastapi.testclient import TestClient  # noqa: E402

from pyraft_lab.api.server import create_app  # noqa: E402


@pytest.fixture
def workspace(tmp_path):
    (tmp_path / "scenarios").mkdir()
    (tmp_path / "results").mkdir()
    return tmp_path


@pytest.fixture
def client(workspace):
    with TestClient(create_app(workspace)) as client:
        yield client


@pytest.fixture
def running(client):
    response = client.post("/api/cluster/start", json={"nodes": 3, "seed": 7})
    assert response.status_code == 200, response.text
    yield client
    client.post("/api/cluster/stop")


def test_health_reports_no_cluster_before_one_is_started(client):
    body = client.get("/api/health").json()
    assert body["ok"] is True
    assert body["clusterRunning"] is False


def test_cluster_routes_refuse_rather_than_invent_an_empty_answer(client):
    for path in ("/api/cluster/nodes", "/api/kv", "/api/faults", "/api/cluster/trace"):
        assert client.get(path).status_code == 409, path


def test_starting_a_cluster_elects_a_leader_and_reports_it(running):
    body = running.get("/api/cluster").json()
    assert body["running"] is True
    assert body["status"]["nodeCount"] == 3
    assert body["status"]["leader"] in {"n1", "n2", "n3"}
    assert body["status"]["status"] == "HEALTHY"


def test_a_second_start_is_refused_while_one_is_running(running):
    assert running.post("/api/cluster/start", json={"nodes": 3}).status_code == 409


def test_a_put_is_readable_back_and_shows_up_in_the_log_and_the_store(running):
    assert running.post("/api/kv", json={"key": "color", "value": "blue"}).status_code == 200

    read = running.get("/api/kv/color").json()
    assert read == {"key": "color", "value": "blue", "found": True}

    records = running.get("/api/kv").json()["records"]
    assert {"color": "blue"}.items() <= {r["key"]: r["value"] for r in records}.items()

    entries = running.get("/api/cluster/log").json()["entries"]
    assert any(e["key"] == "color" and e["committed"] for e in entries)


def test_delete_and_cas_report_what_actually_happened(running):
    running.post("/api/kv", json={"key": "k", "value": "one"})

    assert running.post(
        "/api/kv/cas", json={"key": "k", "expected": "no", "new": "two"}
    ).json() == {"ok": False, "key": "k"}
    assert running.post(
        "/api/kv/cas", json={"key": "k", "expected": "one", "new": "two"}
    ).json() == {"ok": True, "key": "k"}
    assert running.get("/api/kv/k").json()["value"] == "two"

    assert running.request("DELETE", "/api/kv/k").json()["existed"] is True
    assert running.get("/api/kv/k").json()["found"] is False


def test_a_missing_key_is_a_found_false_answer_not_an_error(running):
    body = running.get("/api/kv/never-written").json()
    assert body["found"] is False and body["value"] is None


def test_a_key_containing_a_slash_can_be_read_and_deleted(running):
    """``a/b`` is an ordinary KV key, not a path.

    Under a plain ``{key}`` parameter it could be written and listed but never read or
    deleted - the route stopped at the separator - so the UI showed a row whose Get and
    delete button both 404'd. Encoding the slash does not help: it is decoded again
    before routing.
    """
    assert running.post("/api/kv", json={"key": "a/b", "value": "slashy"}).status_code == 200

    assert running.get("/api/kv/a/b").json() == {"key": "a/b", "value": "slashy", "found": True}
    assert running.request("DELETE", "/api/kv/a/b").json()["existed"] is True
    assert running.get("/api/kv/a/b").json()["found"] is False


def test_a_key_named_like_a_route_is_still_a_key(running):
    """``cas`` collides with ``POST /api/kv/cas`` only if the router confuses methods."""
    assert running.post("/api/kv", json={"key": "cas", "value": "not-a-route"}).status_code == 200
    assert running.get("/api/kv/cas").json()["value"] == "not-a-route"
    assert (
        running.post(
            "/api/kv/cas", json={"key": "cas", "expected": "not-a-route", "new": "x"}
        ).json()["ok"]
        is True
    )


def test_nodes_report_their_real_raft_state(running):
    nodes = running.get("/api/cluster/nodes").json()
    assert [n["id"] for n in nodes] == ["n1", "n2", "n3"]
    assert sum(1 for n in nodes if n["role"] == "leader") == 1
    assert all(n["alive"] for n in nodes)
    # An in-memory cluster has no WAL, and the API says so rather than faking a path.
    assert all(n["walPath"] is None for n in nodes)


def test_crashing_a_node_takes_it_down_and_recovering_brings_it_back(running):
    leader = running.get("/api/cluster").json()["status"]["leader"]
    victim = next(n for n in ("n1", "n2", "n3") if n != leader)

    crashed = running.post(f"/api/cluster/nodes/{victim}/crash").json()
    assert crashed["alive"] is False and crashed["role"] == "down"
    assert running.post(f"/api/cluster/nodes/{victim}/crash").status_code == 409

    recovered = running.post(f"/api/cluster/nodes/{victim}/recover").json()
    assert recovered["alive"] is True


def test_restarting_a_node_keeps_the_cluster_led(running):
    assert running.post("/api/cluster/nodes/n2/restart").status_code == 200
    assert running.get("/api/cluster").json()["status"]["leader"] is not None


def test_an_unknown_node_is_a_404(running):
    assert running.get("/api/cluster/nodes/n99").status_code == 404


def test_partitioning_is_reported_and_heals(running):
    faults = running.post("/api/faults/partition", json={"groups": [["n1"], ["n2", "n3"]]}).json()
    assert faults["partitioned"] is True
    assert sorted(sorted(g) for g in faults["partitionGroups"]) == [["n1"], ["n2", "n3"]]

    assert running.get("/api/cluster").json()["status"]["status"] == "PARTITIONED"
    assert running.post("/api/faults/heal").json()["partitioned"] is False


def test_a_partition_naming_an_unknown_node_is_refused(running):
    response = running.post("/api/faults/partition", json={"groups": [["n1"], ["nowhere"]]})
    assert response.status_code == 422
    assert "nowhere" in response.json()["detail"]


def test_latency_and_loss_round_trip_through_the_fault_view(running):
    assert running.post("/api/faults/latency", json={"minMs": 5, "maxMs": 20}).json() == {
        **running.get("/api/faults").json(),
        "minLatencyMs": 5.0,
        "maxLatencyMs": 20.0,
    }
    assert running.post("/api/faults/loss", json={"probability": 0.25}).json()[
        "lossProbability"
    ] == pytest.approx(0.25)

    reset = running.post("/api/faults/reset").json()
    assert reset["lossProbability"] == 0 and reset["maxLatencyMs"] == 0


def test_latency_with_min_above_max_is_refused(running):
    assert running.post("/api/faults/latency", json={"minMs": 50, "maxMs": 5}).status_code == 422


def test_the_console_runs_the_repls_own_commands(running):
    body = running.post("/api/cluster/exec", json={"line": "put greeting hello"}).json()
    assert body == {"ok": True, "output": "ok: greeting = hello"}

    assert (
        "greeting = hello"
        in running.post("/api/cluster/exec", json={"line": "get greeting"}).json()["output"]
    )
    assert (
        "node    alive"
        in running.post("/api/cluster/exec", json={"line": "topology"}).json()["output"]
    )


def test_an_unknown_console_command_answers_rather_than_500s(running):
    body = running.post("/api/cluster/exec", json={"line": "sudo rm -rf /"}).json()
    assert body["ok"] is False and "unknown command" in body["output"]


def test_the_console_refuses_the_two_verbs_that_make_no_sense_over_http(running):
    for line in ("exit", "dashboard"):
        assert running.post("/api/cluster/exec", json={"line": line}).json()["ok"] is False


def test_the_trace_and_the_rpc_view_are_folded_from_the_same_events(running):
    running.post("/api/kv", json={"key": "a", "value": "1"})

    trace = running.get("/api/cluster/trace").json()
    assert trace and {"LEADER_ELECTED"} <= {e["kind"] for e in trace}

    rpc = running.get("/api/cluster/rpc").json()
    assert rpc and all(row["from"] and row["type"] for row in rpc)


def test_an_in_memory_cluster_says_it_has_no_wal_rather_than_pretending(running):
    body = running.get("/api/cluster/nodes/n1/wal").json()
    assert body["exists"] is False and "in-memory" in body["reason"]

    snapshots = running.get("/api/cluster/nodes/n1/snapshots").json()
    assert snapshots["supported"] is False and snapshots["snapshots"] == []


def test_a_persistent_cluster_exposes_a_real_wal_and_can_snapshot(client):
    started = client.post("/api/cluster/start", json={"nodes": 3, "persistent": True})
    assert started.status_code == 200, started.text
    try:
        client.post("/api/kv", json={"key": "durable", "value": "yes"})

        # The node listing reads each member's WAL size, which an in-memory cluster
        # never exercises - this is where a persistent cluster's serializer breaks first.
        nodes = client.get("/api/cluster/nodes").json()
        assert all(node["walPath"] for node in nodes)
        assert all(isinstance(node["walBytes"], int) and node["walBytes"] > 0 for node in nodes)

        wal = client.get("/api/cluster/nodes/n1/wal").json()
        assert wal["exists"] is True
        assert wal["recordsValid"] > 0
        assert all(record["status"] == "VALID" for record in wal["records"])

        leader = client.get("/api/cluster").json()["status"]["leader"]
        meta = client.post(f"/api/cluster/nodes/{leader}/snapshots").json()
        assert meta["lastIncludedIndex"] >= 1

        listing = client.get(f"/api/cluster/nodes/{leader}/snapshots").json()
        assert listing["supported"] is True and len(listing["snapshots"]) == 1
    finally:
        client.post("/api/cluster/stop")


def test_the_config_round_trips_through_cluster_yaml(client, workspace):
    saved = client.put(
        "/api/config",
        json={
            "nodes": 5,
            "persistent": True,
            "electionTimeoutMinMs": 200,
            "electionTimeoutMaxMs": 400,
            "heartbeatIntervalMs": 60,
            "clientTimeoutMs": 250,
            "seed": 11,
        },
    ).json()
    assert saved["nodes"] == 5 and saved["saved"] is True
    assert (workspace / "cluster.yaml").exists()

    # A start with no overrides picks the saved file up, exactly as --config does.
    assert client.post("/api/cluster/start", json={}).status_code == 200
    try:
        assert client.get("/api/cluster").json()["status"]["nodeCount"] == 5
    finally:
        client.post("/api/cluster/stop")


def test_a_config_with_an_inverted_election_range_is_refused(client):
    response = client.put(
        "/api/config",
        json={
            "nodes": 3,
            "persistent": False,
            "electionTimeoutMinMs": 400,
            "electionTimeoutMaxMs": 200,
            "heartbeatIntervalMs": 50,
            "clientTimeoutMs": 200,
            "seed": 0,
        },
    )
    assert response.status_code == 422


# --- the laboratory ------------------------------------------------------------------------

BASELINE = """
name: api-smoke
duration: 2s
nodes: 3
seed: 5
workload:
  puts_per_sec: 5
"""


def _await_job(client, job_id, *, timeout=180.0):
    """Poll until the job leaves ``running``. The work happens on a worker thread, so
    the loop this polls from is free the whole time."""
    import time

    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        body = client.get(f"/api/jobs/{job_id}").json()
        if body["status"] != "running":
            return body
        time.sleep(0.1)
    raise AssertionError(f"job {job_id} did not finish within {timeout}s")


def test_a_scenario_runs_persists_and_becomes_replayable(client, workspace):
    (workspace / "scenarios" / "smoke.yaml").write_text(BASELINE, encoding="utf-8")

    listing = client.get("/api/scenarios").json()
    assert [row["file"] for row in listing] == ["smoke.yaml"]
    assert listing[0]["nodes"] == 3

    started = client.post("/api/experiments/run", json={"scenario": "smoke.yaml"}).json()
    finished = _await_job(client, started["id"])
    assert finished["status"] == "succeeded", finished["error"]

    run_id = finished["result"]["runId"]
    assert finished["result"]["report"]["scenario"] == "api-smoke"

    assert run_id in [row["runId"] for row in client.get("/api/results").json()]
    detail = client.get(f"/api/results/{run_id}").json()
    assert detail["manifest"]["run_id"] == run_id
    assert client.get(f"/api/results/{run_id}/events").json()

    replay = _await_job(client, client.post(f"/api/results/{run_id}/replay").json()["id"])
    assert replay["status"] == "succeeded"
    assert replay["result"]["matched"] is True, replay["result"]["explanation"]


def test_a_campaign_reports_progress_per_trial_while_it_runs(client):
    started = client.post(
        "/api/campaigns/stress", json={"nodes": 3, "duration": "2s", "trials": 2, "seed": 1}
    ).json()
    finished = _await_job(client, started["id"])
    assert finished["status"] == "succeeded", finished["error"]
    assert finished["completed"] == finished["total"] == 2
    assert len(finished["lines"]) == 2
    assert finished["result"]["trials"] == 2


def test_a_benchmark_produces_one_cell_per_grid_point(client):
    started = client.post(
        "/api/campaigns/benchmark",
        json={"nodeCounts": [3], "lossPercents": [0, 10], "duration": "2s", "seed": 3},
    ).json()
    finished = _await_job(client, started["id"])
    assert finished["status"] == "succeeded", finished["error"]
    assert finished["result"]["cells"] == 2
    assert len(finished["result"]["results"]) == 2
    assert finished["result"]["graphs"]


def test_a_campaign_started_over_http_does_not_depose_the_live_cluster(running):
    """The reason campaigns get a worker thread of their own.

    A virtual-clock run never yields to real time, so awaiting one on the API's own
    loop would freeze the live cluster's wall-clock timers for the whole campaign and
    it would elect its way through several terms. Off-loop, the term must hold.
    """
    before = running.get("/api/cluster").json()["status"]
    started = running.post(
        "/api/campaigns/stress", json={"nodes": 3, "duration": "3s", "trials": 2, "seed": 2}
    ).json()
    assert _await_job(running, started["id"])["status"] == "succeeded"

    after = running.get("/api/cluster").json()["status"]
    assert after["leader"] == before["leader"]
    assert after["term"] == before["term"]


def test_an_unparseable_duration_is_refused_before_a_job_is_started(client):
    response = client.post("/api/campaigns/chaos", json={"duration": "soon"})
    assert response.status_code == 422
    assert client.get("/api/jobs").json() == []


def test_a_missing_scenario_is_a_404_and_a_traversal_is_a_400(client):
    assert client.post("/api/experiments/run", json={"scenario": "nope.yaml"}).status_code == 404
    assert client.get("/api/scenarios/..%2F..%2Fcluster.yaml").status_code in (400, 404)


def test_results_for_an_unknown_run_are_a_404(client):
    assert client.get("/api/results/never-ran").status_code == 404


def test_docs_pages_serve_the_repositorys_own_markdown(client):
    names = {row["name"] for row in client.get("/api/docs-pages").json()}
    assert {"readme", "architecture"} <= names
    assert "# Architecture" in client.get("/api/docs-pages/architecture").json()["markdown"]
    assert client.get("/api/docs-pages/nope").status_code == 404


def test_the_stream_pushes_the_current_state_then_live_events(running):
    with running.websocket_connect("/api/stream") as socket:
        hello = socket.receive_json()
        assert hello["channel"] == "hello"
        assert hello["payload"]["running"] is True

        running.post("/api/kv", json={"key": "streamed", "value": "1"})
        for _ in range(40):
            message = socket.receive_json()
            if message["channel"] == "events":
                assert message["payload"] and message["payload"][0]["kind"]
                return
        raise AssertionError("no event batch arrived on the stream")
