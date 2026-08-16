"""The routes.

Grouped the way the front end is: the live cluster, the key-value store it fronts, the
faults you can do to it, the laboratory (scenarios, campaigns, replay), and the
artefacts all of that leaves in ``results/``.

Two rules hold throughout. A route never reimplements a behaviour the library already
has - ``/api/cluster/exec`` runs the REPL's own dispatcher, ``/api/experiments/run``
calls the same ``run_scenario``/``persist_run`` pair ``pyraft-lab run`` does. And a
route that needs a running cluster says so with a 409 rather than inventing an empty
answer, because "no cluster" and "an idle cluster" are different facts.
"""

from __future__ import annotations

import contextlib
import json
from pathlib import Path
from typing import Any

from fastapi import APIRouter, FastAPI, HTTPException, Query, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

from pyraft_lab import __version__
from pyraft_lab.api import serializers as ser
from pyraft_lab.api.jobs import Job
from pyraft_lab.api.state import LabError, LabState
from pyraft_lab.cli.repl import ReplCommandError, ReplParseError, execute_command
from pyraft_lab.client.client import RequestFailed
from pyraft_lab.cluster.config import ClusterConfig, ClusterConfigError
from pyraft_lab.cluster.lifecycle import LifecycleTimeout
from pyraft_lab.cluster.manager import ClusterError
from pyraft_lab.experiments.manifest import ManifestError, RunManifest
from pyraft_lab.experiments.replay import EVENTS_FILE, MANIFEST_FILE, REPORT_FILE
from pyraft_lab.experiments.scenario import Scenario, ScenarioError, parse_duration
from pyraft_lab.raft.snapshot import SnapshotError

DEFAULT_ORIGINS = [
    "http://localhost:5173",
    "http://127.0.0.1:5173",
    "http://localhost:4173",
    "http://127.0.0.1:4173",
]


# --- request bodies -----------------------------------------------------------------------


class StartClusterBody(BaseModel):
    nodes: int | None = Field(default=None, ge=1, le=15)
    seed: int | None = None
    persistent: bool | None = None
    electionTimeoutMinMs: float | None = Field(default=None, gt=0)
    electionTimeoutMaxMs: float | None = Field(default=None, gt=0)
    heartbeatIntervalMs: float | None = Field(default=None, gt=0)
    clientTimeoutMs: float | None = Field(default=None, gt=0)


class ExecBody(BaseModel):
    line: str


class PutBody(BaseModel):
    key: str = Field(min_length=1)
    value: str


class CasBody(BaseModel):
    key: str = Field(min_length=1)
    expected: str
    new: str


class PartitionBody(BaseModel):
    groups: list[list[str]] = Field(min_length=2)


class LatencyBody(BaseModel):
    minMs: float = Field(ge=0)
    maxMs: float = Field(ge=0)


class LossBody(BaseModel):
    probability: float = Field(ge=0, le=1)


class RunScenarioBody(BaseModel):
    scenario: str
    seed: int | None = None
    plot: bool = False


class CampaignBody(BaseModel):
    nodes: int = Field(default=3, ge=1, le=15)
    duration: str = "10s"
    trials: int = Field(default=10, ge=1, le=200)
    seed: int = 0


class BenchmarkBody(BaseModel):
    nodeCounts: list[int] = Field(default=[3, 5], min_length=1)
    lossPercents: list[float] = Field(default=[0, 5, 10], min_length=1)
    duration: str = "10s"
    seed: int = 0


class ConfigBody(BaseModel):
    nodes: int = Field(ge=1, le=15)
    persistent: bool = False
    electionTimeoutMinMs: float = Field(gt=0)
    electionTimeoutMaxMs: float = Field(gt=0)
    heartbeatIntervalMs: float = Field(gt=0)
    clientTimeoutMs: float = Field(gt=0)
    seed: int = 0


# --- the app ---------------------------------------------------------------------------------


def create_app(workspace: Path | str = ".", *, origins: list[str] | None = None) -> FastAPI:
    lab = LabState(Path(workspace).resolve())

    @contextlib.asynccontextmanager
    async def lifespan(app: FastAPI):
        await lab.startup()
        try:
            yield
        finally:
            await lab.shutdown()

    app = FastAPI(
        title="PyRaft Lab API",
        version=__version__,
        description="HTTP access to the live cluster and the fault-injection laboratory.",
        lifespan=lifespan,
    )
    app.add_middleware(
        CORSMiddleware,
        allow_origins=origins if origins is not None else DEFAULT_ORIGINS,
        allow_credentials=False,
        allow_methods=["*"],
        allow_headers=["*"],
    )
    app.state.lab = lab
    app.include_router(_router(lab))
    return app


def _router(lab: LabState) -> APIRouter:  # noqa: C901 - a route table, not a branchy function
    api = APIRouter(prefix="/api")

    def cluster():
        try:
            return lab.require_cluster()
        except LabError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc

    def member(node_id: str):
        manager = cluster()
        if node_id not in manager.nodes:
            raise HTTPException(status_code=404, detail=f"no such node: {node_id}")
        return manager, manager.nodes[node_id]

    def live_node(node_id: str):
        manager, mgr_node = member(node_id)
        if mgr_node.node is None or not mgr_node.running:
            raise HTTPException(status_code=409, detail=f"{node_id} is down")
        return manager, mgr_node.node

    def client():
        manager = cluster()
        if manager.client is None:
            raise HTTPException(status_code=409, detail="the cluster has no client attached")
        return manager, manager.client

    # --- meta -------------------------------------------------------------------------

    @api.get("/health")
    async def health() -> dict[str, Any]:
        return {
            "ok": True,
            "version": __version__,
            "workspace": str(lab.workspace),
            "clusterRunning": lab.manager is not None,
        }

    @api.get("/config")
    async def get_config() -> dict[str, Any]:
        config = lab.base_config()
        low, high = config.election_timeout_range
        return {
            "path": str(lab.config_path),
            "saved": lab.config_path.exists(),
            "nodes": config.nodes,
            "persistent": config.data_dir is not None,
            "electionTimeoutMinMs": low * 1000,
            "electionTimeoutMaxMs": high * 1000,
            "heartbeatIntervalMs": config.heartbeat_interval * 1000,
            "clientTimeoutMs": config.client_timeout * 1000,
            "seed": config.seed,
        }

    @api.put("/config")
    async def put_config(body: ConfigBody) -> dict[str, Any]:
        if body.electionTimeoutMinMs >= body.electionTimeoutMaxMs:
            raise HTTPException(
                status_code=422, detail="the minimum election timeout must be below the maximum"
            )
        try:
            config = ClusterConfig(
                nodes=body.nodes,
                data_dir=lab.data_dir if body.persistent else None,
                election_timeout_range=(
                    body.electionTimeoutMinMs / 1000,
                    body.electionTimeoutMaxMs / 1000,
                ),
                heartbeat_interval=body.heartbeatIntervalMs / 1000,
                client_timeout=body.clientTimeoutMs / 1000,
                seed=body.seed,
            )
        except ClusterConfigError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc
        config.save(lab.config_path)
        return await get_config()

    # --- the live cluster ---------------------------------------------------------------

    @api.get("/cluster")
    async def get_cluster() -> dict[str, Any]:
        return lab.status_payload()

    @api.post("/cluster/start")
    async def start_cluster(body: StartClusterBody) -> dict[str, Any]:
        timeouts = None
        if body.electionTimeoutMinMs is not None and body.electionTimeoutMaxMs is not None:
            if body.electionTimeoutMinMs >= body.electionTimeoutMaxMs:
                raise HTTPException(
                    status_code=422,
                    detail="the minimum election timeout must be below the maximum",
                )
            timeouts = (body.electionTimeoutMinMs / 1000, body.electionTimeoutMaxMs / 1000)
        try:
            await lab.start_cluster(
                nodes=body.nodes,
                seed=body.seed,
                persistent=body.persistent,
                election_timeout_range=timeouts,
                heartbeat_interval=(
                    body.heartbeatIntervalMs / 1000
                    if body.heartbeatIntervalMs is not None
                    else None
                ),
                client_timeout=(
                    body.clientTimeoutMs / 1000 if body.clientTimeoutMs is not None else None
                ),
            )
        except LabError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        except LifecycleTimeout as exc:
            raise HTTPException(
                status_code=503, detail=f"the cluster never elected a leader: {exc}"
            ) from exc
        except (ClusterConfigError, ValueError) as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc
        return lab.status_payload()

    @api.post("/cluster/stop")
    async def stop_cluster() -> dict[str, Any]:
        await lab.stop_cluster()
        return lab.status_payload()

    @api.post("/cluster/exec")
    async def exec_command(body: ExecBody) -> dict[str, Any]:
        manager = cluster()
        try:
            output = await execute_command(manager, body.line)
        except (ReplCommandError, ReplParseError, ClusterError, RequestFailed) as exc:
            return {"ok": False, "output": str(exc)}
        except LifecycleTimeout as exc:
            return {"ok": False, "output": str(exc)}
        return {"ok": True, "output": output}

    @api.get("/cluster/nodes")
    async def get_nodes() -> list[dict[str, Any]]:
        manager = cluster()
        return [ser.node_state(manager, node_id) for node_id in manager.config.node_ids]

    @api.get("/cluster/nodes/{node_id}")
    async def get_node(node_id: str) -> dict[str, Any]:
        manager, _ = member(node_id)
        return ser.node_state(manager, node_id)

    @api.post("/cluster/nodes/{node_id}/restart")
    async def restart_node(node_id: str) -> dict[str, Any]:
        manager, _ = member(node_id)
        try:
            await manager.restart_node(node_id)
        except LifecycleTimeout as exc:
            raise HTTPException(
                status_code=503,
                detail=f"restarted {node_id}, but no leader was re-elected: {exc}",
            ) from exc
        except ClusterError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        return ser.node_state(manager, node_id)

    @api.post("/cluster/nodes/{node_id}/crash")
    async def crash_node(node_id: str) -> dict[str, Any]:
        manager, _ = member(node_id)
        try:
            await manager.crash_node(node_id)
        except ClusterError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        return ser.node_state(manager, node_id)

    @api.post("/cluster/nodes/{node_id}/recover")
    async def recover_node(node_id: str) -> dict[str, Any]:
        manager, _ = member(node_id)
        try:
            manager.recover_node(node_id)
        except ClusterError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        return ser.node_state(manager, node_id)

    @api.get("/cluster/nodes/{node_id}/log")
    async def get_node_log(node_id: str, limit: int | None = None) -> dict[str, Any]:
        _, node = live_node(node_id)
        return {
            "nodeId": node_id,
            "commitIndex": node.state.commit_index,
            "lastApplied": node.state.last_applied,
            "entries": ser.log_entries(node, limit=limit),
        }

    @api.get("/cluster/nodes/{node_id}/events")
    async def get_node_events(node_id: str, last: int = Query(default=50, ge=1, le=2000)):
        manager, _ = member(node_id)
        events = manager.tracer.for_node(node_id)[-last:]
        return [ser.event_row(event) for event in events]

    @api.get("/cluster/nodes/{node_id}/wal")
    async def get_node_wal(node_id: str) -> dict[str, Any]:
        from pyraft_lab.cli.main import _wal_records
        from pyraft_lab.raft.persistence import WalError, read_wal

        _, mgr_node = member(node_id)
        if mgr_node.wal_path is None:
            return {
                "path": None,
                "exists": False,
                "reason": "this cluster is in-memory - start it with persistence to get a WAL",
            }
        path = mgr_node.wal_path
        if not path.exists():
            return {"path": str(path), "exists": False, "reason": "nothing has been written yet"}
        try:
            recovery = read_wal(path)
        except WalError as exc:
            return {"path": str(path), "exists": True, "corrupt": True, "reason": str(exc)}
        return ser.wal_report(path, _wal_records(path), recovery)

    @api.get("/cluster/nodes/{node_id}/snapshots")
    async def get_node_snapshots(node_id: str) -> dict[str, Any]:
        _, mgr_node = member(node_id)
        node = mgr_node.node
        if node is None or node.snapshots is None:
            return {
                "nodeId": node_id,
                "supported": False,
                "reason": "this cluster is in-memory - start it with persistence for snapshots",
                "snapshots": [],
            }
        try:
            metas = node.snapshots.metas()
        except SnapshotError as exc:
            raise HTTPException(status_code=500, detail=str(exc)) from exc
        return {
            "nodeId": node_id,
            "supported": True,
            "directory": str(node.snapshots.directory),
            "snapshots": [ser.snapshot_meta(meta) for meta in metas],
        }

    @api.post("/cluster/nodes/{node_id}/snapshots")
    async def take_node_snapshot(node_id: str) -> dict[str, Any]:
        _, node = live_node(node_id)
        if node.snapshots is None:
            raise HTTPException(
                status_code=409,
                detail="this cluster is in-memory - start it with persistence for snapshots",
            )
        meta = node.take_snapshot()
        if meta is None:
            raise HTTPException(
                status_code=409,
                detail=f"{node_id} has applied nothing past its last snapshot - nothing to capture",
            )
        return ser.snapshot_meta(meta)

    @api.get("/cluster/log")
    async def get_cluster_log(node: str | None = None, limit: int | None = None):
        manager = cluster()
        node_id = node or manager.status()["leader"] or manager.config.node_ids[0]
        return await get_node_log(node_id, limit)

    @api.get("/cluster/trace")
    async def get_trace(last: int = Query(default=200, ge=1, le=5000)):
        manager = cluster()
        return [ser.event_row(event) for event in manager.tracer.events[-last:]]

    @api.get("/cluster/rpc")
    async def get_rpc(last: int = Query(default=60, ge=1, le=500)):
        manager = cluster()
        rows = [ser.rpc_row(event) for event in manager.tracer.events]
        return [row for row in rows if row is not None][-last:]

    # --- the key-value store ---------------------------------------------------------------

    @api.get("/kv")
    async def list_kv(node: str | None = None) -> dict[str, Any]:
        manager = cluster()
        node_id = node or manager.status()["leader"]
        if node_id is None or node_id not in manager.nodes:
            raise HTTPException(status_code=409, detail="the cluster has no leader to read from")
        _, raft = live_node(node_id)
        return {"nodeId": node_id, "records": ser.kv_records(raft)}

    @api.post("/kv")
    async def put_kv(body: PutBody) -> dict[str, Any]:
        _, kv = client()
        try:
            await kv.put(body.key, body.value)
        except RequestFailed as exc:
            raise HTTPException(status_code=503, detail=str(exc)) from exc
        return {"ok": True, "key": body.key, "value": body.value}

    @api.get("/kv/{key}")
    async def get_kv(key: str) -> dict[str, Any]:
        _, kv = client()
        try:
            value, found = await kv.read(key)
        except RequestFailed as exc:
            raise HTTPException(status_code=503, detail=str(exc)) from exc
        return {"key": key, "value": value, "found": found}

    @api.delete("/kv/{key}")
    async def delete_kv(key: str) -> dict[str, Any]:
        _, kv = client()
        try:
            existed = await kv.delete(key)
        except RequestFailed as exc:
            raise HTTPException(status_code=503, detail=str(exc)) from exc
        return {"ok": True, "key": key, "existed": existed}

    @api.post("/kv/cas")
    async def cas_kv(body: CasBody) -> dict[str, Any]:
        _, kv = client()
        try:
            won = await kv.cas(body.key, body.expected, body.new)
        except RequestFailed as exc:
            raise HTTPException(status_code=503, detail=str(exc)) from exc
        return {"ok": won, "key": body.key}

    # --- faults ---------------------------------------------------------------------------

    @api.get("/faults")
    async def get_faults() -> dict[str, Any]:
        return ser.faults(cluster())

    @api.post("/faults/partition")
    async def create_partition(body: PartitionBody) -> dict[str, Any]:
        manager = cluster()
        known = set(manager.config.node_ids)
        named = [node for group in body.groups for node in group]
        unknown = sorted(set(named) - known)
        if unknown:
            raise HTTPException(status_code=422, detail=f"no such node(s): {', '.join(unknown)}")
        if len(named) != len(set(named)):
            raise HTTPException(status_code=422, detail="a node cannot be in two groups")
        manager.network.partition(*body.groups)
        return ser.faults(manager)

    @api.post("/faults/heal")
    async def heal_partition() -> dict[str, Any]:
        manager = cluster()
        manager.network.heal()
        return ser.faults(manager)

    @api.post("/faults/latency")
    async def set_latency(body: LatencyBody) -> dict[str, Any]:
        if body.minMs > body.maxMs:
            raise HTTPException(status_code=422, detail="minMs must not exceed maxMs")
        manager = cluster()
        manager.network.set_latency(body.minMs / 1000, body.maxMs / 1000)
        return ser.faults(manager)

    @api.post("/faults/loss")
    async def set_loss(body: LossBody) -> dict[str, Any]:
        manager = cluster()
        manager.network.set_loss(body.probability)
        return ser.faults(manager)

    @api.post("/faults/reset")
    async def reset_faults() -> dict[str, Any]:
        manager = cluster()
        manager.network.reset_faults()
        return ser.faults(manager)

    # --- scenarios and the laboratory -------------------------------------------------------

    @api.get("/scenarios")
    async def list_scenarios() -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        for path in sorted(lab.scenarios_dir.glob("*.yaml")):
            try:
                scenario = Scenario.load(path)
            except ScenarioError as exc:
                rows.append({"file": path.name, "error": str(exc)})
                continue
            rows.append(
                {
                    "file": path.name,
                    "name": scenario.name,
                    "nodes": scenario.nodes,
                    "durationSec": scenario.duration,
                    "seed": scenario.seed,
                    "faultCount": len(scenario.faults),
                    "expected": scenario.expected,
                }
            )
        return rows

    @api.get("/scenarios/{name}")
    async def get_scenario(name: str) -> dict[str, Any]:
        path = _safe_child(lab.scenarios_dir, name)
        if not path.exists():
            raise HTTPException(status_code=404, detail=f"no scenario named {name}")
        try:
            scenario = Scenario.load(path)
        except ScenarioError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc
        return {
            "file": path.name,
            "yaml": path.read_text(encoding="utf-8"),
            "scenario": scenario.to_dict(),
        }

    @api.post("/experiments/run")
    async def run_experiment(body: RunScenarioBody) -> dict[str, Any]:
        path = _safe_child(lab.scenarios_dir, body.scenario)
        if not path.exists():
            raise HTTPException(status_code=404, detail=f"no scenario named {body.scenario}")
        try:
            scenario = Scenario.load(path)
        except ScenarioError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc

        results_dir = lab.results_dir
        plot_dir = (results_dir / "plots") if body.plot else None

        async def work(job: Job) -> dict[str, Any]:
            from pyraft_lab.experiments.metrics import report
            from pyraft_lab.experiments.replay import persist_run
            from pyraft_lab.experiments.runner import run_scenario

            job.progress(0, 1, f"running {scenario.name}")
            result = await run_scenario(scenario, seed=body.seed)
            job.log(f"{scenario.name}: {len(result.events)} events, run_id {result.run_id}")
            persist_run(result, results_dir)
            document = report(result)
            job.progress(1, 1, "done")

            plots: list[str] = []
            if plot_dir is not None:
                from pyraft_lab.experiments.visualization import plot_run

                plots = [str(p) for p in plot_run(result, plot_dir / result.run_id)]
            return {"runId": result.run_id, "report": document, "plots": plots}

        job = lab.jobs.start("scenario", {"scenario": path.name, "seed": body.seed}, work)
        return job.to_dict()

    @api.post("/campaigns/stress")
    async def run_stress_campaign(body: CampaignBody) -> dict[str, Any]:
        duration = _duration(body.duration)
        results_dir = lab.results_dir

        async def work(job: Job) -> dict[str, Any]:
            from pyraft_lab.experiments.stress import StressConfig, run_stress

            config = StressConfig(
                nodes=body.nodes,
                trial_duration=duration,
                trials=body.trials,
                base_seed=body.seed,
                results_dir=results_dir,
            )

            def on_trial(trial, done, total):
                mark = "PASS" if trial.passed else "FAIL"
                job.log(f"trial {trial.trial_index}: {trial.fault_kind:<22} {mark}")
                job.progress(done, total, f"trial {done}/{total}")

            job.progress(0, body.trials, "starting")
            campaign = await run_stress(config, on_trial=on_trial)
            return campaign.to_dict()

        job = lab.jobs.start("stress", body.model_dump(), work)
        return job.to_dict()

    @api.post("/campaigns/chaos")
    async def run_chaos_campaign(body: CampaignBody) -> dict[str, Any]:
        duration = _duration(body.duration)
        results_dir = lab.results_dir

        async def work(job: Job) -> dict[str, Any]:
            from pyraft_lab.experiments.chaos import ChaosConfig, run_chaos

            config = ChaosConfig(
                nodes=body.nodes,
                trial_duration=duration,
                trials=body.trials,
                base_seed=body.seed,
                results_dir=results_dir,
            )

            def on_trial(trial, done, total):
                mark = "PASS" if trial.passed else "FAIL"
                job.log(
                    f"trial {trial.trial_index}: {trial.fault_count} faults  "
                    f"lost={trial.lost_write_count}  {mark}"
                )
                job.progress(done, total, f"trial {done}/{total}")

            job.progress(0, body.trials, "starting")
            campaign = await run_chaos(config, on_trial=on_trial)
            return campaign.to_dict()

        job = lab.jobs.start("chaos", body.model_dump(), work)
        return job.to_dict()

    @api.post("/campaigns/benchmark")
    async def run_benchmark_campaign(body: BenchmarkBody) -> dict[str, Any]:
        duration = _duration(body.duration)
        results_dir = lab.results_dir
        output_dir = lab.results_dir / "benchmark"

        async def work(job: Job) -> dict[str, Any]:
            from pyraft_lab.experiments.benchmark import BenchmarkConfig, run_benchmark
            from pyraft_lab.experiments.visualization import plot_benchmark

            config = BenchmarkConfig(
                node_counts=tuple(sorted(set(body.nodeCounts))),
                loss_rates=tuple(sorted({p / 100 for p in body.lossPercents})),
                trial_duration=duration,
                base_seed=body.seed,
                results_dir=results_dir,
            )
            total = len(config.node_counts) * len(config.loss_rates)

            def on_cell(cell, done, count):
                job.log(
                    f"{cell.nodes} nodes @ {cell.loss_rate * 100:.0f}% loss: "
                    f"{cell.throughput_per_sec:.1f} ops/s"
                )
                job.progress(done, count, f"cell {done}/{count}")

            job.progress(0, total, "starting")
            campaign = await run_benchmark(config, on_cell=on_cell)
            graphs = [str(p) for p in plot_benchmark(campaign, output_dir)]
            return {**campaign.to_dict(), "graphs": graphs}

        job = lab.jobs.start("benchmark", body.model_dump(), work)
        return job.to_dict()

    # --- persisted runs -------------------------------------------------------------------

    @api.get("/results")
    async def list_results() -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        if not lab.results_dir.exists():
            return rows
        for run_dir in sorted(lab.results_dir.iterdir()):
            manifest_path = run_dir / MANIFEST_FILE
            if not manifest_path.exists():
                continue
            try:
                manifest = RunManifest.load(manifest_path)
            except ManifestError as exc:
                rows.append({"runId": run_dir.name, "error": str(exc)})
                continue

            summary: dict[str, Any] = {}
            report_path = run_dir / REPORT_FILE
            if report_path.exists():
                document = json.loads(report_path.read_text(encoding="utf-8"))
                consistency = document.get("consistency") or {}
                summary = {
                    "passed": document.get("passed"),
                    "availability": document["summary"]["availability"],
                    "throughputPerSec": document["summary"]["throughput_per_sec"],
                    "dataLoss": document["summary"]["data_loss"],
                    "totalRequests": document["summary"]["total_requests"],
                    "p99CommitLatencyMs": document["timing"]["p99_commit_latency_ms"],
                    "totalElections": document["leadership"]["total_elections"],
                    "linearizable": consistency.get("linearizable"),
                }
            rows.append(
                {
                    "runId": manifest.run_id,
                    "scenarioName": manifest.scenario_name,
                    "seed": manifest.seed,
                    "createdAt": manifest.created_at,
                    **summary,
                }
            )
        return sorted(rows, key=lambda row: row.get("createdAt") or "", reverse=True)

    @api.get("/results/{run_id}")
    async def get_result(run_id: str) -> dict[str, Any]:
        run_dir = _run_dir(lab, run_id)
        manifest_path = run_dir / MANIFEST_FILE
        report_path = run_dir / REPORT_FILE
        payload: dict[str, Any] = {"runId": run_id}
        if manifest_path.exists():
            payload["manifest"] = json.loads(manifest_path.read_text(encoding="utf-8"))
        if report_path.exists():
            payload["report"] = json.loads(report_path.read_text(encoding="utf-8"))
        if "manifest" not in payload and "report" not in payload:
            raise HTTPException(status_code=404, detail=f"nothing readable in run {run_id}")
        return payload

    @api.get("/results/{run_id}/events")
    async def get_result_events(run_id: str, last: int = Query(default=500, ge=1, le=20000)):
        from pyraft_lab.observability.events import read_jsonl

        path = _run_dir(lab, run_id) / EVENTS_FILE
        if not path.exists():
            raise HTTPException(status_code=404, detail=f"no event trace for run {run_id}")
        return [ser.event_row(event) for event in read_jsonl(path)[-last:]]

    @api.post("/results/{run_id}/replay")
    async def replay_result(run_id: str) -> dict[str, Any]:
        _run_dir(lab, run_id)
        results_dir = lab.results_dir

        async def work(job: Job) -> dict[str, Any]:
            from pyraft_lab.experiments.replay import replay_run

            job.progress(0, 1, f"replaying {run_id}")
            comparison, _ = await replay_run(results_dir, run_id)
            job.log(comparison.explain())
            job.progress(1, 1, "done")
            return {
                "runId": run_id,
                "matched": comparison.matched,
                "explanation": comparison.explain(),
            }

        job = lab.jobs.start("replay", {"runId": run_id}, work)
        return job.to_dict()

    # --- jobs ---------------------------------------------------------------------------

    @api.get("/jobs")
    async def list_jobs(kind: str | None = None) -> list[dict[str, Any]]:
        jobs = lab.jobs.all()
        if kind:
            jobs = [job for job in jobs if job.kind == kind]
        return [job.to_dict() for job in jobs]

    @api.get("/jobs/{job_id}")
    async def get_job(job_id: str) -> dict[str, Any]:
        job = lab.jobs.get(job_id)
        if job is None:
            raise HTTPException(status_code=404, detail=f"no such job: {job_id}")
        return job.to_dict()

    @api.post("/jobs/{job_id}/cancel")
    async def cancel_job(job_id: str) -> dict[str, Any]:
        job = lab.jobs.get(job_id)
        if job is None:
            raise HTTPException(status_code=404, detail=f"no such job: {job_id}")
        job.cancel()
        return job.to_dict()

    # --- documentation --------------------------------------------------------------------

    @api.get("/docs-pages")
    async def list_docs() -> list[dict[str, str]]:
        return [{"name": name, "title": title} for name, (title, _) in _DOC_PAGES.items()]

    @api.get("/docs-pages/{name}")
    async def get_doc(name: str) -> dict[str, str]:
        entry = _DOC_PAGES.get(name)
        if entry is None:
            raise HTTPException(status_code=404, detail=f"no such document: {name}")
        title, relative = entry
        path = _project_root() / relative
        if not path.exists():
            raise HTTPException(status_code=404, detail=f"{relative} is not in this checkout")
        return {"name": name, "title": title, "markdown": path.read_text(encoding="utf-8")}

    # --- the live stream --------------------------------------------------------------------

    @api.websocket("/stream")
    async def stream(socket: WebSocket) -> None:
        await socket.accept()
        with lab.subscribe() as queue:
            await socket.send_json({"channel": "hello", "payload": lab.status_payload()})
            try:
                while True:
                    message = await queue.get()
                    await socket.send_text(message)
            except WebSocketDisconnect:
                return
            except RuntimeError:
                # The socket closed under us between the get and the send.
                return

    return api


# --- helpers ------------------------------------------------------------------------------


#: Documents the UI can render. A fixed table rather than a directory walk: this serves
#: file contents to a browser, and the set of files it may serve should be a decision in
#: the source, not whatever happens to be on disk.
_DOC_PAGES = {
    "readme": ("Project README", Path("README.md")),
    "architecture": ("Architecture decisions", Path("docs/architecture.md")),
    "technical-report": ("Technical report", Path("docs/technical_report.md")),
    "plan": ("Implementation plan", Path("docs/plan.md")),
}


def _project_root() -> Path:
    """The ``pyraft_lab`` checkout root - four levels up from this file."""
    return Path(__file__).resolve().parents[3]


def _safe_child(directory: Path, name: str) -> Path:
    """``directory/name``, refusing anything that escapes ``directory``.

    The name arrives from a URL, so ``../../etc/passwd`` has to be a 400 rather than a
    read - resolving both sides and checking containment is what makes that structural
    instead of a blocklist of separators.
    """
    candidate = (directory / name).resolve()
    root = directory.resolve()
    if root != candidate and root not in candidate.parents:
        raise HTTPException(status_code=400, detail=f"{name!r} is not inside {directory.name}/")
    return candidate


def _run_dir(lab: LabState, run_id: str) -> Path:
    path = _safe_child(lab.results_dir, run_id)
    if not path.is_dir():
        raise HTTPException(status_code=404, detail=f"no such run: {run_id}")
    return path


def _duration(value: str) -> float:
    try:
        return parse_duration(value, what="duration")
    except ScenarioError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
