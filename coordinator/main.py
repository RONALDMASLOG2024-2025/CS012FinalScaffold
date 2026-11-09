from __future__ import annotations

import asyncio
import json
import os
import time
import uuid
from datetime import datetime
from pathlib import Path
from urllib.parse import quote

import httpx
from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import FileResponse, PlainTextResponse
from fastapi.staticfiles import StaticFiles

from common.logger import (
    generate_trace_id,
    generation_var,
    get_logger,
    parent_span_id_var,
    request_id_var,
    span_id_var,
    trace_id_var,
)
from common.models import (
    DataPayload,
    DistributedResponse,
    ReplicaUpdate,
    ResidentRecord,
    SenseRequest,
    SimulationControl,
    SimulationEvent,
    SimulationFaults,
)
from common.pipeline import abnormality_tags, alert_summary, classify_urgency, fuse_links, scenario_catalog
from common.simulation import EventStore, SimulationClock, create_event

app = FastAPI(
    title="WiCare Ward Gateway",
    description=(
        "Entry point for the WiCare cluster. A sensing request is traced from this gateway "
        "to the private transmitter, then to both receivers in parallel. "
        "Urgency is decided here from the fused estimates."
    ),
    version="1.0.0",
)
logger = get_logger("Coordinator")
WEB_DIR = Path(__file__).resolve().parents[1] / "web"
app.mount("/assets", StaticFiles(directory=str(WEB_DIR)), name="assets")

TX_URL = os.getenv("TX_URL", "http://localhost:8001").rstrip("/")
RX_A_URL = os.getenv("RX_A_URL", "http://localhost:8002").rstrip("/")
RX_B_URL = os.getenv("RX_B_URL", "http://localhost:8003").rstrip("/")
WORKER_NODES = [TX_URL, RX_A_URL, RX_B_URL]
NODE_URLS = {
    "Worker-TX": TX_URL,
    "Worker-RX-A": RX_A_URL,
    "Worker-RX-B": RX_B_URL,
}
NODE_IDS = tuple(NODE_URLS)
RX_BY_ID = {
    "Worker-RX-A": RX_A_URL,
    "Worker-RX-B": RX_B_URL,
}
MAX_NODE_ATTEMPTS = 2
simulation_clock = SimulationClock()
event_store = EventStore(simulation_clock)
failed_nodes: set[str] = set()
network_partitions: set[frozenset[str]] = set()
replication_pending: set[str] = set()
telemetry_versions: dict[str, int] = {}
active_request_tasks: dict[str, asyncio.Task] = {}


class NodeUnavailable(Exception):
    pass


def _new_span_id() -> str:
    return uuid.uuid4().hex[:16]


def _emit_event(
    event_type: str,
    *,
    operation: str,
    status: str,
    message: str,
    node: str = "Coordinator",
    source: str | None = None,
    destination: str | None = None,
    duration_ms: float | None = None,
    attributes: dict | None = None,
    event_span_id: str | None = None,
    event_parent_span_id: str | None = None,
    generation: int | None = None,
) -> dict | None:
    trace_id = trace_id_var.get()
    request_id = request_id_var.get()
    current_span_id = span_id_var.get()
    current_parent_span_id = parent_span_id_var.get()
    event = create_event(
        event_type,
        trace_id=trace_id,
        request_id=request_id,
        span_id=event_span_id or (
            current_span_id if current_span_id != "NO_SPAN_ID" else None
        ),
        parent_span_id=event_parent_span_id or (
            current_parent_span_id
            if current_parent_span_id != "NO_PARENT_SPAN_ID"
            else None
        ),
        generation=generation_var.get() if generation is None else generation,
        service="Coordinator",
        node=node,
        operation=operation,
        status=status,
        message=message,
        source=source,
        destination=destination,
        duration_ms=duration_ms,
        attributes=attributes,
    )
    stored = event_store.record(event)
    if stored is not None:
        logger.info(
            "%s | request=%s | %s",
            event_type,
            request_id,
            message,
            extra={"trace_id": trace_id},
        )
    return stored


def _unavailable_reason(node_id: str, source: str = "Coordinator") -> str | None:
    if node_id in failed_nodes:
        return "simulated_node_failure"
    if frozenset({source, node_id}) in network_partitions:
        return "network_partition"
    return None


def _headers(zone: str, span_id: str | None = None) -> dict[str, str]:
    return {
        "X-Trace-ID": trace_id_var.get(),
        "X-Request-ID": request_id_var.get(),
        "X-Span-ID": span_id or span_id_var.get(),
        "X-Simulation-Generation": str(generation_var.get()),
        "X-Network-Zone": zone,
    }


async def _node_request(
    client: httpx.AsyncClient,
    *,
    node_id: str,
    method: str,
    path: str,
    operation: str,
    network_zone: str = "EDGE_COMPUTE",
    payload: dict | None = None,
    timeout_s: float = 10.0,
    respect_faults: bool = True,
) -> httpx.Response:
    base_url = NODE_URLS.get(node_id)
    if base_url is None:
        raise NodeUnavailable(f"No route is configured for {node_id}.")

    call_span_id = _new_span_id()
    parent = span_id_var.get()
    parent_id = parent if parent != "NO_SPAN_ID" else None
    last_error = "request failed"
    for attempt in range(1, MAX_NODE_ATTEMPTS + 1):
        started = time.perf_counter()
        _emit_event(
            "message.sent",
            operation=operation,
            status="sent",
            message=f"Sending {method} {path} to {node_id} (attempt {attempt}).",
            node=node_id,
            source="Coordinator",
            destination=node_id,
            attributes={"attempt": attempt, "method": method, "path": path},
            event_span_id=call_span_id,
            event_parent_span_id=parent_id,
        )

        unavailable = _unavailable_reason(node_id) if respect_faults else None
        if unavailable is not None:
            await simulation_clock.sleep(simulation_clock.network_latency_ms / 1000)
            last_error = f"{node_id} is unavailable ({unavailable})."
            _emit_event(
                "message.dropped",
                operation=operation,
                status=unavailable,
                message=f"Request to {node_id} was dropped: {unavailable}.",
                node=node_id,
                source="Coordinator",
                destination=node_id,
                duration_ms=(time.perf_counter() - started) * 1000,
                attributes={"attempt": attempt, "cause": unavailable},
                event_span_id=call_span_id,
                event_parent_span_id=parent_id,
            )
            if attempt == 1:
                _emit_event(
                    "failure.detected",
                    operation=operation,
                    status="unavailable",
                    message=f"Coordinator detected {node_id} as unavailable.",
                    node=node_id,
                    source="Coordinator",
                    destination=node_id,
                    attributes={"cause": unavailable},
                    event_span_id=call_span_id,
                    event_parent_span_id=parent_id,
                )
            if attempt < MAX_NODE_ATTEMPTS:
                _emit_event(
                    "retry.scheduled",
                    operation=operation,
                    status="retrying",
                    message=f"Retrying {node_id} once after a bounded backoff.",
                    node=node_id,
                    source="Coordinator",
                    destination=node_id,
                    attributes={"attempt": attempt, "max_attempts": MAX_NODE_ATTEMPTS},
                    event_span_id=call_span_id,
                    event_parent_span_id=parent_id,
                )
                await simulation_clock.sleep(0.1)
                continue
            _emit_event(
                "timeout",
                operation=operation,
                status="failure",
                message=f"Bounded retries exhausted for {node_id}.",
                node=node_id,
                source="Coordinator",
                destination=node_id,
                duration_ms=(time.perf_counter() - started) * 1000,
                attributes={"attempts": attempt, "cause": unavailable},
                event_span_id=call_span_id,
                event_parent_span_id=parent_id,
            )
            raise NodeUnavailable(last_error)

        try:
            await simulation_clock.sleep(simulation_clock.network_latency_ms / 1000)
            response = await client.request(
                method,
                f"{base_url}{path}",
                json=payload,
                headers=_headers(network_zone, call_span_id),
                timeout=timeout_s,
            )
            await simulation_clock.sleep(simulation_clock.network_latency_ms / 1000)
        except httpx.RequestError as exc:
            reason = str(exc) or type(exc).__name__
            last_error = f"{node_id} request failed: {reason}"
            event_type = "timeout" if isinstance(exc, httpx.TimeoutException) else "message.failed"
            _emit_event(
                event_type,
                operation=operation,
                status="failure",
                message=last_error,
                node=node_id,
                source="Coordinator",
                destination=node_id,
                duration_ms=(time.perf_counter() - started) * 1000,
                attributes={"attempt": attempt},
                event_span_id=call_span_id,
                event_parent_span_id=parent_id,
            )
            if attempt < MAX_NODE_ATTEMPTS:
                _emit_event(
                    "retry.scheduled",
                    operation=operation,
                    status="retrying",
                    message=f"Retrying {node_id} once after a bounded backoff.",
                    node=node_id,
                    source="Coordinator",
                    destination=node_id,
                    attributes={"attempt": attempt, "max_attempts": MAX_NODE_ATTEMPTS},
                    event_span_id=call_span_id,
                    event_parent_span_id=parent_id,
                )
                await simulation_clock.sleep(0.1)
                continue
            raise NodeUnavailable(last_error) from exc

        _emit_event(
            "message.received",
            operation=operation,
            status="success" if response.status_code < 400 else "error",
            message=f"{node_id} returned HTTP {response.status_code}.",
            node=node_id,
            source=node_id,
            destination="Coordinator",
            duration_ms=(time.perf_counter() - started) * 1000,
            attributes={"attempt": attempt, "http_status": response.status_code},
            event_span_id=call_span_id,
            event_parent_span_id=parent_id,
        )
        if response.status_code >= 500 and attempt < MAX_NODE_ATTEMPTS:
            last_error = f"{node_id} returned HTTP {response.status_code}."
            _emit_event(
                "retry.scheduled",
                operation=operation,
                status="retrying",
                message=f"Retrying {node_id} after HTTP {response.status_code}.",
                node=node_id,
                source="Coordinator",
                destination=node_id,
                attributes={"attempt": attempt, "max_attempts": MAX_NODE_ATTEMPTS},
                event_span_id=call_span_id,
                event_parent_span_id=parent_id,
            )
            await simulation_clock.sleep(0.1)
            continue
        if response.status_code >= 500:
            raise NodeUnavailable(
                f"{node_id} returned HTTP {response.status_code} after bounded retries."
            )
        return response

    raise NodeUnavailable(last_error)


async def _push_control_to_workers(
    path: str,
    payload: dict,
    *,
    record_events: bool = True,
) -> list[dict]:
    async with httpx.AsyncClient(timeout=2.0) as client:
        async def send(node_id: str) -> dict:
            span = _new_span_id()
            started = time.perf_counter()
            if record_events:
                _emit_event(
                    "message.sent",
                    operation="simulation_control",
                    status="sent",
                    message=f"Sent control command to {node_id}.",
                    node=node_id,
                    source="Coordinator",
                    destination=node_id,
                    attributes={"path": path},
                    event_span_id=span,
                )
            try:
                response = await client.post(
                    f"{NODE_URLS[node_id]}{path}",
                    json=payload,
                    headers=_headers("EDGE_COMPUTE", span),
                )
            except httpx.RequestError as exc:
                if record_events:
                    _emit_event(
                        "message.failed",
                        operation="simulation_control",
                        status="failure",
                        message=f"Control command to {node_id} failed: {exc}.",
                        node=node_id,
                        source="Coordinator",
                        destination=node_id,
                        duration_ms=(time.perf_counter() - started) * 1000,
                        attributes={"path": path},
                        event_span_id=span,
                    )
                return {"node_id": node_id, "status": "UNREACHABLE", "error": str(exc)}
            if record_events:
                _emit_event(
                    "message.received",
                    operation="simulation_control",
                    status="success" if response.status_code < 400 else "error",
                    message=f"{node_id} accepted control command with HTTP {response.status_code}.",
                    node=node_id,
                    source=node_id,
                    destination="Coordinator",
                    duration_ms=(time.perf_counter() - started) * 1000,
                    attributes={"path": path, "http_status": response.status_code},
                    event_span_id=span,
                )
            if response.is_error:
                return {
                    "node_id": node_id,
                    "status": "ERROR",
                    "error": f"HTTP {response.status_code}: {response.text}",
                }
            return {"node_id": node_id, "status": "OK", "data": response.json()}

        return await asyncio.gather(*(send(node_id) for node_id in NODE_IDS))
ZONE_PRIMARY = {
    "zone-a": "Worker-RX-A",
    "zone-b": "Worker-RX-B",
}

ARCHITECTURE = {
    "system": "WiCare",
    "sensing": "Wi-Fi CSI vital signs for one resident at a time",
    "topology": "Gateway star plus a direct receiver-to-receiver telemetry replication link.",
    "cloud_deployment_model": (
        "Hybrid deployment: private facility services retain identity, raw CSI, and alerts; "
        "edge receiver services process each zone; a cloud-hosted gateway/observability tier "
        "is the logical deployment target. All services are simulated locally."
    ),
    "placement": {
        "Worker-TX": "Private facility node. Holds resident identity, raw CSI, and the alert archive.",
        "Worker-RX-A": "Edge compute for zone-a (bed 1, about 2 m from the transmitter).",
        "Worker-RX-B": "Edge compute for zone-b (bed 2, about 2 m from the transmitter).",
        "Coordinator": "Gateway. Routes work, fuses the two links, and applies urgency rules.",
    },
    "data_distribution": {
        "resident_identity_raw_csi_alerts": "Single private transmitter store; not replicated.",
        "zone_a_zone_b_storage": "Deterministic zone routing (logical sharding).",
        "telemetry_namespace": (
            "Non-sensitive telemetry: two edge replicas, local-write availability, "
            "versioned last-writer-wins reconciliation after a partition heals."
        ),
    },
    "resilience": (
        "Two bounded attempts for gateway-to-node calls; a surviving receiver can return "
        "a degraded sensing result; telemetry replicas reconcile after recovery."
    ),
    "pipeline": [
        "Transmitter builds a CSI window: empty-room baseline plus breathing and heartbeat.",
        "Each receiver removes the empty-room baseline, keeps the strongest subcarriers, and estimates heart rate, breathing, movement, and a fall burst.",
        "The gateway fuses the two estimates. A weak neighbor link is not averaged in.",
        "Rules assign Emergency, Warning, or Normal. The sentence underneath only restates that label.",
    ],
    "nodes": [
        {"id": "Coordinator", "url": "http://localhost:8000", "role": "gateway"},
        {"id": "Worker-TX", "url": TX_URL, "role": "transmitter", "zone_id": "facility", "private": True},
        {"id": "Worker-RX-A", "url": RX_A_URL, "role": "receiver", "zone_id": "zone-a", "private": False},
        {"id": "Worker-RX-B", "url": RX_B_URL, "role": "receiver", "zone_id": "zone-b", "private": False},
    ],
}


@app.middleware("http")
async def trace_middleware(request: Request, call_next):
    trace_id = request.headers.get("X-Trace-ID") or generate_trace_id()
    request_id = request.headers.get("X-Request-ID") or uuid.uuid4().hex[:16]
    parent_span_id = request.headers.get("X-Span-ID")
    root_span_id = _new_span_id()
    request.state.trace_id = trace_id
    request.state.request_id = request_id
    request.state.span_id = root_span_id

    trace_token = trace_id_var.set(trace_id)
    request_token = request_id_var.set(request_id)
    span_token = span_id_var.set(root_span_id)
    parent_token = parent_span_id_var.set(parent_span_id or "NO_PARENT_SPAN_ID")
    generation_token = generation_var.set(event_store.generation)
    path = request.url.path
    is_internal_events = path == "/api/v1/events/ingest"
    is_observed = path.startswith("/api/v1/") and not is_internal_events and path not in {
        "/api/v1/events",
        "/api/v1/events/export",
        "/api/v1/metrics",
        "/api/v1/simulation/state",
        "/api/v1/simulation/reset",
    } and not path.startswith("/api/v1/traces/")
    should_track = path in {
        "/api/v1/sense",
        "/api/v1/write",
        "/api/v1/simulation/control",
        "/api/v1/simulation/faults",
        "/api/v1/simulation/recover",
    } or path.startswith("/api/v1/telemetry/")
    started = time.perf_counter()
    if should_track:
        active_request_tasks[request_id] = asyncio.current_task()
    if is_observed:
        _emit_event(
            "request.received",
            operation=f"{request.method} {path}",
            status="received",
            message=f"Gateway received {request.method} {path}.",
            source="Client",
            destination="Coordinator",
        )
    try:
        response = await call_next(request)
        if is_observed:
            is_failure = response.status_code >= 400
            result_status = getattr(
                request.state,
                "simulation_outcome",
                "failure" if is_failure else "success",
            )
            _emit_event(
                "request.failed" if is_failure else "request.completed",
                operation=f"{request.method} {path}",
                status="failure" if is_failure else result_status,
                message=(
                    f"Gateway request {request.method} {path} failed with HTTP {response.status_code}."
                    if is_failure
                    else f"Gateway completed {request.method} {path} ({result_status})."
                ),
                source="Coordinator",
                destination="Client",
                duration_ms=(time.perf_counter() - started) * 1000,
                attributes={"http_status": response.status_code},
            )
        response.headers["X-Trace-ID"] = trace_id
        response.headers["X-Request-ID"] = request_id
        return response
    finally:
        active_request_tasks.pop(request_id, None)
        generation_var.reset(generation_token)
        parent_span_id_var.reset(parent_token)
        span_id_var.reset(span_token)
        request_id_var.reset(request_token)
        trace_id_var.reset(trace_token)


def _route_storage(key: str, is_sensitive: bool) -> tuple[str, str]:
    """Clinical records stay on the transmitter. Zone telemetry stays on that zone's receiver."""
    if is_sensitive or key.startswith("resident:") or key.startswith("alert:"):
        return TX_URL, "PRIVATE_VPC"
    if key.startswith("telemetry:"):
        digest = sum(ord(char) for char in key)
        target = (RX_A_URL, RX_B_URL)[digest % 2]
        return target, "EDGE_COMPUTE"
    if key.startswith("zone-a"):
        return RX_A_URL, "EDGE_COMPUTE"
    if key.startswith("zone-b"):
        return RX_B_URL, "EDGE_COMPUTE"
    digest = sum(ord(char) for char in key)
    target = (RX_A_URL, RX_B_URL)[digest % 2]
    return target, "EDGE_COMPUTE"


@app.get("/")
async def root():
    return {
        "service": "WiCare Ward Gateway",
        "demo": "/demo",
        "docs": "/docs",
        "topology": "/cluster/topology",
        "health": "/cluster/health",
        "sense": "/api/v1/sense",
    }


@app.get("/demo", response_class=FileResponse)
async def demo_dashboard():
    return FileResponse(WEB_DIR / "index.html")


@app.get("/cluster/health")
async def cluster_health():
    headers = _headers("EDGE_COMPUTE")
    async with httpx.AsyncClient(timeout=2.0) as client:
        tasks = [client.get(f"{node}/health", headers=headers) for node in WORKER_NODES]
        results = await asyncio.gather(*tasks, return_exceptions=True)

    status_report = {}
    id_by_url = {url: node_id for node_id, url in NODE_URLS.items()}
    for node, resp in zip(WORKER_NODES, results):
        if isinstance(resp, Exception):
            status_report[node] = {"status": "UNREACHABLE", "error": str(resp)}
        else:
            report = resp.json()
            node_id = id_by_url[node]
            if node_id in failed_nodes:
                report["physical_status"] = report["status"]
                report["status"] = "SIMULATED_DOWN"
                report["fault"] = "Node failure injected at the gateway."
            elif frozenset({"Coordinator", node_id}) in network_partitions:
                report["physical_status"] = report["status"]
                report["status"] = "ISOLATED"
                report["fault"] = "Gateway-to-node link is partitioned."
            status_report[node] = report
    return {"cluster_status": status_report, "trace_id": trace_id_var.get()}


@app.get("/cluster/topology")
async def cluster_topology():
    live = await cluster_health()
    return {"architecture": ARCHITECTURE, "live": live}


@app.get("/api/v1/scenarios")
async def list_scenarios():
    return {"scenarios": scenario_catalog()}


@app.get("/api/v1/events")
async def list_events(
    after: int = Query(default=0, ge=0),
    trace_id: str | None = None,
    request_id: str | None = None,
    limit: int = Query(default=500, ge=1, le=1000),
):
    return {
        "generation": event_store.generation,
        "events": event_store.query(
            after=after,
            trace_id=trace_id,
            request_id=request_id,
            limit=limit,
        ),
    }


@app.get("/api/v1/events/export", response_class=PlainTextResponse)
async def export_events(
    trace_id: str | None = None,
    request_id: str | None = None,
):
    events = event_store.query(trace_id=trace_id, request_id=request_id, limit=5000)
    content = "\n".join(json.dumps(event, separators=(",", ":"), ensure_ascii=True) for event in events)
    return PlainTextResponse(
        content=content,
        media_type="application/x-ndjson",
        headers={"Content-Disposition": 'attachment; filename="wicare-events.jsonl"'},
    )


@app.post("/api/v1/events/ingest")
async def ingest_node_event(body: SimulationEvent):
    stored = event_store.record(body.model_dump())
    return {
        "accepted": stored is not None,
        "sequence": stored["sequence"] if stored is not None else None,
    }


@app.get("/api/v1/metrics")
async def get_metrics():
    metrics = event_store.metrics(queue_depth=len(replication_pending))
    metrics["replication_pending"] = len(replication_pending)
    metrics["generation"] = event_store.generation
    return metrics


@app.get("/api/v1/traces/{trace_id}")
async def get_trace(
    trace_id: str,
    request_id: str | None = None,
):
    events = event_store.query(trace_id=trace_id, request_id=request_id, limit=5000)
    if not events:
        raise HTTPException(status_code=404, detail="No events found for this trace.")
    return {
        "trace_id": trace_id,
        "request_id": request_id,
        "event_count": len(events),
        "events": events,
    }


@app.get("/api/v1/simulation/state")
async def simulation_state():
    return {
        "simulation": simulation_clock.snapshot(),
        "faults": {
            "failed_nodes": sorted(failed_nodes),
            "partitions": [sorted(pair) for pair in sorted(network_partitions, key=lambda x: sorted(x))],
        },
        "generation": event_store.generation,
        "pending_replications": sorted(replication_pending),
        "queue_depth": len(replication_pending),
    }


@app.post("/api/v1/simulation/control")
async def update_simulation_control(body: SimulationControl):
    simulation_clock.configure(
        paused=body.paused,
        speed=body.speed,
        network_latency_ms=body.network_latency_ms,
        processing_latency_ms=body.processing_latency_ms,
    )
    control = {
        **simulation_clock.snapshot(),
        "partitions": [list(pair) for pair in sorted(network_partitions, key=lambda x: sorted(x))],
        "generation": event_store.generation,
    }
    results = await _push_control_to_workers("/simulation/control", control)
    warnings = [row for row in results if row["status"] != "OK"]
    _emit_event(
        "simulation.control",
        operation="configure_simulation",
        status="degraded" if warnings else "success",
        message="Updated simulation clock and latency settings.",
        attributes={"control": control, "warnings": warnings},
    )
    return {
        "status": "DEGRADED" if warnings else "SUCCESS",
        "simulation": simulation_clock.snapshot(),
        "workers": results,
    }


@app.post("/api/v1/simulation/faults")
async def inject_faults(body: SimulationFaults):
    global network_partitions
    allowed_nodes = set(NODE_IDS)
    unknown_nodes = set(body.failed_nodes) - allowed_nodes
    if unknown_nodes:
        raise HTTPException(
            status_code=400,
            detail=f"Unknown node IDs: {', '.join(sorted(unknown_nodes))}.",
        )

    normalized_partitions: set[frozenset[str]] = set()
    for left, right in body.partitions:
        if left == right or left not in allowed_nodes or right not in allowed_nodes:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid partition link: {left} <-> {right}.",
            )
        normalized_partitions.add(frozenset({left, right}))

    failed_nodes.clear()
    failed_nodes.update(body.failed_nodes)
    network_partitions = normalized_partitions
    control = {
        **simulation_clock.snapshot(),
        "partitions": [list(pair) for pair in sorted(network_partitions, key=lambda x: sorted(x))],
        "generation": event_store.generation,
    }
    workers = await _push_control_to_workers("/simulation/control", control)
    warnings = [row for row in workers if row["status"] != "OK"]
    _emit_event(
        "simulation.fault_injected",
        operation="fault_injection",
        status="degraded" if warnings else "active",
        message="Applied node failure and network partition settings.",
        attributes={
            "failed_nodes": sorted(failed_nodes),
            "partitions": control["partitions"],
            "warnings": warnings,
        },
    )
    return {
        "status": "DEGRADED" if warnings else "ACTIVE",
        "faults": {
            "failed_nodes": sorted(failed_nodes),
            "partitions": control["partitions"],
        },
        "workers": workers,
    }


@app.post("/api/v1/simulation/recover")
async def recover_simulation():
    failed_nodes.clear()
    network_partitions.clear()
    simulation_clock.configure(paused=False)
    _emit_event(
        "recovery.started",
        operation="recover_simulation",
        status="started",
        message="Healing simulated links and restoring node availability.",
    )
    control = {
        **simulation_clock.snapshot(),
        "partitions": [],
        "generation": event_store.generation,
    }
    workers = await _push_control_to_workers("/simulation/control", control)
    sync_results = []
    errors = [row for row in workers if row["status"] != "OK"]
    if not errors:
        async with httpx.AsyncClient(timeout=5.0) as client:
            for node_id in ("Worker-RX-A", "Worker-RX-B"):
                try:
                    response = await _node_request(
                        client,
                        node_id=node_id,
                        method="POST",
                        path="/telemetry/sync",
                        operation="anti_entropy_sync",
                    )
                except NodeUnavailable as exc:
                    errors.append({"node_id": node_id, "error": str(exc)})
                    continue
                if response.is_error:
                    errors.append(
                        {"node_id": node_id, "error": f"HTTP {response.status_code}: {response.text}"}
                    )
                else:
                    sync_results.append(response.json())

    converged = []
    if not errors and replication_pending:
        async with httpx.AsyncClient(timeout=5.0) as client:
            for key in sorted(replication_pending):
                versions = []
                for node_id in ("Worker-RX-A", "Worker-RX-B"):
                    try:
                        response = await _node_request(
                            client,
                            node_id=node_id,
                            method="GET",
                            path=f"/telemetry/read/{quote(key, safe=':')}",
                            operation="replica_verification",
                        )
                    except NodeUnavailable:
                        versions = []
                        break
                    if response.status_code == 200:
                        row = response.json()
                        versions.append((row["version"], row["writer_id"]))
                if len(versions) == 2 and versions[0] == versions[1]:
                    replication_pending.discard(key)
                    converged.append(key)
                    _emit_event(
                        "queue.dequeued",
                        operation="replica_repair",
                        status="completed",
                        message=f"Removed converged key {key} from the replica repair queue.",
                        source="replica-repair-queue",
                        destination="Coordinator",
                        attributes={"key": key, "version": versions[0][0]},
                    )

    _emit_event(
        "recovery.completed",
        operation="recover_simulation",
        status="degraded" if errors else "success",
        message=(
            "Recovery completed with errors."
            if errors
            else "Links healed and available replicas synchronized."
        ),
        attributes={
            "sync_results": sync_results,
            "converged_keys": converged,
            "pending_replications": sorted(replication_pending),
            "errors": errors,
        },
    )
    return {
        "status": "DEGRADED" if errors else "RECOVERED",
        "workers": workers,
        "sync_results": sync_results,
        "converged_keys": converged,
        "pending_replications": sorted(replication_pending),
        "errors": errors,
    }


@app.post("/api/v1/simulation/reset")
async def reset_simulation():
    current_task = asyncio.current_task()
    pending = [
        task
        for task in set(active_request_tasks.values())
        if task is not None and task is not current_task and not task.done()
    ]
    for task in pending:
        task.cancel()
    if pending:
        await asyncio.gather(*pending, return_exceptions=True)

    generation = event_store.reset()
    failed_nodes.clear()
    network_partitions.clear()
    replication_pending.clear()
    telemetry_versions.clear()
    simulation_clock.reset()
    results = await _push_control_to_workers(
        "/simulation/reset",
        {"generation": generation},
        record_events=False,
    )
    errors = [row for row in results if row["status"] != "OK"]
    if errors:
        raise HTTPException(
            status_code=503,
            detail={"message": "Simulation reset could not reach every worker.", "workers": errors},
        )
    return {
        "status": "RESET",
        "generation": generation,
        "cancelled_requests": len(pending),
        "cleared": [
            "pending requests",
            "events and metrics",
            "faults",
            "replicated telemetry",
            "simulation timing",
        ],
    }


@app.post("/api/v1/residents")
async def create_resident(body: ResidentRecord):
    return await _proxy_json("POST", f"{TX_URL}/residents", "PRIVATE_VPC", body.model_dump())


@app.get("/api/v1/residents")
async def list_residents():
    return await _proxy_json("GET", f"{TX_URL}/residents", "PRIVATE_VPC")


@app.get("/api/v1/residents/{resident_id}")
async def get_resident(resident_id: str):
    return await _proxy_json("GET", f"{TX_URL}/residents/{resident_id}", "PRIVATE_VPC")


@app.get("/api/v1/alerts")
async def list_alerts():
    return await _proxy_json("GET", f"{TX_URL}/alerts", "PRIVATE_VPC")


@app.get("/api/v1/sessions/{session_id}")
async def get_session(session_id: str):
    return await _proxy_json("GET", f"{TX_URL}/sessions/{session_id}", "PRIVATE_VPC")


@app.post("/api/v1/sense")
async def sense(body: SenseRequest, request: Request):
    """Run one distributed WiCare window: transmit, both receivers, then fuse."""
    started = time.perf_counter()
    hops = [{"node": "Coordinator", "action": "accepted sensing request", "resident_id": body.resident_id}]

    async with httpx.AsyncClient(timeout=15.0) as client:
        try:
            resident_resp = await _node_request(
                client,
                node_id="Worker-TX",
                method="GET",
                path=f"/residents/{body.resident_id}",
                operation="load_resident",
                network_zone="PRIVATE_VPC",
            )
        except NodeUnavailable as exc:
            raise HTTPException(status_code=503, detail=f"Private transmitter unavailable: {exc}") from exc
        if resident_resp.status_code == 404:
            raise HTTPException(status_code=404, detail="Resident not found on the private transmitter node.")
        if resident_resp.status_code != 200:
            raise HTTPException(status_code=503, detail="Transmitter could not load the resident record.")
        resident = resident_resp.json()
        primary_id = ZONE_PRIMARY.get(resident["zone_id"])
        if primary_id is None:
            raise HTTPException(status_code=400, detail="Resident zone is not covered by a receiver.")

        _emit_event(
            "routing.decision",
            operation="resident_zone_routing",
            status="routed",
            message=f"Routed {resident['zone_id']} sensing to primary {primary_id}.",
            source="Coordinator",
            destination=primary_id,
            attributes={"resident_id": resident["resident_id"], "zone_id": resident["zone_id"]},
        )
        hops.append(
            {
                "node": "Worker-TX",
                "action": "loaded resident identity from the private store",
                "zone_id": resident["zone_id"],
                "primary_receiver": primary_id,
            }
        )

        tx_started = time.perf_counter()
        try:
            tx_resp = await _node_request(
                client,
                node_id="Worker-TX",
                method="POST",
                path="/transmit",
                operation="transmit_csi_window",
                network_zone="PRIVATE_VPC",
                payload={
                    "resident_id": resident["resident_id"],
                    "zone_id": resident["zone_id"],
                    "scenario": body.scenario,
                    "duration_s": body.duration_s,
                    "session_id": uuid.uuid4().hex[:12],
                },
                timeout_s=15.0,
            )
        except NodeUnavailable as exc:
            raise HTTPException(status_code=503, detail=f"Transmitter failed: {exc}") from exc
        if tx_resp.status_code != 200:
            raise HTTPException(status_code=503, detail=f"Transmitter failed: {tx_resp.text}")
        session = tx_resp.json()
        transmit_ms = round((time.perf_counter() - tx_started) * 1000, 1)
        hops.append(
            {
                "node": "Worker-TX",
                "action": "built two CSI links and kept the raw window in the private store",
                "session_id": session["session_id"],
                "elapsed_ms": transmit_ms,
            }
        )

        async def run_receiver(
            link: dict,
            *,
            fallback_from: str | None = None,
        ):
            node_id = link["receiver_id"]
            if node_id not in RX_BY_ID:
                raise HTTPException(status_code=500, detail=f"No route for {link['receiver_id']}")
            _emit_event(
                "routing.decision",
                operation="receiver_failover" if fallback_from else "receiver_fanout",
                status="degraded" if fallback_from else "routed",
                message=(
                    f"Primary {fallback_from} failed after {MAX_NODE_ATTEMPTS} attempts; "
                    f"dispatched CSI link {link['link_id']} to {node_id} as a degraded fallback."
                    if fallback_from
                    else f"Dispatched CSI link {link['link_id']} to {node_id}."
                ),
                source="Coordinator",
                destination=node_id,
                attributes={
                    "session_id": session["session_id"],
                    "link_id": link["link_id"],
                    "zone_id": resident["zone_id"],
                    **(
                        {
                            "fallback_from": fallback_from,
                            "primary_attempts": MAX_NODE_ATTEMPTS,
                        }
                        if fallback_from
                        else {}
                    ),
                },
            )
            response = await _node_request(
                client,
                node_id=node_id,
                method="POST",
                path="/process",
                operation="process_csi_link",
                timeout_s=15.0,
                payload={
                    "session_id": session["session_id"],
                    "link_id": link["link_id"],
                    "receiver_id": node_id,
                    "zone_id": resident["zone_id"],
                    "fs_hz": session["fs_hz"],
                    "amplitudes": link["amplitudes"],
                },
            )
            if response.status_code >= 400:
                raise NodeUnavailable(
                    f"{node_id} rejected CSI processing with HTTP {response.status_code}: {response.text}"
                )
            return response.json()

        parallel_started = time.perf_counter()
        fallback_route = None
        if _unavailable_reason(primary_id) is not None:
            links_by_receiver = {link["receiver_id"]: link for link in session["links"]}
            fallback_id = next(
                receiver_id for receiver_id in RX_BY_ID if receiver_id != primary_id
            )
            receiver_results = {}
            try:
                receiver_results[primary_id] = await run_receiver(
                    links_by_receiver[primary_id]
                )
            except NodeUnavailable as exc:
                receiver_results[primary_id] = exc
                fallback_route = {
                    "after_receiver": primary_id,
                    "receiver": fallback_id,
                    "after_attempts": MAX_NODE_ATTEMPTS,
                }
                try:
                    receiver_results[fallback_id] = await run_receiver(
                        links_by_receiver[fallback_id],
                        fallback_from=primary_id,
                    )
                except NodeUnavailable as fallback_error:
                    receiver_results[fallback_id] = fallback_error
            raw_results = [
                receiver_results.get(link["receiver_id"])
                for link in session["links"]
            ]
        else:
            raw_results = await asyncio.gather(
                *[run_receiver(link) for link in session["links"]],
                return_exceptions=True,
            )
        parallel_ms = round((time.perf_counter() - parallel_started) * 1000, 1)

    estimates = []
    failures = []
    for link, result in zip(session["links"], raw_results):
        if isinstance(result, Exception):
            failures.append({"receiver_id": link["receiver_id"], "error": str(result)})
            hops.append(
                {
                    "node": link["receiver_id"],
                    "action": "pipeline failed",
                    "error": str(result),
                }
            )
        else:
            estimates.append(result)
            hops.append(
                {
                    "node": result["node_id"],
                    "action": "ran Fresnel removal, subcarrier selection, and rate estimation",
                    "link_id": result["link_id"],
                    "heart_bpm": result["heart_bpm"],
                    "breath_bpm": result["breath_bpm"],
                    "elapsed_ms": result["processing_ms"],
                }
            )

    if not estimates:
        raise HTTPException(status_code=503, detail={"message": "Both receivers were unavailable.", "failures": failures})

    fusion_started = time.perf_counter()
    _emit_event(
        "processing.started",
        operation="fuse_receiver_estimates",
        status="started",
        message=f"Started fusing {len(estimates)} receiver result(s).",
        attributes={"session_id": session["session_id"], "receiver_count": len(estimates)},
    )
    fused = fuse_links(estimates, primary_id)
    urgency = classify_urgency(
        fused["heart_bpm"],
        fused["breath_bpm"],
        fused["activity"],
        fused["fall_detected"],
        fused["vitals_reliable"],
    )
    tags = abnormality_tags(
        fused["heart_bpm"],
        fused["breath_bpm"],
        fused["fall_detected"],
        fused["vitals_reliable"],
    )
    summary = alert_summary(
        urgency=urgency,
        heart_bpm=fused["heart_bpm"],
        breath_bpm=fused["breath_bpm"],
        activity=fused["activity"],
        fall_detected=fused["fall_detected"],
        motion_gated=fused["motion_gated"],
        resident_name=resident["display_name"],
        zone_id=resident["zone_id"],
        care_level=resident["care_level"],
        tags=tags,
    )
    _emit_event(
        "processing.completed",
        operation="fuse_receiver_estimates",
        status="success",
        message=f"Fusion completed and rules assigned {urgency}.",
        duration_ms=(time.perf_counter() - fusion_started) * 1000,
        attributes={
            "session_id": session["session_id"],
            "fusion": fused["fusion"],
            "urgency": urgency,
            "receivers_used": fused["receivers_used"],
        },
    )
    hops.append(
        {
            "node": "Coordinator",
            "action": "fused receiver links and assigned urgency with rules",
            "fusion": fused["fusion"],
            "urgency": urgency,
        }
    )

    truth = session["ground_truth"]
    error = None
    if fused["vitals_reliable"]:
        error = {
            "heart_bpm": round(fused["heart_bpm"] - truth["heart_bpm"], 1),
            "breath_bpm": round(fused["breath_bpm"] - truth["breath_bpm"], 1),
        }

    alert = {
        "alert_id": uuid.uuid4().hex[:12],
        "session_id": session["session_id"],
        "resident_id": resident["resident_id"],
        "resident_name": resident["display_name"],
        "zone_id": resident["zone_id"],
        "care_level": resident["care_level"],
        "urgency": urgency,
        "summary": summary,
        "tags": tags,
        "heart_bpm": fused["heart_bpm"],
        "breath_bpm": fused["breath_bpm"],
        "activity": fused["activity"],
        "fall_detected": fused["fall_detected"],
        "trace_id": trace_id_var.get(),
        "created_at": datetime.now().isoformat(timespec="seconds"),
        "urgency_authority": "rules",
        "summary_authority": "template",
    }

    archive_status = "SKIPPED"
    if urgency in {"Warning", "Emergency"}:
        try:
            async with httpx.AsyncClient(timeout=3.0) as archive_client:
                saved = await _node_request(
                    archive_client,
                    node_id="Worker-TX",
                    method="POST",
                    path="/alerts",
                    operation="archive_alert",
                    network_zone="PRIVATE_VPC",
                    payload=alert,
                    timeout_s=10.0,
                )
            archive_status = "STORED" if saved.status_code == 200 else "FAILED"
        except NodeUnavailable:
            archive_status = "FAILED"
        hops.append(
            {
                "node": "Worker-TX",
                "action": "archived the alert on the private node" if archive_status == "STORED" else "alert archive failed",
            }
        )

    receiver_sum_ms = round(sum(row["processing_ms"] for row in estimates), 1)
    logger.info(
        "Session %s fused on %s -> %s (parallel %.0f ms, sum of receivers %.0f ms).",
        session["session_id"],
        fused["fusion"],
        urgency,
        parallel_ms,
        receiver_sum_ms,
    )
    result_status = "SUCCESS" if not failures else "DEGRADED"
    request.state.simulation_outcome = result_status.lower()
    return {
        "status": result_status,
        "trace_id": trace_id_var.get(),
        "resident": resident,
        "session_id": session["session_id"],
        "scenario": body.scenario,
        "routing": {
            "primary_receiver": primary_id,
            "reason": f"{resident['zone_id']} is the Fresnel zone covered by {primary_id}.",
            "degraded_fallback": fallback_route,
        },
        "placement": {
            "identity_and_raw_csi": "Worker-TX private store",
            "parallel_pipelines": [row["node_id"] for row in estimates],
            "urgency": "Coordinator rules",
        },
        "receivers": estimates,
        "receiver_failures": failures,
        "fused": fused,
        "simulation_ground_truth": truth,
        "estimate_error": error,
        "alert": alert,
        "archive_status": archive_status,
        "timing_ms": {
            "transmit": transmit_ms,
            "receivers_parallel": parallel_ms,
            "receivers_sum": receiver_sum_ms,
            "total": round((time.perf_counter() - started) * 1000, 1),
        },
        "hops": hops,
    }


@app.post("/api/v1/write", response_model=DistributedResponse)
async def write_data(payload: DataPayload, replica: str | None = None):
    if replica is not None and replica not in {"Worker-RX-A", "Worker-RX-B"}:
        raise HTTPException(status_code=400, detail="replica must be Worker-RX-A or Worker-RX-B.")

    if payload.key.startswith("telemetry:") and not payload.is_sensitive:
        if replica is None:
            target_url, zone = _route_storage(payload.key, False)
            target_id = next(node_id for node_id, url in NODE_URLS.items() if url == target_url)
        else:
            target_id = replica
            target_url = NODE_URLS[target_id]
            zone = "EDGE_COMPUTE"
        version = telemetry_versions.get(payload.key, 0) + 1
        telemetry_versions[payload.key] = version
        update = ReplicaUpdate(
            key=payload.key,
            value=payload.value,
            version=version,
            writer_id=target_id,
        )
        _emit_event(
            "routing.decision",
            operation="telemetry_replica_selection",
            status="routed",
            message=f"Selected {target_id} as the local writer for {payload.key}.",
            source="Coordinator",
            destination=target_id,
            attributes={"key": payload.key, "version": version},
        )
        async with httpx.AsyncClient(timeout=5.0) as client:
            try:
                resp = await _node_request(
                    client,
                    node_id=target_id,
                    method="POST",
                    path="/telemetry/write",
                    operation="write_replicated_telemetry",
                    network_zone=zone,
                    payload=update.model_dump(),
                    timeout_s=15.0,
                )
            except NodeUnavailable as exc:
                raise HTTPException(status_code=503, detail=str(exc)) from exc
        if resp.status_code >= 400:
            raise HTTPException(status_code=resp.status_code, detail=_error_detail(resp))
        result = resp.json()
        if result["replication_status"] == "REPLICATED":
            if payload.key in replication_pending:
                replication_pending.discard(payload.key)
                _emit_event(
                    "queue.dequeued",
                    operation="replica_repair",
                    status="completed",
                    message=f"Removed replicated key {payload.key} from the repair queue.",
                    source="replica-repair-queue",
                    destination="Coordinator",
                    attributes={"key": payload.key, "version": version},
                )
        else:
            if payload.key not in replication_pending:
                replication_pending.add(payload.key)
                _emit_event(
                    "queue.enqueued",
                    operation="replica_repair",
                    status="pending",
                    message=f"Queued key {payload.key} for reconciliation after recovery.",
                    source="Coordinator",
                    destination="replica-repair-queue",
                    attributes={"key": payload.key, "version": version},
                )
        return DistributedResponse(
            status="SUCCESS",
            node_id=target_id,
            message=(
                f"Telemetry version {version} accepted on {target_id}; "
                f"replication status is {result['replication_status']}."
            ),
            data={
                "key": payload.key,
                "value": payload.value,
                "version": version,
                "writer_id": target_id,
                "replication_status": result["replication_status"],
                "pending_replications": len(replication_pending),
            },
            trace_id=trace_id_var.get(),
            routed_to=target_url,
            network_zone=zone,
        )

    if replica is not None:
        raise HTTPException(
            status_code=400,
            detail="Explicit replica selection is available only for non-sensitive telemetry keys.",
        )

    target, zone = _route_storage(payload.key, payload.is_sensitive)
    target_id = next(node_id for node_id, url in NODE_URLS.items() if url == target)
    _emit_event(
        "routing.decision",
        operation="key_value_routing",
        status="routed",
        message=f"Routed key {payload.key} to {target_id} in {zone}.",
        source="Coordinator",
        destination=target_id,
        attributes={"key": payload.key, "sensitive": payload.is_sensitive, "network_zone": zone},
    )
    async with httpx.AsyncClient(timeout=3.0) as client:
        try:
            resp = await _node_request(
                client,
                node_id=target_id,
                method="POST",
                path="/store",
                operation="write_key_value",
                network_zone=zone,
                payload=payload.model_dump(),
            )
        except NodeUnavailable as exc:
            raise HTTPException(status_code=503, detail=f"Target node {target_id} unavailable: {exc}") from exc
    if resp.status_code >= 400:
        raise HTTPException(status_code=resp.status_code, detail=_error_detail(resp))
    body = DistributedResponse(**resp.json())
    return body.model_copy(update={"routed_to": target, "network_zone": zone})


@app.get("/api/v1/read/{key}", response_model=DistributedResponse)
async def read_data(key: str, is_sensitive: bool = False, replica: str | None = None):
    if replica is not None and replica not in {"Worker-RX-A", "Worker-RX-B"}:
        raise HTTPException(status_code=400, detail="replica must be Worker-RX-A or Worker-RX-B.")
    if key.startswith("telemetry:") and not is_sensitive:
        if replica is None:
            target, zone = _route_storage(key, False)
            target_id = next(node_id for node_id, url in NODE_URLS.items() if url == target)
        else:
            target_id = replica
            target = NODE_URLS[target_id]
            zone = "EDGE_COMPUTE"
        async with httpx.AsyncClient(timeout=3.0) as client:
            try:
                resp = await _node_request(
                    client,
                    node_id=target_id,
                    method="GET",
                    path=f"/telemetry/read/{quote(key, safe=':')}",
                    operation="read_replicated_telemetry",
                    network_zone=zone,
                )
            except NodeUnavailable as exc:
                raise HTTPException(status_code=503, detail=str(exc)) from exc
        if resp.status_code == 404:
            raise HTTPException(status_code=404, detail="Telemetry key not found on the selected replica.")
        if resp.status_code >= 400:
            raise HTTPException(status_code=resp.status_code, detail=_error_detail(resp))
        record = resp.json()
        return DistributedResponse(
            status="SUCCESS",
            node_id=target_id,
            message=f"Read telemetry version {record['version']} from {target_id}.",
            data=record,
            trace_id=trace_id_var.get(),
            routed_to=target,
            network_zone=zone,
        )
    if replica is not None:
        raise HTTPException(
            status_code=400,
            detail="Explicit replica selection is available only for non-sensitive telemetry keys.",
        )

    target, zone = _route_storage(key, is_sensitive)
    target_id = next(node_id for node_id, url in NODE_URLS.items() if url == target)
    _emit_event(
        "routing.decision",
        operation="key_value_routing",
        status="routed",
        message=f"Routed read for key {key} to {target_id} in {zone}.",
        source="Coordinator",
        destination=target_id,
        attributes={"key": key, "sensitive": is_sensitive, "network_zone": zone},
    )
    async with httpx.AsyncClient(timeout=3.0) as client:
        try:
            resp = await _node_request(
                client,
                node_id=target_id,
                method="GET",
                path=f"/read/{quote(key, safe=':')}",
                operation="read_key_value",
                network_zone=zone,
            )
        except NodeUnavailable as exc:
            raise HTTPException(status_code=503, detail=f"Target node {target_id} unavailable: {exc}") from exc
    if resp.status_code == 404:
        raise HTTPException(status_code=404, detail="Key not found in the distributed store.")
    if resp.status_code >= 400:
        raise HTTPException(status_code=resp.status_code, detail=_error_detail(resp))
    body = DistributedResponse(**resp.json())
    return body.model_copy(update={"routed_to": target, "network_zone": zone})


@app.get("/api/v1/replicas/{key}")
async def read_replicas(key: str):
    if not key.startswith("telemetry:"):
        raise HTTPException(status_code=400, detail="Only telemetry: keys are replicated.")

    async with httpx.AsyncClient(timeout=3.0) as client:
        async def read_one(node_id: str) -> dict:
            try:
                response = await _node_request(
                    client,
                    node_id=node_id,
                    method="GET",
                    path=f"/telemetry/read/{quote(key, safe=':')}",
                    operation="inspect_replica",
                )
            except NodeUnavailable as exc:
                return {"node_id": node_id, "status": "UNAVAILABLE", "error": str(exc)}
            if response.status_code == 404:
                return {"node_id": node_id, "status": "MISSING"}
            if response.status_code >= 400:
                return {
                    "node_id": node_id,
                    "status": "ERROR",
                    "error": _error_detail(response),
                }
            return {"status": "FOUND", **response.json()}

        replicas = await asyncio.gather(read_one("Worker-RX-A"), read_one("Worker-RX-B"))

    found = [record for record in replicas if record["status"] == "FOUND"]
    unavailable = any(record["status"] in {"UNAVAILABLE", "ERROR"} for record in replicas)
    diverged = None if unavailable else (
        bool(found)
        and (
            len(found) != 2
            or len({(row["version"], row["writer_id"]) for row in found}) > 1
            or len({json.dumps(row["value"], sort_keys=True, default=str) for row in found}) > 1
        )
    )
    return {
        "key": key,
        "replicas": replicas,
        "diverged": diverged,
        "consistent": diverged is False,
    }


async def _proxy_json(method: str, url: str, zone: str, payload: dict | None = None):
    node_id = next((name for name, base in NODE_URLS.items() if url.startswith(base)), None)
    if node_id is None:
        raise HTTPException(status_code=500, detail="No node is configured for this proxy request.")
    path = url[len(NODE_URLS[node_id]) :]
    async with httpx.AsyncClient(timeout=3.0) as client:
        try:
            resp = await _node_request(
                client,
                node_id=node_id,
                method=method,
                path=path,
                operation=f"proxy_{method.lower()}",
                network_zone=zone,
                payload=payload,
            )
        except NodeUnavailable as exc:
            raise HTTPException(status_code=503, detail=f"{node_id} unavailable: {exc}") from exc
    if resp.status_code >= 400:
        raise HTTPException(status_code=resp.status_code, detail=_error_detail(resp))
    return resp.json()


def _error_detail(resp: httpx.Response):
    try:
        body = resp.json()
    except ValueError:
        return resp.text
    return body.get("detail", body)
