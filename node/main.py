import asyncio
import os
import time
import uuid
from contextlib import asynccontextmanager
from datetime import datetime

import httpx
from fastapi import FastAPI, HTTPException, Request

from common.logger import (
    generate_trace_id,
    get_logger,
    parent_span_id_var,
    request_id_var,
    span_id_var,
    trace_id_var,
)
from common.models import (
    DataPayload,
    DistributedResponse,
    LinkEstimate,
    ProcessRequest,
    ReplicaUpdate,
    ResidentRecord,
    SimulationControl,
    SystemHealth,
    TransmitRequest,
    TransmitResponse,
)
from common.pipeline import process_link, synthesize_session
from common.simulation import SimulationClock, create_event
from fastapi.responses import JSONResponse

NODE_ID = os.getenv("NODE_ID", "Worker-Default")
ROLE = os.getenv("ROLE", "receiver")
ZONE = os.getenv("ZONE", "zone-a")
IS_PRIVATE_NODE = os.getenv("IS_PRIVATE", "false").lower() == "true"
REPLICA_ID = os.getenv("REPLICA_ID", "")
REPLICA_URL = os.getenv("REPLICA_URL", "").rstrip("/")
EVENT_SINK_URL = os.getenv(
    "EVENT_SINK_URL",
    "http://127.0.0.1:8000/api/v1/events/ingest",
)
event_sink_client: httpx.AsyncClient | None = None
replica_http_client: httpx.AsyncClient | None = None


@asynccontextmanager
async def lifespan(_app: FastAPI):
    global event_sink_client, replica_http_client

    event_sink_client = httpx.AsyncClient(timeout=1.0)
    replica_http_client = httpx.AsyncClient(timeout=8.0)
    try:
        yield
    finally:
        await event_sink_client.aclose()
        await replica_http_client.aclose()
        event_sink_client = None
        replica_http_client = None

app = FastAPI(
    title=f"WiCare {ROLE.title()} {NODE_ID}",
    description=(
        "Transmitter nodes keep raw CSI and resident identity on the private side. "
        "Receiver nodes only run the vital-sign pipeline for the link they are given."
    ),
    lifespan=lifespan,
)
logger = get_logger(NODE_ID)

local_store: dict[str, object] = {}
residents: dict[str, dict] = {}
sessions: dict[str, dict] = {}
alerts: list[dict] = []
telemetry_store: dict[str, dict] = {}
telemetry_pending: set[str] = set()
blocked_peers: set[str] = set()
simulation_clock = SimulationClock()
simulation_generation = 0

if ROLE == "transmitter":
    residents.update(
        {
            "P001": {
                "resident_id": "P001",
                "display_name": "Ana Reyes",
                "zone_id": "zone-a",
                "care_level": "standard",
                "notes": "Demo resident for bed 1. Covered by the transmitter to receiver A link.",
            },
            "P002": {
                "resident_id": "P002",
                "display_name": "Jose Cruz",
                "zone_id": "zone-b",
                "care_level": "high",
                "notes": "Demo resident for bed 2. Covered by the transmitter to receiver B link.",
            },
        }
    )


def _require_zone(request: Request, allowed: set[str], detail: str) -> None:
    zone = request.headers.get("X-Network-Zone")
    if zone not in allowed:
        logger.warning("Rejected %s %s from network zone %s", request.method, request.url.path, zone)
        raise HTTPException(status_code=403, detail=detail)


def _require_current_generation(request: Request) -> None:
    if request.state.generation < simulation_generation:
        raise HTTPException(
            status_code=409,
            detail="Request belongs to a reset simulation generation.",
        )


async def _emit_node_event(
    request: Request,
    event_type: str,
    *,
    operation: str,
    status: str,
    message: str,
    source: str | None = None,
    destination: str | None = None,
    duration_ms: float | None = None,
    attributes: dict | None = None,
    span_id: str | None = None,
    parent_span_id: str | None = None,
) -> None:
    event = create_event(
        event_type,
        trace_id=request.state.trace_id,
        request_id=request.state.request_id,
        span_id=span_id or request.state.node_span_id,
        parent_span_id=(
            request.state.parent_span_id if parent_span_id is None else parent_span_id
        ),
        generation=request.state.generation,
        service=NODE_ID,
        node=NODE_ID,
        operation=operation,
        status=status,
        message=message,
        source=source,
        destination=destination,
        duration_ms=duration_ms,
        attributes=attributes,
    )
    if not EVENT_SINK_URL:
        logger.warning("Event sink is not configured for %s.", event_type)
        return

    if event_sink_client is None:
        logger.warning("Event sink client is unavailable for %s.", event_type)
        return
    try:
        response = await event_sink_client.post(EVENT_SINK_URL, json=event)
    except httpx.RequestError as exc:
        logger.warning("Could not publish %s to the event sink: %s", event_type, exc)
        return
    if response.is_error:
        logger.warning(
            "Event sink rejected %s with HTTP %s.",
            event_type,
            response.status_code,
        )


@app.middleware("http")
async def trace_middleware(request: Request, call_next):
    trace_id = request.headers.get("X-Trace-ID") or generate_trace_id()
    request_id = request.headers.get("X-Request-ID") or uuid.uuid4().hex[:16]
    parent_span_id = request.headers.get("X-Span-ID")
    node_span_id = uuid.uuid4().hex[:16]
    try:
        raw_generation = request.headers.get("X-Simulation-Generation")
        generation = int(raw_generation) if raw_generation is not None else simulation_generation
    except ValueError:
        return JSONResponse(
            status_code=400,
            content={"detail": "X-Simulation-Generation must be an integer."},
        )
    if generation < simulation_generation and request.url.path not in {
        "/health",
        "/simulation/control",
        "/simulation/reset",
    }:
        return JSONResponse(
            status_code=409,
            content={"detail": "Request belongs to a reset simulation generation."},
        )

    request.state.trace_id = trace_id
    request.state.request_id = request_id
    request.state.parent_span_id = parent_span_id
    request.state.node_span_id = node_span_id
    request.state.generation = generation

    trace_token = trace_id_var.set(trace_id)
    request_token = request_id_var.set(request_id)
    span_token = span_id_var.set(node_span_id)
    parent_token = parent_span_id_var.set(parent_span_id or "NO_PARENT_SPAN_ID")
    if request.url.path not in {"/health", "/simulation/control", "/simulation/reset"}:
        logger.info("Processing request: %s %s", request.method, request.url.path)
    try:
        response = await call_next(request)
        response.headers["X-Trace-ID"] = trace_id
        return response
    finally:
        parent_span_id_var.reset(parent_token)
        span_id_var.reset(span_token)
        request_id_var.reset(request_token)
        trace_id_var.reset(trace_token)


@app.get("/")
async def root():
    return {
        "node_id": NODE_ID,
        "role": ROLE,
        "zone_id": ZONE,
        "is_private": IS_PRIVATE_NODE,
        "health": "/health",
    }


@app.get("/health", response_model=SystemHealth)
async def health_check():
    return SystemHealth(
        status="HEALTHY",
        node_id=NODE_ID,
        role=ROLE,
        zone_id=ZONE,
        is_private=IS_PRIVATE_NODE,
        stored_keys_count=len(local_store) + len(telemetry_store),
        residents_count=len(residents),
    )


@app.post("/simulation/control")
async def configure_simulation(body: SimulationControl):
    global blocked_peers, simulation_generation

    if body.generation is not None and body.generation < simulation_generation:
        return {
            "node_id": NODE_ID,
            "status": "STALE_GENERATION",
            "generation": simulation_generation,
        }
    if body.generation is not None:
        simulation_generation = body.generation
    simulation_clock.configure(
        paused=body.paused,
        speed=body.speed,
        network_latency_ms=body.network_latency_ms,
        processing_latency_ms=body.processing_latency_ms,
    )
    if body.partitions is not None:
        blocked_peers = {
            peer
            for left, right in body.partitions
            if NODE_ID in {left, right}
            for peer in {left, right}
            if peer != NODE_ID
        }
    return {
        "node_id": NODE_ID,
        "blocked_peers": sorted(blocked_peers),
        "simulation": simulation_clock.snapshot(),
        "generation": simulation_generation,
    }


@app.post("/simulation/reset")
async def reset_simulation(body: SimulationControl):
    global simulation_generation

    if body.generation is not None and body.generation < simulation_generation:
        return {
            "node_id": NODE_ID,
            "status": "STALE_GENERATION",
            "generation": simulation_generation,
        }
    simulation_clock.reset()
    blocked_peers.clear()
    telemetry_store.clear()
    telemetry_pending.clear()
    if body.generation is not None:
        simulation_generation = body.generation
    return {
        "node_id": NODE_ID,
        "status": "RESET",
        "generation": simulation_generation,
    }


@app.post("/store", response_model=DistributedResponse)
async def store_data(payload: DataPayload, request: Request):
    if ROLE == "transmitter":
        _require_zone(
            request,
            {"PRIVATE_VPC"},
            "The transmitter store is only reachable through the private facility zone.",
        )
    else:
        if payload.key.startswith("telemetry:"):
            raise HTTPException(
                status_code=400,
                detail="Replicated telemetry must use the telemetry write route.",
            )
        if payload.is_sensitive:
            raise HTTPException(
                status_code=403,
                detail="Sensitive clinical records stay on the private transmitter node.",
            )
        _require_zone(
            request,
            {"EDGE_COMPUTE"},
            "Receiver telemetry expects the edge-compute zone header.",
        )

    await simulation_clock.sleep(simulation_clock.processing_latency_ms / 1000)
    _require_current_generation(request)
    local_store[payload.key] = payload.value
    logger.info("Stored key '%s' (sensitive=%s).", payload.key, payload.is_sensitive)
    await _emit_node_event(
        request,
        "database.write",
        operation="key_value_store",
        status="success",
        message=f"Stored key {payload.key} on {NODE_ID}.",
        source="Coordinator",
        attributes={"key": payload.key, "sensitive": payload.is_sensitive},
    )
    return DistributedResponse(
        status="SUCCESS",
        node_id=NODE_ID,
        message=f"Key '{payload.key}' stored on {NODE_ID}",
        data={"key": payload.key, "value": payload.value, "is_sensitive": payload.is_sensitive},
        trace_id=trace_id_var.get(),
    )


@app.get("/read/{key}", response_model=DistributedResponse)
async def read_data(key: str, request: Request):
    if ROLE == "transmitter":
        _require_zone(
            request,
            {"PRIVATE_VPC"},
            "The transmitter store is only reachable through the private facility zone.",
        )
    else:
        _require_zone(
            request,
            {"EDGE_COMPUTE"},
            "Receiver telemetry expects the edge-compute zone header.",
        )

    if key not in local_store:
        await _emit_node_event(
            request,
            "database.read",
            operation="key_value_store",
            status="not_found",
            message=f"Key {key} was not present on {NODE_ID}.",
            source="Coordinator",
            attributes={"key": key},
        )
        raise HTTPException(status_code=404, detail="Key not found on this node")

    await _emit_node_event(
        request,
        "database.read",
        operation="key_value_store",
        status="success",
        message=f"Read key {key} from {NODE_ID}.",
        source="Coordinator",
        attributes={"key": key},
    )
    return DistributedResponse(
        status="SUCCESS",
        node_id=NODE_ID,
        message="Data retrieved",
        data={"key": key, "value": local_store[key]},
        trace_id=trace_id_var.get(),
    )


def _apply_telemetry_update(update: ReplicaUpdate) -> tuple[bool, dict]:
    current = telemetry_store.get(update.key)
    incoming_version = (update.version, update.writer_id)
    current_version = (
        (current["version"], current["writer_id"])
        if current is not None
        else None
    )
    if current_version is not None and incoming_version <= current_version:
        return False, current

    record = {
        "key": update.key,
        "value": update.value,
        "version": update.version,
        "writer_id": update.writer_id,
    }
    telemetry_store[update.key] = record
    return True, record


def _peer_headers(request: Request, span_id: str) -> dict[str, str]:
    return {
        "X-Trace-ID": request.state.trace_id,
        "X-Request-ID": request.state.request_id,
        "X-Span-ID": span_id,
        "X-Simulation-Generation": str(request.state.generation),
        "X-Network-Zone": "EDGE_COMPUTE",
    }


async def _replicate_to_peer(
    update: ReplicaUpdate,
    request: Request,
    *,
    operation: str = "telemetry_replication",
) -> str:
    if not REPLICA_ID or not REPLICA_URL:
        telemetry_pending.add(update.key)
        await _emit_node_event(
            request,
            "replication.pending",
            operation=operation,
            status="unconfigured",
            message=f"No replica is configured for {NODE_ID}.",
            source=NODE_ID,
            destination=REPLICA_ID or None,
            attributes={"key": update.key, "version": update.version},
        )
        return "PENDING"

    if REPLICA_ID in blocked_peers:
        telemetry_pending.add(update.key)
        await _emit_node_event(
            request,
            "replication.blocked",
            operation=operation,
            status="partitioned",
            message=f"Partition blocked replication from {NODE_ID} to {REPLICA_ID}.",
            source=NODE_ID,
            destination=REPLICA_ID,
            attributes={"key": update.key, "version": update.version},
        )
        return "PARTITIONED"

    child_span_id = uuid.uuid4().hex[:16]
    started = time.perf_counter()
    await _emit_node_event(
        request,
        "message.sent",
        operation=operation,
        status="sent",
        message=f"Sent telemetry version {update.version} to {REPLICA_ID}.",
        source=NODE_ID,
        destination=REPLICA_ID,
        attributes={"key": update.key, "version": update.version},
        span_id=child_span_id,
        parent_span_id=request.state.node_span_id,
    )
    await simulation_clock.sleep(simulation_clock.network_latency_ms / 1000)
    if replica_http_client is None:
        telemetry_pending.add(update.key)
        await _emit_node_event(
            request,
            "replication.pending",
            operation=operation,
            status="unavailable",
            message=f"Replica HTTP client is unavailable on {NODE_ID}.",
            source=NODE_ID,
            destination=REPLICA_ID,
            attributes={"key": update.key, "version": update.version},
            span_id=child_span_id,
            parent_span_id=request.state.node_span_id,
        )
        return "PENDING"
    try:
        response = await replica_http_client.post(
            f"{REPLICA_URL}/telemetry/replicate",
            json=update.model_dump(),
            headers=_peer_headers(request, child_span_id),
        )
    except httpx.RequestError as exc:
        telemetry_pending.add(update.key)
        event_type = "timeout" if isinstance(exc, httpx.TimeoutException) else "message.failed"
        await _emit_node_event(
            request,
            event_type,
            operation=operation,
            status="failure",
            message=f"Replication to {REPLICA_ID} failed: {exc}.",
            source=NODE_ID,
            destination=REPLICA_ID,
            duration_ms=(time.perf_counter() - started) * 1000,
            attributes={"key": update.key, "version": update.version},
            span_id=child_span_id,
            parent_span_id=request.state.node_span_id,
        )
        logger.warning("Replication to %s failed: %s", REPLICA_ID, exc)
        return "PENDING"

    await simulation_clock.sleep(simulation_clock.network_latency_ms / 1000)
    if response.is_error:
        telemetry_pending.add(update.key)
        await _emit_node_event(
            request,
            "message.failed",
            operation=operation,
            status="failure",
            message=f"Replica {REPLICA_ID} returned HTTP {response.status_code}.",
            source=NODE_ID,
            destination=REPLICA_ID,
            duration_ms=(time.perf_counter() - started) * 1000,
            attributes={"key": update.key, "version": update.version},
            span_id=child_span_id,
            parent_span_id=request.state.node_span_id,
        )
        logger.warning(
            "Replication to %s returned HTTP %s.",
            REPLICA_ID,
            response.status_code,
        )
        return "PENDING"

    remote = response.json()
    remote_version = (remote["version"], remote["writer_id"])
    sent_version = (update.version, update.writer_id)
    if remote_version > sent_version:
        telemetry_pending.add(update.key)
        status = "STALE"
    else:
        telemetry_pending.discard(update.key)
        status = "REPLICATED"
    await _emit_node_event(
        request,
        "message.received",
        operation=operation,
        status="success" if status == "REPLICATED" else "stale",
        message=f"Replica {REPLICA_ID} acknowledged version {remote['version']}.",
        source=REPLICA_ID,
        destination=NODE_ID,
        duration_ms=(time.perf_counter() - started) * 1000,
        attributes={
            "key": update.key,
            "sent_version": update.version,
            "remote_version": remote["version"],
            "replication_status": status,
        },
        span_id=child_span_id,
        parent_span_id=request.state.node_span_id,
    )
    return status


if ROLE == "receiver":

    @app.post("/telemetry/write")
    async def write_telemetry(update: ReplicaUpdate, request: Request):
        _require_zone(
            request,
            {"EDGE_COMPUTE"},
            "Replicated telemetry is stored on the edge-compute nodes.",
        )
        if update.writer_id != NODE_ID:
            raise HTTPException(
                status_code=400,
                detail="The writer_id must identify the selected replica.",
            )

        started = time.perf_counter()
        await simulation_clock.sleep(simulation_clock.processing_latency_ms / 1000)
        _require_current_generation(request)
        applied, local_record = _apply_telemetry_update(update)
        telemetry_pending.add(update.key)
        outbound = ReplicaUpdate(**local_record)
        replication_status = await _replicate_to_peer(outbound, request)
        await _emit_node_event(
            request,
            "database.write",
            operation="replicated_telemetry",
            status="success" if applied else "stale_write_ignored",
            message=f"Accepted telemetry version {local_record['version']} on {NODE_ID}.",
            source="Coordinator",
            duration_ms=(time.perf_counter() - started) * 1000,
            attributes={
                "key": update.key,
                "version": local_record["version"],
                "writer_id": local_record["writer_id"],
                "replication_status": replication_status,
            },
        )
        return {
            "status": "SUCCESS",
            "node_id": NODE_ID,
            "applied": applied,
            "record": local_record,
            "replication_status": replication_status,
            "pending_replications": len(telemetry_pending),
        }

    @app.post("/telemetry/replicate")
    async def receive_replica(update: ReplicaUpdate, request: Request):
        _require_zone(
            request,
            {"EDGE_COMPUTE"},
            "Replica updates are accepted only from the edge-compute network.",
        )
        applied, current = _apply_telemetry_update(update)
        if applied:
            telemetry_pending.discard(update.key)
            await _emit_node_event(
                request,
                "replication.received",
                operation="telemetry_replication",
                status="applied",
                message=f"Applied telemetry version {update.version} on {NODE_ID}.",
                source=REPLICA_ID or "Worker-RX",
                destination=NODE_ID,
                attributes={"key": update.key, "version": update.version},
            )
        else:
            await _emit_node_event(
                request,
                "replication.received",
                operation="telemetry_replication",
                status="older_or_duplicate",
                message=f"Kept version {current['version']} on {NODE_ID}.",
                source=REPLICA_ID or "Worker-RX",
                destination=NODE_ID,
                attributes={
                    "key": update.key,
                    "incoming_version": update.version,
                    "current_version": current["version"],
                },
            )
        return {
            "node_id": NODE_ID,
            "applied": applied,
            "version": current["version"],
            "writer_id": current["writer_id"],
        }

    @app.get("/telemetry/read/{key}")
    async def read_telemetry(key: str, request: Request):
        _require_zone(
            request,
            {"EDGE_COMPUTE"},
            "Telemetry replicas are readable only from the edge-compute network.",
        )
        record = telemetry_store.get(key)
        if record is None:
            await _emit_node_event(
                request,
                "database.read",
                operation="replicated_telemetry",
                status="not_found",
                message=f"No telemetry copy of {key} exists on {NODE_ID}.",
                source="Coordinator",
                attributes={"key": key},
            )
            raise HTTPException(status_code=404, detail="Telemetry key not found on this replica.")

        await _emit_node_event(
            request,
            "database.read",
            operation="replicated_telemetry",
            status="success",
            message=f"Read telemetry version {record['version']} from {NODE_ID}.",
            source="Coordinator",
            attributes={"key": key, "version": record["version"]},
        )
        return {"node_id": NODE_ID, **record}

    @app.post("/telemetry/sync")
    async def synchronize_telemetry(request: Request):
        _require_zone(
            request,
            {"EDGE_COMPUTE"},
            "Replica synchronization is available only in the edge-compute network.",
        )
        synced = []
        for key in sorted(telemetry_store):
            _require_current_generation(request)
            record = telemetry_store[key]
            update = ReplicaUpdate(**record)
            status = await _replicate_to_peer(
                update,
                request,
                operation="anti_entropy_sync",
            )
            synced.append(
                {
                    "key": key,
                    "version": record["version"],
                    "status": status,
                }
            )
        return {
            "node_id": NODE_ID,
            "synced": synced,
            "pending_replications": len(telemetry_pending),
        }


if ROLE == "transmitter":

    @app.post("/transmit", response_model=TransmitResponse)
    async def transmit(body: TransmitRequest, request: Request):
        _require_zone(
            request,
            {"PRIVATE_VPC"},
            "Raw CSI is generated only for callers inside the private facility zone.",
        )
        started = time.perf_counter()
        await _emit_node_event(
            request,
            "processing.started",
            operation="synthesize_csi_window",
            status="started",
            message=f"Started CSI synthesis for {body.zone_id}.",
            source="Coordinator",
            attributes={"session_id": body.session_id, "scenario": body.scenario},
        )
        session_id = body.session_id or uuid.uuid4().hex[:12]
        try:
            await simulation_clock.sleep(simulation_clock.processing_latency_ms / 1000)
            _require_current_generation(request)
            session = synthesize_session(
                scenario=body.scenario,
                zone_id=body.zone_id,
                session_id=session_id,
                duration_s=body.duration_s,
            )
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

        session["resident_id"] = body.resident_id
        session["created_at"] = datetime.now().isoformat(timespec="seconds")
        sessions[session_id] = session
        while len(sessions) > 20:
            sessions.pop(next(iter(sessions)))

        logger.info(
            "Transmitted session %s for %s in %s (%s samples x %s subcarriers, 2 links).",
            session_id,
            body.resident_id,
            body.zone_id,
            len(session["links"][0]["amplitudes"]),
            session["n_subcarriers"],
        )
        transmit_ms = round((time.perf_counter() - started) * 1000, 1)
        await _emit_node_event(
            request,
            "processing.completed",
            operation="synthesize_csi_window",
            status="success",
            message=f"Generated two CSI links for session {session_id}.",
            source=NODE_ID,
            destination="Coordinator",
            duration_ms=transmit_ms,
            attributes={
                "session_id": session_id,
                "scenario": body.scenario,
                "zone_id": body.zone_id,
                "n_samples": len(session["links"][0]["amplitudes"]),
                "n_links": len(session["links"]),
            },
        )
        return TransmitResponse(
            session_id=session_id,
            node_id=NODE_ID,
            scenario=session["scenario"],
            zone_id=session["zone_id"],
            fs_hz=session["fs_hz"],
            n_samples=len(session["links"][0]["amplitudes"]),
            n_subcarriers=session["n_subcarriers"],
            ground_truth=session["ground_truth"],
            links=session["links"],
            transmit_ms=transmit_ms,
            trace_id=trace_id_var.get(),
        )

    @app.post("/residents")
    async def upsert_resident(body: ResidentRecord, request: Request):
        _require_zone(
            request,
            {"PRIVATE_VPC"},
            "Resident identity is stored only on the private transmitter node.",
        )
        residents[body.resident_id] = body.model_dump()
        logger.info("Upserted resident %s into %s.", body.resident_id, body.zone_id)
        await _emit_node_event(
            request,
            "database.write",
            operation="resident_directory",
            status="success",
            message=f"Stored resident {body.resident_id} on the private transmitter.",
            source="Coordinator",
            attributes={"resident_id": body.resident_id, "zone_id": body.zone_id},
        )
        return residents[body.resident_id]

    @app.get("/residents")
    async def list_residents(request: Request):
        _require_zone(
            request,
            {"PRIVATE_VPC"},
            "Resident identity is stored only on the private transmitter node.",
        )
        await _emit_node_event(
            request,
            "database.read",
            operation="resident_directory",
            status="success",
            message=f"Read {len(residents)} residents from the private transmitter.",
            source="Coordinator",
            attributes={"count": len(residents)},
        )
        return {"residents": list(residents.values())}

    @app.get("/residents/{resident_id}")
    async def get_resident(resident_id: str, request: Request):
        _require_zone(
            request,
            {"PRIVATE_VPC"},
            "Resident identity is stored only on the private transmitter node.",
        )
        record = residents.get(resident_id)
        if record is None:
            await _emit_node_event(
                request,
                "database.read",
                operation="resident_directory",
                status="not_found",
                message=f"Resident {resident_id} was not found on the private transmitter.",
                source="Coordinator",
                attributes={"resident_id": resident_id},
            )
            raise HTTPException(status_code=404, detail="Resident not found")
        await _emit_node_event(
            request,
            "database.read",
            operation="resident_directory",
            status="success",
            message=f"Loaded resident {resident_id} from the private transmitter.",
            source="Coordinator",
            attributes={"resident_id": resident_id, "zone_id": record["zone_id"]},
        )
        return record

    @app.post("/alerts")
    async def store_alert(payload: dict, request: Request):
        _require_zone(
            request,
            {"PRIVATE_VPC"},
            "Alert history is stored only on the private transmitter node.",
        )
        alerts.append(payload)
        del alerts[:-50]
        logger.info("Archived %s alert %s.", payload.get("urgency"), payload.get("alert_id"))
        await _emit_node_event(
            request,
            "database.write",
            operation="alert_archive",
            status="success",
            message=f"Archived {payload.get('urgency')} alert on the private transmitter.",
            source="Coordinator",
            attributes={
                "alert_id": payload.get("alert_id"),
                "urgency": payload.get("urgency"),
            },
        )
        return {"status": "STORED", "node_id": NODE_ID, "alert_id": payload.get("alert_id")}

    @app.get("/alerts")
    async def list_alerts(request: Request):
        _require_zone(
            request,
            {"PRIVATE_VPC"},
            "Alert history is stored only on the private transmitter node.",
        )
        await _emit_node_event(
            request,
            "database.read",
            operation="alert_archive",
            status="success",
            message=f"Read {len(alerts)} archived alerts from the private transmitter.",
            source="Coordinator",
            attributes={"count": len(alerts)},
        )
        return {"alerts": list(reversed(alerts)), "node_id": NODE_ID}

    @app.get("/sessions/{session_id}")
    async def get_session(session_id: str, request: Request):
        _require_zone(
            request,
            {"PRIVATE_VPC"},
            "Raw CSI sessions are stored only on the private transmitter node.",
        )
        session = sessions.get(session_id)
        if session is None:
            await _emit_node_event(
                request,
                "database.read",
                operation="raw_csi_session",
                status="not_found",
                message=f"Session {session_id} was not found on the private transmitter.",
                source="Coordinator",
                attributes={"session_id": session_id},
            )
            raise HTTPException(status_code=404, detail="Session not found")
        await _emit_node_event(
            request,
            "database.read",
            operation="raw_csi_session",
            status="success",
            message=f"Read metadata for session {session_id}; raw CSI remains private.",
            source="Coordinator",
            attributes={"session_id": session_id},
        )
        first_link = session["links"][0]["amplitudes"]
        preview = [round(row[0], 4) for row in first_link[:12]]
        return {
            "session_id": session["session_id"],
            "resident_id": session.get("resident_id"),
            "scenario": session["scenario"],
            "zone_id": session["zone_id"],
            "fs_hz": session["fs_hz"],
            "n_samples": len(first_link),
            "n_subcarriers": session["n_subcarriers"],
            "n_links": len(session["links"]),
            "ground_truth": session["ground_truth"],
            "created_at": session.get("created_at"),
            "csi_preview_subcarrier_0": preview,
            "stored_on": NODE_ID,
            "note": "Full CSI matrices stay on this private node. The preview is the first 12 samples of subcarrier 0.",
        }


if ROLE == "receiver":

    @app.post("/process", response_model=LinkEstimate)
    async def process_csi(body: ProcessRequest, request: Request):
        _require_zone(
            request,
            {"EDGE_COMPUTE"},
            "Receivers accept CSI windows only from the edge-compute zone.",
        )
        n_samples = len(body.amplitudes)
        n_sub = len(body.amplitudes[0]) if body.amplitudes else 0
        if n_samples < 64 or n_samples > 800 or n_sub < 1 or n_sub > 64:
            raise HTTPException(status_code=400, detail="CSI window shape is outside the simulator limits.")

        started = time.perf_counter()
        await _emit_node_event(
            request,
            "processing.started",
            operation="process_csi_link",
            status="started",
            message=f"Started CSI processing for link {body.link_id}.",
            source="Coordinator",
            attributes={
                "session_id": body.session_id,
                "link_id": body.link_id,
                "zone_id": body.zone_id,
            },
        )
        try:
            estimate = process_link(body.amplitudes, body.fs_hz)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

        await simulation_clock.sleep(simulation_clock.processing_latency_ms / 1000)
        _require_current_generation(request)
        logger.info(
            "Link %s HR %.1f RR %.1f activity %s fall %s.",
            body.link_id,
            estimate["heart_bpm"],
            estimate["breath_bpm"],
            estimate["activity"],
            estimate["fall_detected"],
        )
        processing_ms = round((time.perf_counter() - started) * 1000, 1)
        await _emit_node_event(
            request,
            "processing.completed",
            operation="process_csi_link",
            status="success",
            message=f"Completed CSI processing for link {body.link_id}.",
            source=NODE_ID,
            destination="Coordinator",
            duration_ms=processing_ms,
            attributes={
                "session_id": body.session_id,
                "link_id": body.link_id,
                "heart_bpm": estimate["heart_bpm"],
                "breath_bpm": estimate["breath_bpm"],
                "fall_detected": estimate["fall_detected"],
            },
        )
        return LinkEstimate(
            node_id=NODE_ID,
            role=ROLE,
            zone_id=ZONE,
            link_id=body.link_id,
            session_id=body.session_id,
            processing_ms=processing_ms,
            trace_id=trace_id_var.get(),
            **estimate,
        )
