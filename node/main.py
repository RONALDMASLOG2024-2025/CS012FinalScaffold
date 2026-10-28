import asyncio
import os
import time
import uuid
from datetime import datetime

from fastapi import FastAPI, HTTPException, Request

from common.logger import get_logger, trace_id_var
from common.models import (
    DataPayload,
    DistributedResponse,
    LinkEstimate,
    ProcessRequest,
    ResidentRecord,
    SystemHealth,
    TransmitRequest,
    TransmitResponse,
)
from common.pipeline import process_link, synthesize_session

NODE_ID = os.getenv("NODE_ID", "Worker-Default")
ROLE = os.getenv("ROLE", "receiver")
ZONE = os.getenv("ZONE", "zone-a")
IS_PRIVATE_NODE = os.getenv("IS_PRIVATE", "false").lower() == "true"

app = FastAPI(
    title=f"WiCare {ROLE.title()} {NODE_ID}",
    description=(
        "Transmitter nodes keep raw CSI and resident identity on the private side. "
        "Receiver nodes only run the vital-sign pipeline for the link they are given."
    ),
)
logger = get_logger(NODE_ID)

local_store: dict[str, object] = {}
residents: dict[str, dict] = {}
sessions: dict[str, dict] = {}
alerts: list[dict] = []

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


@app.middleware("http")
async def trace_middleware(request: Request, call_next):
    trace_id = request.headers.get("X-Trace-ID", "NO_TRACE_ID")
    trace_id_var.set(trace_id)
    logger.info("Processing request: %s %s", request.method, request.url.path)
    response = await call_next(request)
    response.headers["X-Trace-ID"] = trace_id
    return response


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
        stored_keys_count=len(local_store),
        residents_count=len(residents),
    )


@app.post("/store", response_model=DistributedResponse)
async def store_data(payload: DataPayload, request: Request):
    if ROLE == "transmitter":
        _require_zone(
            request,
            {"PRIVATE_VPC"},
            "The transmitter store is only reachable through the private facility zone.",
        )
    else:
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

    await asyncio.sleep(0.02)
    local_store[payload.key] = payload.value
    logger.info("Stored key '%s' (sensitive=%s).", payload.key, payload.is_sensitive)
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
        raise HTTPException(status_code=404, detail="Key not found on this node")

    return DistributedResponse(
        status="SUCCESS",
        node_id=NODE_ID,
        message="Data retrieved",
        data={"key": key, "value": local_store[key]},
        trace_id=trace_id_var.get(),
    )


if ROLE == "transmitter":

    @app.post("/transmit", response_model=TransmitResponse)
    async def transmit(body: TransmitRequest, request: Request):
        _require_zone(
            request,
            {"PRIVATE_VPC"},
            "Raw CSI is generated only for callers inside the private facility zone.",
        )
        started = time.perf_counter()
        session_id = body.session_id or uuid.uuid4().hex[:12]
        try:
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

        # Stand-in for the ESP32 serial dump arriving at the edge host.
        await asyncio.sleep(0.03)
        logger.info(
            "Transmitted session %s for %s in %s (%s samples x %s subcarriers, 2 links).",
            session_id,
            body.resident_id,
            body.zone_id,
            len(session["links"][0]["amplitudes"]),
            session["n_subcarriers"],
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
            transmit_ms=round((time.perf_counter() - started) * 1000, 1),
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
        return residents[body.resident_id]

    @app.get("/residents")
    async def list_residents(request: Request):
        _require_zone(
            request,
            {"PRIVATE_VPC"},
            "Resident identity is stored only on the private transmitter node.",
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
            raise HTTPException(status_code=404, detail="Resident not found")
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
        return {"status": "STORED", "node_id": NODE_ID, "alert_id": payload.get("alert_id")}

    @app.get("/alerts")
    async def list_alerts(request: Request):
        _require_zone(
            request,
            {"PRIVATE_VPC"},
            "Alert history is stored only on the private transmitter node.",
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
            raise HTTPException(status_code=404, detail="Session not found")
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
        try:
            estimate = process_link(body.amplitudes, body.fs_hz)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

        # Fixed per-link delay so a parallel fan-out is faster than calling both receivers in series.
        await asyncio.sleep(0.06)
        logger.info(
            "Link %s HR %.1f RR %.1f activity %s fall %s.",
            body.link_id,
            estimate["heart_bpm"],
            estimate["breath_bpm"],
            estimate["activity"],
            estimate["fall_detected"],
        )
        return LinkEstimate(
            node_id=NODE_ID,
            role=ROLE,
            zone_id=ZONE,
            link_id=body.link_id,
            session_id=body.session_id,
            processing_ms=round((time.perf_counter() - started) * 1000, 1),
            trace_id=trace_id_var.get(),
            **estimate,
        )
