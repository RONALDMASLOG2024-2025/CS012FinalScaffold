from __future__ import annotations

import asyncio
import os
import time
import uuid
from datetime import datetime

import httpx
from fastapi import FastAPI, HTTPException, Request

from common.logger import generate_trace_id, get_logger, trace_id_var
from common.models import DataPayload, DistributedResponse, ResidentRecord, SenseRequest
from common.pipeline import abnormality_tags, alert_summary, classify_urgency, fuse_links, scenario_catalog

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

TX_URL = os.getenv("TX_URL", "http://localhost:8001").rstrip("/")
RX_A_URL = os.getenv("RX_A_URL", "http://localhost:8002").rstrip("/")
RX_B_URL = os.getenv("RX_B_URL", "http://localhost:8003").rstrip("/")
WORKER_NODES = [TX_URL, RX_A_URL, RX_B_URL]
RX_BY_ID = {
    "Worker-RX-A": RX_A_URL,
    "Worker-RX-B": RX_B_URL,
}
ZONE_PRIMARY = {
    "zone-a": "Worker-RX-A",
    "zone-b": "Worker-RX-B",
}

ARCHITECTURE = {
    "system": "WiCare",
    "sensing": "Wi-Fi CSI vital signs for one resident at a time",
    "topology": "star: ward gateway, one transmitter, two receivers",
    "placement": {
        "Worker-TX": "Private facility node. Holds resident identity, raw CSI, and the alert archive.",
        "Worker-RX-A": "Edge compute for zone-a (bed 1, about 2 m from the transmitter).",
        "Worker-RX-B": "Edge compute for zone-b (bed 2, about 2 m from the transmitter).",
        "Coordinator": "Gateway. Routes work, fuses the two links, and applies urgency rules.",
    },
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
    trace_id_var.set(trace_id)
    logger.info("Incoming gateway request: %s %s", request.method, request.url.path)
    response = await call_next(request)
    response.headers["X-Trace-ID"] = trace_id
    return response


def _headers(zone: str) -> dict[str, str]:
    return {"X-Trace-ID": trace_id_var.get(), "X-Network-Zone": zone}


def _route_storage(key: str, is_sensitive: bool) -> tuple[str, str]:
    """Clinical records stay on the transmitter. Zone telemetry stays on that zone's receiver."""
    if is_sensitive or key.startswith("resident:") or key.startswith("alert:"):
        return TX_URL, "PRIVATE_VPC"
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
        "docs": "/docs",
        "topology": "/cluster/topology",
        "health": "/cluster/health",
        "sense": "/api/v1/sense",
    }


@app.get("/cluster/health")
async def cluster_health():
    headers = _headers("EDGE_COMPUTE")
    async with httpx.AsyncClient(timeout=2.0) as client:
        tasks = [client.get(f"{node}/health", headers=headers) for node in WORKER_NODES]
        results = await asyncio.gather(*tasks, return_exceptions=True)

    status_report = {}
    for node, resp in zip(WORKER_NODES, results):
        if isinstance(resp, Exception):
            status_report[node] = {"status": "UNREACHABLE", "error": str(resp)}
        else:
            status_report[node] = resp.json()
    return {"cluster_status": status_report, "trace_id": trace_id_var.get()}


@app.get("/cluster/topology")
async def cluster_topology():
    live = await cluster_health()
    return {"architecture": ARCHITECTURE, "live": live}


@app.get("/api/v1/scenarios")
async def list_scenarios():
    return {"scenarios": scenario_catalog()}


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
async def sense(body: SenseRequest):
    """Run one distributed WiCare window: transmit, both receivers, then fuse."""
    started = time.perf_counter()
    hops = [{"node": "Coordinator", "action": "accepted sensing request", "resident_id": body.resident_id}]

    async with httpx.AsyncClient(timeout=15.0) as client:
        resident_resp = await client.get(
            f"{TX_URL}/residents/{body.resident_id}",
            headers=_headers("PRIVATE_VPC"),
        )
        if resident_resp.status_code == 404:
            raise HTTPException(status_code=404, detail="Resident not found on the private transmitter node.")
        if resident_resp.status_code != 200:
            raise HTTPException(status_code=503, detail="Transmitter could not load the resident record.")
        resident = resident_resp.json()
        primary_id = ZONE_PRIMARY.get(resident["zone_id"])
        if primary_id is None:
            raise HTTPException(status_code=400, detail="Resident zone is not covered by a receiver.")

        hops.append(
            {
                "node": "Worker-TX",
                "action": "loaded resident identity from the private store",
                "zone_id": resident["zone_id"],
                "primary_receiver": primary_id,
            }
        )

        tx_started = time.perf_counter()
        tx_resp = await client.post(
            f"{TX_URL}/transmit",
            headers=_headers("PRIVATE_VPC"),
            json={
                "resident_id": resident["resident_id"],
                "zone_id": resident["zone_id"],
                "scenario": body.scenario,
                "duration_s": body.duration_s,
                "session_id": uuid.uuid4().hex[:12],
            },
        )
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

        async def run_receiver(link: dict):
            url = RX_BY_ID.get(link["receiver_id"])
            if url is None:
                raise HTTPException(status_code=500, detail=f"No route for {link['receiver_id']}")
            response = await client.post(
                f"{url}/process",
                headers=_headers("EDGE_COMPUTE"),
                json={
                    "session_id": session["session_id"],
                    "link_id": link["link_id"],
                    "receiver_id": link["receiver_id"],
                    "zone_id": resident["zone_id"],
                    "fs_hz": session["fs_hz"],
                    "amplitudes": link["amplitudes"],
                },
            )
            response.raise_for_status()
            return response.json()

        parallel_started = time.perf_counter()
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
            async with httpx.AsyncClient(timeout=3.0) as client:
                saved = await client.post(f"{TX_URL}/alerts", headers=_headers("PRIVATE_VPC"), json=alert)
            archive_status = "STORED" if saved.status_code == 200 else "FAILED"
        except httpx.RequestError:
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
    return {
        "status": "SUCCESS" if not failures else "DEGRADED",
        "trace_id": trace_id_var.get(),
        "resident": resident,
        "session_id": session["session_id"],
        "scenario": body.scenario,
        "routing": {
            "primary_receiver": primary_id,
            "reason": f"{resident['zone_id']} is the Fresnel zone covered by {primary_id}.",
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
async def write_data(payload: DataPayload):
    target, zone = _route_storage(payload.key, payload.is_sensitive)
    logger.info("Routing key '%s' -> %s (%s)", payload.key, target, zone)
    async with httpx.AsyncClient(timeout=3.0) as client:
        try:
            resp = await client.post(f"{target}/store", json=payload.model_dump(), headers=_headers(zone))
        except httpx.RequestError as exc:
            raise HTTPException(status_code=503, detail=f"Target node {target} unavailable: {exc}") from exc
    if resp.status_code >= 400:
        raise HTTPException(status_code=resp.status_code, detail=_error_detail(resp))
    body = DistributedResponse(**resp.json())
    return body.model_copy(update={"routed_to": target, "network_zone": zone})


@app.get("/api/v1/read/{key}", response_model=DistributedResponse)
async def read_data(key: str, is_sensitive: bool = False):
    target, zone = _route_storage(key, is_sensitive)
    async with httpx.AsyncClient(timeout=3.0) as client:
        try:
            resp = await client.get(f"{target}/read/{key}", headers=_headers(zone))
        except httpx.RequestError as exc:
            raise HTTPException(status_code=503, detail=f"Target node {target} unavailable: {exc}") from exc
    if resp.status_code == 404:
        raise HTTPException(status_code=404, detail="Key not found in the distributed store.")
    if resp.status_code >= 400:
        raise HTTPException(status_code=resp.status_code, detail=_error_detail(resp))
    body = DistributedResponse(**resp.json())
    return body.model_copy(update={"routed_to": target, "network_zone": zone})


async def _proxy_json(method: str, url: str, zone: str, payload: dict | None = None):
    async with httpx.AsyncClient(timeout=3.0) as client:
        try:
            resp = await client.request(method, url, headers=_headers(zone), json=payload)
        except httpx.RequestError as exc:
            raise HTTPException(status_code=503, detail=f"Private transmitter unavailable: {exc}") from exc
    if resp.status_code >= 400:
        raise HTTPException(status_code=resp.status_code, detail=_error_detail(resp))
    return resp.json()


def _error_detail(resp: httpx.Response):
    try:
        body = resp.json()
    except ValueError:
        return resp.text
    return body.get("detail", body)
