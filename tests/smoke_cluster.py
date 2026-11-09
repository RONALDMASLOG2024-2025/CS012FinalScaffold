"""End-to-end checks for a running WiCare cluster.

Start the services with ``python run_cluster.py`` before running this script.
"""

from __future__ import annotations

import argparse
import concurrent.futures
import json
import os
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any

GATEWAY = os.environ.get("WICARE_GATEWAY", "http://127.0.0.1:8000")


def request(method: str, path: str, payload: dict[str, Any] | None = None) -> Any:
    data = None if payload is None else json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        GATEWAY + path,
        data=data,
        headers={"Content-Type": "application/json"},
        method=method,
    )
    try:
        with urllib.request.urlopen(req, timeout=45) as response:
            body = response.read().decode("utf-8")
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        raise AssertionError(f"{method} {path} returned HTTP {exc.code}: {detail}") from exc
    return json.loads(body) if body else {}


def request_text(path: str) -> str:
    req = urllib.request.Request(GATEWAY + path, method="GET")
    try:
        with urllib.request.urlopen(req, timeout=30) as response:
            return response.read().decode("utf-8")
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        raise AssertionError(f"GET {path} returned HTTP {exc.code}: {detail}") from exc


def require(condition: bool, message: str) -> None:
    if not condition:
        raise AssertionError(message)


def sense(resident_id: str, scenario: str) -> dict[str, Any]:
    return request(
        "POST",
        "/api/v1/sense",
        {"resident_id": resident_id, "scenario": scenario},
    )


def trace_events(trace_id: str) -> list[dict[str, Any]]:
    return request("GET", f"/api/v1/traces/{trace_id}")["events"]


def events_for_trace(trace_id: str) -> list[dict[str, Any]]:
    return request("GET", f"/api/v1/events?trace_id={trace_id}")["events"]


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--export-sample",
        action="store_true",
        help="Save the normal-operation trace to docs/sample-events.jsonl.",
    )
    args = parser.parse_args()

    health = request("GET", "/cluster/health")
    require(
        all(row.get("status") == "HEALTHY" for row in health["cluster_status"].values()),
        "All cluster processes must be healthy before the smoke test.",
    )

    normal = sense("P001", "resting")
    require(normal["status"] == "SUCCESS", "Normal request did not complete successfully.")
    require(normal["alert"]["urgency"] == "Normal", "Normal scenario urgency changed.")
    normal_events = trace_events(normal["trace_id"])
    normal_types = {event["event_type"] for event in normal_events}
    for event_type in (
        "request.received",
        "routing.decision",
        "message.sent",
        "processing.started",
        "processing.completed",
        "request.completed",
    ):
        require(event_type in normal_types, f"Normal trace is missing {event_type}.")
    if args.export_sample:
        exported = request_text(
            f"/api/v1/events/export?trace_id={urllib.parse.quote(normal['trace_id'])}"
        )
        sample_path = Path(__file__).resolve().parents[1] / "docs" / "sample-events.jsonl"
        sample_path.parent.mkdir(parents=True, exist_ok=True)
        sample_path.write_text(exported, encoding="utf-8")
    print("PASS normal request, rule label, and complete distributed trace")

    concurrent_cases = (
        ("P001", "resting"),
        ("P002", "warning"),
        ("P001", "walking"),
        ("P002", "emergency"),
    )
    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
        concurrent_results = list(executor.map(lambda item: sense(*item), concurrent_cases))
    concurrent_trace_ids = {result["trace_id"] for result in concurrent_results}
    require(len(concurrent_trace_ids) == 4, "Concurrent requests did not retain unique trace IDs.")
    print("PASS four concurrent requests retain distinct trace identities")

    request(
        "POST",
        "/api/v1/simulation/faults",
        {"failed_nodes": ["Worker-RX-A"], "partitions": []},
    )
    degraded = sense("P001", "resting")
    require(degraded["status"] == "DEGRADED", "Receiver failure did not produce degraded service.")
    require(
        any(row["receiver_id"] == "Worker-RX-A" for row in degraded["receiver_failures"]),
        "The failed receiver was not reported.",
    )
    failure_events = trace_events(degraded["trace_id"])
    require(
        [row["node_id"] for row in degraded["receivers"]] == ["Worker-RX-B"],
        "The degraded result did not use RX-B as its only surviving receiver.",
    )
    require(
        degraded["routing"]["degraded_fallback"]
        == {
            "after_receiver": "Worker-RX-A",
            "receiver": "Worker-RX-B",
            "after_attempts": 2,
        },
        "The response did not describe the RX-A retry and RX-B fallback route.",
    )
    require(
        sum(
            event["event_type"] == "retry.scheduled"
            and event["operation"] == "process_csi_link"
            and event["destination"] == "Worker-RX-A"
            for event in failure_events
        ) == 1,
        "The failed receiver did not use exactly one bounded retry.",
    )
    primary_sends = [
        event
        for event in failure_events
        if event["event_type"] == "message.sent"
        and event["operation"] == "process_csi_link"
        and event["destination"] == "Worker-RX-A"
    ]
    require(
        [event["attributes"]["attempt"] for event in primary_sends] == [1, 2],
        "RX-A was not called once and retried once before fallback.",
    )
    primary_drops = [
        event
        for event in failure_events
        if event["event_type"] == "message.dropped"
        and event["operation"] == "process_csi_link"
        and event["destination"] == "Worker-RX-A"
    ]
    require(
        [event["attributes"]["attempt"] for event in primary_drops] == [1, 2],
        "RX-A failures were not logged for the first attempt and its single retry.",
    )
    first_send_index = failure_events.index(primary_sends[0])
    first_drop_index = failure_events.index(primary_drops[0])
    retry_index = next(
        index for index, event in enumerate(failure_events)
        if event["event_type"] == "retry.scheduled"
        and event["operation"] == "process_csi_link"
        and event["destination"] == "Worker-RX-A"
    )
    timeout_index = next(
        index for index, event in enumerate(failure_events)
        if event["event_type"] == "timeout"
        and event["operation"] == "process_csi_link"
        and event["destination"] == "Worker-RX-A"
    )
    retry_send_index = failure_events.index(primary_sends[1])
    retry_drop_index = failure_events.index(primary_drops[1])
    fallback_index = next(
        index for index, event in enumerate(failure_events)
        if event["event_type"] == "routing.decision"
        and event["operation"] == "receiver_failover"
    )
    fallback_send_index = next(
        index for index, event in enumerate(failure_events)
        if event["event_type"] == "message.sent"
        and event["operation"] == "process_csi_link"
        and event["destination"] == "Worker-RX-B"
    )
    require(
        first_send_index < first_drop_index < retry_index < retry_send_index
        < retry_drop_index < timeout_index < fallback_index < fallback_send_index,
        "The trace did not show RX-A retry exhaustion before RX-B fallback.",
    )
    fallback_event = failure_events[fallback_index]
    require(
        fallback_event["status"] == "degraded"
        and fallback_event["destination"] == "Worker-RX-B"
        and fallback_event["attributes"]["fallback_from"] == "Worker-RX-A",
        "The routing event did not identify RX-B as RX-A's degraded fallback.",
    )
    require(
        [event["event_id"] for event in events_for_trace(degraded["trace_id"])]
        == [event["event_id"] for event in failure_events],
        "The event stream and trace did not expose the same receiver-failure sequence.",
    )
    print("PASS simulated receiver failure, bounded retry, and degraded fallback")

    request("POST", "/api/v1/simulation/recover", {})
    require(
        all(row.get("status") == "HEALTHY" for row in request("GET", "/cluster/health")["cluster_status"].values()),
        "Recovery did not restore all nodes.",
    )

    key = "telemetry:smoke:room-a:occupancy"
    initial = request(
        "POST",
        "/api/v1/write?replica=Worker-RX-A",
        {"key": key, "value": "in bed", "is_sensitive": False},
    )
    require(initial["data"]["replication_status"] == "REPLICATED", "Initial replica write failed.")
    request(
        "POST",
        "/api/v1/simulation/faults",
        {
            "failed_nodes": [],
            "partitions": [["Worker-RX-A", "Worker-RX-B"]],
        },
    )
    partition_write = request(
        "POST",
        "/api/v1/write?replica=Worker-RX-B",
        {"key": key, "value": "out of bed", "is_sensitive": False},
    )
    require(
        partition_write["data"]["replication_status"] == "PARTITIONED",
        "The partition did not block replica propagation.",
    )
    divergent = request("GET", f"/api/v1/replicas/{urllib.parse.quote(key, safe=':')}")
    require(divergent["diverged"] is True, "Replicas did not diverge during the partition.")
    partition_state = request("GET", "/api/v1/simulation/state")
    require(
        partition_state["faults"]["partitions"] == [["Worker-RX-A", "Worker-RX-B"]],
        "The replica partition did not remain active after the partition write.",
    )
    require(
        partition_state["queue_depth"] == 1,
        "The partition did not leave its pending replica repair visible.",
    )
    require(
        sorted(row["version"] for row in divergent["replicas"] if row["status"] == "FOUND") == [1, 2],
        "The partition did not expose both divergent telemetry versions.",
    )
    print("PASS partition keeps a local telemetry write available and exposes divergence")

    recovery = request("POST", "/api/v1/simulation/recover", {})
    require(recovery["status"] == "RECOVERED", "Recovery did not complete successfully.")
    converged = request("GET", f"/api/v1/replicas/{urllib.parse.quote(key, safe=':')}")
    require(converged["diverged"] is False, "Replicas did not converge after recovery.")
    require(
        {row["value"] for row in converged["replicas"]} == {"out of bed"},
        "Recovery did not keep the highest logical version.",
    )
    require(
        {row["version"] for row in converged["replicas"]} == {2},
        "Recovery did not converge both replicas on the highest logical version.",
    )
    require(key in recovery["converged_keys"], "Recovery did not report the reconciled key.")
    recovered_state = request("GET", "/api/v1/simulation/state")
    require(
        recovered_state["faults"] == {"failed_nodes": [], "partitions": []},
        "Recovery did not clear the active replica partition.",
    )
    require(
        recovered_state["queue_depth"] == 0 and not recovered_state["pending_replications"],
        "Recovery did not clear the replica repair queue.",
    )
    print("PASS anti-entropy recovery converges both replicas on the newest version")

    shard = request(
        "POST",
        "/api/v1/write",
        {"key": "zone-b:occupancy", "value": "in bed", "is_sensitive": False},
    )
    require(shard["node_id"] == "Worker-RX-B", "Zone-b storage did not route to its shard.")
    print("PASS zone-b sharding routes storage to Worker-RX-B")

    reset = request("POST", "/api/v1/simulation/reset", {})
    empty_events = request("GET", "/api/v1/events?after=0")
    metrics = request("GET", "/api/v1/metrics")
    final_state = request("GET", "/api/v1/simulation/state")
    require(reset["status"] == "RESET", "Simulation reset did not complete.")
    require(empty_events["events"] == [], "Reset retained old events.")
    require(metrics["events_total"] == 0 and metrics["requests_completed"] == 0, "Reset retained metrics.")
    require(final_state["faults"] == {"failed_nodes": [], "partitions": []}, "Reset retained fault state.")
    require(final_state["queue_depth"] == 0, "Reset retained pending replication work.")
    print("PASS reset clears events, metrics, pending work, and fault state")
    print("All cluster smoke checks passed.")


if __name__ == "__main__":
    main()
