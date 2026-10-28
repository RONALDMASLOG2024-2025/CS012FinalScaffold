"""Call the ward gateway and print one line per WiCare scenario."""

from __future__ import annotations

import json
import sys
import urllib.error
import urllib.request

GATEWAY = "http://127.0.0.1:8000"

CASES = [
    ("P001", "resting", "Normal"),
    ("P001", "warning", "Warning"),
    ("P002", "emergency", "Emergency"),
    ("P002", "fall", "Emergency"),
    ("P001", "walking", "Normal"),
]


def _request(method: str, path: str, payload: dict | None = None) -> dict:
    data = None if payload is None else json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        GATEWAY + path,
        data=data,
        headers={"Content-Type": "application/json"},
        method=method,
    )
    with urllib.request.urlopen(req, timeout=20) as resp:
        return json.loads(resp.read().decode("utf-8"))


def main() -> None:
    try:
        health = _request("GET", "/cluster/health")
    except urllib.error.URLError as exc:
        print("Gateway is not up. Start it with: python run_cluster.py")
        print(exc)
        raise SystemExit(1) from exc

    print("Cluster")
    for url, node in health["cluster_status"].items():
        print(f"  {node.get('node_id', url):12} {node.get('role', '?'):12} {node.get('status')}")

    print("\nSensing")
    failed = False
    for resident_id, scenario, expected in CASES:
        body = _request(
            "POST",
            "/api/v1/sense",
            {"resident_id": resident_id, "scenario": scenario},
        )
        fused = body["fused"]
        alert = body["alert"]
        timing = body["timing_ms"]
        ok = alert["urgency"] == expected and timing["receivers_parallel"] < timing["receivers_sum"]
        mark = "OK" if ok else "CHECK"
        if not ok:
            failed = True
        print(
            f"  [{mark}] {resident_id} {scenario:10} "
            f"-> {alert['urgency']:9} (expected {expected:9}) "
            f"HR {fused['heart_bpm']:6.1f} RR {fused['breath_bpm']:5.1f} "
            f"via {fused['primary_receiver']} "
            f"parallel {timing['receivers_parallel']:.0f} ms "
            f"vs sum {timing['receivers_sum']:.0f} ms "
            f"trace {body['trace_id']}"
        )
        print(f"         {alert['summary']}")

    write = _request(
        "POST",
        "/api/v1/write",
        {"key": "resident:P001:note", "value": "prefers a night check", "is_sensitive": True},
    )
    zone_write = _request(
        "POST",
        "/api/v1/write",
        {"key": "zone-b:occupancy", "value": "in bed", "is_sensitive": False},
    )
    print("\nPlacement")
    print(f"  sensitive note  -> {write['node_id']} ({write['network_zone']})")
    print(f"  zone-b telemetry -> {zone_write['node_id']} ({zone_write['network_zone']})")

    if failed:
        raise SystemExit(1)
    print("\nDemo finished.")


if __name__ == "__main__":
    main()
