import json
import os
import subprocess
import sys
import time
import urllib.error
import urllib.request

ROOT = os.path.dirname(os.path.abspath(__file__))


def _wait_until_healthy(timeout_s: float = 20.0) -> bool:
    deadline = time.time() + timeout_s
    url = "http://127.0.0.1:8000/cluster/health"
    while time.time() < deadline:
        try:
            with urllib.request.urlopen(url, timeout=1.5) as resp:
                body = json.loads(resp.read().decode("utf-8"))
            nodes = body.get("cluster_status", {})
            if nodes and all(item.get("status") == "HEALTHY" for item in nodes.values()):
                return True
        except (urllib.error.URLError, TimeoutError, json.JSONDecodeError, OSError):
            pass
        time.sleep(0.4)
    return False


def main():
    try:
        sys.stdout.reconfigure(line_buffering=True)
    except (AttributeError, OSError):
        pass

    print("==================================================")
    print(" Starting WiCare distributed sensing cluster...")
    print(" 1 transmitter + 2 receivers + ward gateway")
    print("==================================================")

    processes = []

    # Private transmitter keeps identity and raw CSI.
    # The two receivers are the distributed pipelines (zone A and zone B).
    workers = [
        {
            "id": "Worker-TX",
            "port": "8001",
            "role": "transmitter",
            "zone": "facility",
            "is_private": "true",
        },
        {
            "id": "Worker-RX-A",
            "port": "8002",
            "role": "receiver",
            "zone": "zone-a",
            "is_private": "false",
            "replica_id": "Worker-RX-B",
            "replica_url": "http://127.0.0.1:8003",
        },
        {
            "id": "Worker-RX-B",
            "port": "8003",
            "role": "receiver",
            "zone": "zone-b",
            "is_private": "false",
            "replica_id": "Worker-RX-A",
            "replica_url": "http://127.0.0.1:8002",
        },
    ]

    for worker in workers:
        print(f"[Cluster Manager] Starting {worker['id']} ({worker['role']}, {worker['zone']}) on http://localhost:{worker['port']}")
        cmd = [
            sys.executable, "-m", "uvicorn", "node.main:app",
            "--host", "127.0.0.1",
            "--port", worker["port"],
            "--log-level", "info",
            "--no-access-log",
        ]
        env = {
            "NODE_ID": worker["id"],
            "PORT": worker["port"],
            "ROLE": worker["role"],
            "ZONE": worker["zone"],
            "IS_PRIVATE": worker["is_private"],
            "REPLICA_ID": worker.get("replica_id", ""),
            "REPLICA_URL": worker.get("replica_url", ""),
            "EVENT_SINK_URL": "http://127.0.0.1:8000/api/v1/events/ingest",
        }
        process = subprocess.Popen(cmd, cwd=ROOT, env={**os.environ, **env})
        processes.append(process)

    print("[Cluster Manager] Starting WiCare ward gateway on http://localhost:8000")
    coord_cmd = [
        sys.executable, "-m", "uvicorn", "coordinator.main:app",
        "--host", "127.0.0.1",
        "--port", "8000",
        "--log-level", "info",
        "--no-access-log",
    ]
    coord_env = {
        "TX_URL": "http://127.0.0.1:8001",
        "RX_A_URL": "http://127.0.0.1:8002",
        "RX_B_URL": "http://127.0.0.1:8003",
    }
    coordinator = subprocess.Popen(coord_cmd, cwd=ROOT, env={**os.environ, **coord_env})
    processes.append(coordinator)

    print("\n[Cluster Manager] Waiting until every node answers /health ...")
    if _wait_until_healthy():
        print("[Cluster Online] Transmitter, both receivers, and the gateway are healthy.")
    else:
        print("[Cluster Manager] Nodes did not all report healthy yet. Check the logs above.")

    print("Ward gateway:        http://localhost:8000")
    print("Animated dashboard:  http://localhost:8000/demo")
    print("Interactive API docs: http://localhost:8000/docs")
    print("Try a window:         python demo_wicare.py")
    print("Press Ctrl+C to stop all cluster processes.\n")

    try:
        coordinator.wait()
    except KeyboardInterrupt:
        print("\n[Cluster Manager] Shutting down all cluster nodes...")
    finally:
        for process in processes:
            if process.poll() is None:
                process.terminate()
        print("[Cluster Manager] Cluster stopped.")


if __name__ == "__main__":
    main()
