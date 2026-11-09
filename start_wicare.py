"""Prepare and launch the local WiCare cluster and open its dashboard."""

from __future__ import annotations

import json
import os
import subprocess
import sys
import time
import urllib.error
import urllib.request
import webbrowser
from pathlib import Path


ROOT = Path(__file__).resolve().parent
REQUIREMENTS = ROOT / "requirements.txt"
CLUSTER_SCRIPT = ROOT / "run_cluster.py"
GATEWAY_HEALTH_URL = "http://127.0.0.1:8000/cluster/health"
DASHBOARD_URL = "http://localhost:8000/demo"
REQUIRED_MODULES = ("fastapi", "starlette", "uvicorn", "httpx", "pydantic", "qrcode", "PIL")


def _venv_python() -> Path:
    if os.name == "nt":
        return ROOT / "venv" / "Scripts" / "python.exe"
    return ROOT / "venv" / "bin" / "python"


def _prepare_environment() -> Path:
    python = _venv_python()
    if not python.exists():
        print("[WiCare] Creating the project virtual environment...")
        subprocess.run([sys.executable, "-m", "venv", str(ROOT / "venv")], check=True)

    probe = "; ".join(f"import {module}" for module in REQUIRED_MODULES)
    result = subprocess.run(
        [str(python), "-c", probe],
        cwd=ROOT,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        check=False,
    )
    if result.returncode != 0:
        if not REQUIREMENTS.is_file():
            raise FileNotFoundError(f"Requirements file not found: {REQUIREMENTS}")
        print("[WiCare] Installing project dependencies...")
        subprocess.run(
            [str(python), "-m", "pip", "install", "-r", str(REQUIREMENTS)],
            cwd=ROOT,
            check=True,
        )

    return python


def _is_healthy() -> bool:
    try:
        with urllib.request.urlopen(GATEWAY_HEALTH_URL, timeout=1.5) as response:
            body = json.loads(response.read().decode("utf-8"))
        nodes = body.get("cluster_status", {})
        return bool(nodes) and all(node.get("status") == "HEALTHY" for node in nodes.values())
    except (urllib.error.URLError, TimeoutError, json.JSONDecodeError, OSError):
        return False


def _wait_until_healthy(process: subprocess.Popen, timeout_s: float = 45.0) -> bool:
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        if _is_healthy():
            return True
        if process.poll() is not None:
            return False
        time.sleep(0.4)
    return _is_healthy()


def main() -> int:
    if _is_healthy():
        print("[WiCare] The cluster is already running.")
        webbrowser.open(DASHBOARD_URL, new=2)
        print(f"[WiCare] Dashboard: {DASHBOARD_URL}")
        return 0

    try:
        python = _prepare_environment()
    except (OSError, subprocess.CalledProcessError) as exc:
        print(f"[WiCare] Setup failed: {exc}", file=sys.stderr)
        return 1

    print("[WiCare] Starting transmitter, receivers, and ward gateway...")
    process = subprocess.Popen([str(python), str(CLUSTER_SCRIPT)], cwd=ROOT)
    try:
        if _wait_until_healthy(process):
            print("[WiCare] Cluster is ready; opening the dashboard.")
            webbrowser.open(DASHBOARD_URL, new=2)
            print(f"[WiCare] Dashboard: {DASHBOARD_URL}")
            print("[WiCare] Press Ctrl+C here to stop the cluster.")
        else:
            print(
                "[WiCare] The cluster did not become healthy. "
                "Check the service output above.",
                file=sys.stderr,
            )
        return process.wait()
    except KeyboardInterrupt:
        print("\n[WiCare] Stopping the cluster...")
        return 0
    finally:
        if process.poll() is None:
            process.terminate()
            try:
                process.wait(timeout=8)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()


if __name__ == "__main__":
    raise SystemExit(main())
