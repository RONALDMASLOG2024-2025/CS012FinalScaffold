# WiCare on the CS012 Distributed Cluster

Local simulation of **WiCare** (Wi-Fi CSI monitoring for a care setting) as a small distributed system. No cloud account, containers, or ESP32 boards are required. The processes on this machine stand in for the ward gateway, the Wi-Fi transmitter, and two receivers.

Sensing follows the WiCare pipeline at simulation scale:

1. The transmitter builds one CSI window (empty-room baseline + breathing + heartbeat).
2. Receiver A and receiver B each run that window's link: baseline removal, subcarrier selection, heart rate, breathing rate, movement, and a fall burst.
3. The gateway fuses the two links. A weak neighbor link is dropped instead of averaged into a fake rate.
4. Rules set **Emergency / Warning / Normal**. The sentence in the response only restates that label.

## Nodes

| Process | URL | Role |
| --- | --- | --- |
| Ward gateway | http://localhost:8000 | Routes work, fuses links, assigns urgency, Swagger docs |
| Worker-TX | http://localhost:8001 | Transmitter. Private store for identity, raw CSI, and alerts |
| Worker-RX-A | http://localhost:8002 | Receiver for zone-a (bed 1) |
| Worker-RX-B | http://localhost:8003 | Receiver for zone-b (bed 2) |

Demo residents already sit on the transmitter: **P001 Ana Reyes** in zone-a, **P002 Jose Cruz** in zone-b.

`zone-a` is covered by receiver A. The person still leaks a weak copy onto receiver B, and the other way around for zone-b. Both receivers always run. The gateway keeps the link that actually covers the bed.

## Distributed behavior this scaffold is meant to show

- **Topology.** One gateway, one transmitter, two receivers.
- **Parallel work.** The gateway calls both receivers at the same time. `timing_ms.receivers_parallel` stays below `timing_ms.receivers_sum`.
- **Routing.** A resident's zone picks the primary receiver. Storage keys route the same way: `zone-a...` to receiver A, `zone-b...` to receiver B, sensitive keys to the transmitter.
- **Hybrid placement.** Identity, raw CSI, and Warning/Emergency alerts stay on the private transmitter. Receivers only compute. Callers on the worker ports must send `X-Network-Zone: PRIVATE_VPC` or `EDGE_COMPUTE`. The gateway adds that header for you.
- **Tracing.** Every hop carries `X-Trace-ID`. The sensing response lists the hops.

## Setup

Python 3.9 or newer.

```powershell
python -m venv venv
venv\Scripts\activate
pip install -r requirements.txt
python run_cluster.py
```

Then, in a second terminal:

```powershell
venv\Scripts\activate
python demo_wicare.py
```

Or open http://localhost:8000/docs and call `POST /api/v1/sense` with:

```json
{"resident_id": "P001", "scenario": "resting"}
```

Scenarios: `resting`, `warning`, `emergency`, `fall`, `walking`. Planted rates and the expected urgency are listed at `GET /api/v1/scenarios`.

Check the pipeline without starting the cluster:

```powershell
python -m common.pipeline
```

## Layout

* `run_cluster.py` — starts the transmitter, both receivers, and the gateway.
* `demo_wicare.py` — runs the five scenarios and a placement check.
* `common/pipeline.py` — CSI synthesis, per-link processing, fusion, and urgency rules.
* `common/models.py` — request and response contracts.
* `common/logger.py` — trace id on each log line.
* `coordinator/main.py` — ward gateway.
* `node/main.py` — transmitter or receiver, selected with the `ROLE` environment variable.

Raw CSI is not returned by the gateway. A short preview is available from `GET /api/v1/sessions/{session_id}` after a sensing call. The numbers are simulated so the pipeline can be checked against a known heart rate and breathing rate. They are not a medical measurement.
