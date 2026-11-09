# WiCare Final Presentation and Recording Guide

This outline is grounded in the code and the WiCare context documented in the project README. It does not claim that simulated signals are clinical measurements or invent team-member contributions.

## 15-minute presentation outline

| Time | Segment | Content |
| --- | --- | --- |
| 0:00-1:00 | Overview | State the WiCare problem context: using Wi-Fi CSI as a research prototype for care-setting activity and vital-sign estimation. Explain that this submission is a local distributed simulation with synthetic signals. |
| 1:00-3:00 | Architecture | Show the topology diagram. Identify the private transmitter, two zone receivers, gateway, hybrid-cloud target model, zone routing, telemetry replicas, and the event/trace path. |
| 3:00-13:00 | Demonstration | Follow the timestamped recording script below: normal request, concurrent traces, node failure, telemetry partition, recovery, replication/sharding, and trace/log evidence. |
| 13:00-15:00 | Conclusion | Summarize the implemented distributed behavior, the AP-style telemetry tradeoff, what is simulated, known limitations, and the next engineering improvements. End by reminding viewers that this is not a medical device. |

## Recording preparation

1. Install the project dependencies and start the services:

   ```powershell
   python -m venv venv
   venv\Scripts\activate
   python -m pip install -r requirements.txt
   python run_cluster.py
   ```

2. In a browser, open `http://localhost:8000/demo`. Wait until the cluster badge says `HEALTHY`.
3. Use a readable browser zoom (about 100-110%) and a wide window so the topology, controls, and event list are visible.
4. Click **Reset** once before recording. This clears event history and simulated state while preserving the two demo residents.
5. Keep the service terminal visible only if useful; do not show personal files or credentials. All data in the demo is synthetic.
6. If the environment is under heavy CPU load, let each scenario finish before starting the next. The request latency shown is measured host/simulation time and will vary.

## Exact 10-minute demonstration script

### 3:00-4:15 - Normal operation

**Actions**

1. Click **Normal operation** (or **Start normal run**).
2. Wait for the result badge and event stream to settle.
3. Click **Open its trace** or click a log event.

**Expected visual result**

- Particles travel from the gateway to Worker-TX and both receivers.
- Receiver nodes briefly show busy.
- The result is `SUCCESS` with the `Normal` rule label for P001.
- The event log contains request receipt, routing, message, processing, database, fusion, and request completion events.
- The trace inspector shows the same trace ID and the end-to-end hop sequence.

**Speaking notes**

> The transmitter creates synthetic data for this session and retains the raw window. The receivers process separate links in parallel. The gateway fuses the results and applies the existing urgency rules. Every displayed path corresponds to an event from this request.

**Concept and limit**

This demonstrates service routing, parallel edge work, rule-based fusion, and tracing. Synthetic samples do not validate real Wi-Fi sensing or clinical accuracy.

### 4:15-5:30 - Concurrent workload

**Actions**

1. Click **Concurrent workload**.
2. Point to the interleaved event rows and recent request traces.
3. Select two trace IDs in turn and compare their request IDs.

**Expected visual result**

- Four requests run concurrently with four distinct trace identities.
- Event rows interleave while each trace inspector remains scoped to one request.
- Completed and in-flight metrics update from the event stream.

**Speaking notes**

> The gateway handles independent requests at the same time. The trace and request IDs keep their events separate even though the event stream interleaves them.

**Concept and limit**

This demonstrates asynchronous request handling and trace isolation. Host scheduling, especially CPU-bound signal processing, affects measured latency.

### 5:30-7:00 - Receiver failure and degraded fallback

**Actions**

1. Click **Receiver failure**.
2. Show Worker-RX-A's failed state and the retry count.
3. Open the request trace and point to `failure.detected`, `retry.scheduled`, and the final request result.

**Expected visual result**

- The gateway treats RX-A as unavailable in the data plane.
- One bounded retry is visible for the failed receiver.
- RX-B still processes its link; the request returns `DEGRADED`.
- Health and topology indicate the injected fault while the actual process remains reachable to the control plane.

**Speaking notes**

> The fault is a deterministic gateway overlay rather than killing an operating-system process. The retry limit is two attempts total. The surviving neighbor link can provide a degraded result but may not have enough confidence for a reliable vital estimate.

**Concept and limit**

This demonstrates bounded retry and graceful degradation. It does not claim that a weak neighboring receiver can replace the primary sensor.

### 7:00-9:00 - Partition and CAP tradeoff

**Actions**

1. Click **CAP partition**.
2. Point to the partitioned RX-A/RX-B replication edge.
3. Show the successful local-write result and the replica inspector values.
4. Point to `replication.blocked` and `queue.enqueued` in the event stream.

**Expected visual result**

- A baseline telemetry value is first replicated to both receivers.
- The direct replica link is then marked partitioned.
- A newer write targeted at RX-B succeeds locally; RX-A retains the older value.
- The two versions and pending repair depth show the divergence.

**Speaking notes**

> For this non-sensitive telemetry operation, the system favors availability: a reachable replica accepts a local write instead of waiting for synchronous consistency across a broken link. This permits stale reads and temporary divergence. This is not a linearizable data path, and the central gateway remains available in this small model.

**Concept and limit**

This is the project's explicit CAP tradeoff for replicated telemetry. Private resident data is single-copy and is not included in this CAP claim.

### 9:00-10:30 - Recovery

**Actions**

1. Click **Recovery**.
2. Watch the replica edge return to normal and the repair queue drain.
3. Show both replica values and versions after synchronization.

**Expected visual result**

- Recovery events appear.
- Nodes exchange versioned telemetry.
- Both replicas hold the higher logical version and the repair queue reaches zero.

**Speaking notes**

> Once communication is restored, anti-entropy sends the current records in both directions. The replica applies the larger logical version, with writer ID as a deterministic tie-breaker.

**Concept and limit**

This demonstrates eventual convergence in the simulation. The repair backlog is in memory and is not durable across process restarts.

### 10:30-11:30 - Replication and sharding

**Actions**

1. Click **Replication and shards**.
2. Point out that the `zone-b:occupancy` write routes to Worker-RX-B.
3. Show a `telemetry:` value present at both edge replicas.

**Expected visual result**

- The gateway's route decision identifies the target shard.
- The telemetry write is acknowledged by the peer replica.
- Database and message events describe the operation.

**Speaking notes**

> Zone-specific keys use deterministic routing. The separate telemetry namespace is replicated because a second non-sensitive copy supports the availability and recovery demonstration.

**Concept and limit**

These are in-memory educational stores, not durable database shards or a production replication protocol.

### 11:30-13:00 - Observability and hand-off

**Actions**

1. Select the normal-operation trace again.
2. Show event timestamps, simulation times, span/parent-span links, and actual durations.
3. Click **Export JSONL**.
4. Point to the metrics and summarize one limitation.

**Speaking notes**

> The same event stream feeds the dashboard, trace inspector, export, and event-derived metrics. The next steps would be durable storage and repair, stronger security, hardware-in-the-loop tests, and independent gateway failure handling.

## 13:00-15:00 - Conclusion notes

- Restate the topology and the real data path through the gateway, private transmitter, and edge receivers.
- Summarize bounded retries, degraded sensing, telemetry replication, visible partition divergence, and recovery.
- State the exact boundary: local processes execute, but cloud placement and injected network faults are modeled, not deployed.
- Close with the synthetic-data and non-medical disclaimer.

## Individual reflection prompts

Answer these individually using your own work and experiences. Do not copy the example outline as a claim about a contribution.

1. Which specific files, tests, diagrams, or presentation tasks did I personally contribute?
2. What technical decision or implementation detail did I learn most from?
3. What issue or failed approach did I encounter, and how did I verify the final behavior?
4. How did my work connect to distributed-systems concepts such as routing, replication, CAP, observability, or resilience?
5. What would I improve if I had another iteration, and why?
6. What part of the system was simulated rather than deployed, and how does that affect my conclusions?

### Customizable reflection outline

> My contribution was **[describe only work I completed]**. I worked on **[specific files/features/evidence]**. One challenge was **[real challenge]**, which I addressed by **[actions and verification]**. This helped me understand **[concept]**, especially **[specific implementation behavior]**. A limitation I noticed is **[limitation]**. If I continued the project, I would **[specific improvement and reason]**.

## Rubric mapping

| Rubric area | Concrete implementation | Evidence to show |
| --- | --- | --- |
| Architecture design - 25 points | Gateway/transmitter/receiver topology, hybrid-cloud target mapping, private and edge placement, zone routes, Mermaid diagram | README architecture section and dashboard topology |
| Distributed implementation and routing - 25 points | Four local FastAPI processes, CSI fan-out, per-zone routes, sensitive-data placement, telemetry replicas, two-attempt retries | `/api/v1/sense`, `/api/v1/write`, trace and route-decision events |
| Cloud simulation - 20 points | Configurable simulated latency and speed, independent processes, health/failure overlays, replica partition and recovery | Dashboard controls and the failure/partition/recovery presets |
| Event tracing and logging - 20 points | Structured event IDs, trace/request/span IDs, live log, trace filter, measured durations, metrics, JSONL export | Request inspector and exported `docs/sample-events.jsonl` |
| Presentation - 10 points | Repeatable scenario buttons, readable topology, recording outline and exact action notes | This guide and the dashboard demo |

## Known limitations to disclose

- Signals are generated from planted values; no radio hardware, empirical dataset, or clinical validation is included.
- Simulated partitions and node failures do not alter the actual OS network or terminate worker processes.
- The gateway, transmitter store, event store, logical versions, and repair backlog are single-process/in-memory components.
- The telemetry policy is simple last-writer-wins and does not provide quorum or linearizable behavior.
- Network-zone headers are instructional placement labels, not authentication.
