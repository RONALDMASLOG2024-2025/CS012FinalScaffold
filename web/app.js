"use strict";

const API = "";
const NODE_IDS = ["Coordinator", "Worker-TX", "Worker-RX-A", "Worker-RX-B"];
const NODE_URLS = {
  "Worker-TX": "http://127.0.0.1:8001",
  "Worker-RX-A": "http://127.0.0.1:8002",
  "Worker-RX-B": "http://127.0.0.1:8003",
};
const state = {
  cursor: 0,
  generation: null,
  speed: 1,
  paused: false,
  faults: { failed_nodes: [], partitions: [] },
  events: [],
  traces: new Map(),
  busyNodes: new Set(),
  health: new Map(),
  selectedTrace: "",
  traceFilter: "",
  latestTrace: "",
  recovering: false,
  controlsBusy: false,
  pendingParticles: new Set(),
};

const elements = {
  clusterStatus: document.getElementById("clusterStatus"),
  simulationTime: document.getElementById("simulationTime"),
  scenarioStatus: document.getElementById("scenarioStatus"),
  startButton: document.getElementById("startButton"),
  pauseButton: document.getElementById("pauseButton"),
  resetButton: document.getElementById("resetButton"),
  speedSelect: document.getElementById("speedSelect"),
  networkLatency: document.getElementById("networkLatency"),
  processingLatency: document.getElementById("processingLatency"),
  applyControlButton: document.getElementById("applyControlButton"),
  topologyState: document.getElementById("topologyState"),
  metricCompleted: document.getElementById("metricCompleted"),
  metricFailed: document.getElementById("metricFailed"),
  metricLatency: document.getElementById("metricLatency"),
  metricQueue: document.getElementById("metricQueue"),
  metricRetries: document.getElementById("metricRetries"),
  metricInFlight: document.getElementById("metricInFlight"),
  latestStatus: document.getElementById("latestStatus"),
  latestSummary: document.getElementById("latestSummary"),
  inspectLatestButton: document.getElementById("inspectLatestButton"),
  eventList: document.getElementById("eventList"),
  traceFilter: document.getElementById("traceFilter"),
  filterTraceButton: document.getElementById("filterTraceButton"),
  clearFilterButton: document.getElementById("clearFilterButton"),
  traceSelect: document.getElementById("traceSelect"),
  traceSummary: document.getElementById("traceSummary"),
  traceEvents: document.getElementById("traceEvents"),
  generationLabel: document.getElementById("generationLabel"),
  particles: document.getElementById("particles"),
};

function setScenarioStatus(message, kind = "") {
  elements.scenarioStatus.textContent = message;
  elements.scenarioStatus.dataset.kind = kind;
}

async function api(path, options = {}) {
  const response = await fetch(`${API}${path}`, {
    headers: {
      Accept: "application/json",
      ...(options.body ? { "Content-Type": "application/json" } : {}),
      ...(options.headers || {}),
    },
    ...options,
  });
  const contentType = response.headers.get("content-type") || "";
  const body = contentType.includes("application/json")
    ? await response.json()
    : await response.text();
  if (!response.ok) {
    const detail = body && typeof body === "object" ? body.detail : body;
    throw new Error(typeof detail === "string" ? detail : JSON.stringify(detail || body));
  }
  return body;
}

function jsonOptions(method, payload) {
  return { method, body: JSON.stringify(payload) };
}

function setScenarioButtonsDisabled(disabled) {
  document.querySelectorAll(".scenario-card").forEach((button) => {
    button.disabled = disabled;
  });
  elements.startButton.disabled = disabled;
}

function setClockControlsBusy(busy) {
  state.controlsBusy = busy;
  elements.pauseButton.disabled = busy;
  elements.resetButton.disabled = busy;
  elements.applyControlButton.disabled = busy;
  elements.speedSelect.disabled = busy;
  elements.networkLatency.disabled = busy;
  elements.processingLatency.disabled = busy;
}

function updateLatest(result, traceId = result.trace_id || "") {
  const status = (result.status || "UNKNOWN").toUpperCase();
  elements.latestStatus.textContent = status;
  elements.latestStatus.className = "result-badge";
  if (status === "SUCCESS" || status === "RECOVERED") {
    elements.latestStatus.classList.add("success");
  } else if (status === "DEGRADED" || status === "PARTIAL" || status === "ACTIVE") {
    elements.latestStatus.classList.add("degraded");
  } else if (status === "FAILED" || status === "ERROR") {
    elements.latestStatus.classList.add("failed");
  }

  if (result.alert) {
    elements.latestSummary.textContent =
      `${result.alert.urgency}: ${result.alert.summary} ` +
      `Path ${result.fused.primary_receiver}; ` +
      `${result.receivers.length} receiver result(s).`;
  } else if (result.diverged !== undefined) {
    const values = (result.replicas || [])
      .map((row) => `${row.node_id}: ${row.status === "FOUND" ? `v${row.version} ${JSON.stringify(row.value)}` : row.status}`)
      .join(" | ");
    elements.latestSummary.textContent =
      `Telemetry replicas ${result.diverged ? "DIVERGED" : "MATCH"}: ${values}`;
  } else {
    elements.latestSummary.textContent = result.message || JSON.stringify(result);
  }

  if (traceId) {
    state.latestTrace = traceId;
    elements.inspectLatestButton.disabled = false;
  }
}

async function sense(residentId, scenario) {
  const result = await api("/api/v1/sense", jsonOptions("POST", {
    resident_id: residentId,
    scenario,
  }));
  updateLatest(result);
  return result;
}

async function writeTelemetry(key, value, replica) {
  const query = replica ? `?replica=${encodeURIComponent(replica)}` : "";
  return api(`/api/v1/write${query}`, jsonOptions("POST", {
    key,
    value,
    is_sensitive: false,
  }));
}

async function readReplicas(key) {
  return api(`/api/v1/replicas/${encodeURIComponent(key)}`);
}

async function setFaults(faults) {
  const result = await api(
    "/api/v1/simulation/faults",
    jsonOptions("POST", faults),
  );
  state.faults = result.faults;
  return result;
}

function withReplicaPartition(partitions) {
  const next = partitions.map((pair) => [...pair]);
  const alreadyPartitioned = next.some(
    (pair) => pair.includes("Worker-RX-A") && pair.includes("Worker-RX-B"),
  );
  if (!alreadyPartitioned) {
    next.push(["Worker-RX-A", "Worker-RX-B"]);
  }
  return next;
}

async function runPreset(name) {
  setScenarioButtonsDisabled(true);
  setScenarioStatus(`Starting ${name.replaceAll("-", " ")} scenario...`, "running");
  try {
    if (name === "normal") {
      await sense("P001", "resting");
      setScenarioStatus("Normal operation completed. Click a trace event to inspect its full path.", "success");
      return;
    }

    if (name === "concurrent") {
      const outcomes = await Promise.allSettled([
        sense("P001", "resting"),
        sense("P002", "warning"),
        sense("P001", "walking"),
        sense("P002", "emergency"),
      ]);
      const succeeded = outcomes.filter((item) => item.status === "fulfilled").length;
      setScenarioStatus(
        `Concurrent workload finished: ${succeeded}/4 requests returned. Compare the distinct trace IDs in the log.`,
        succeeded === 4 ? "success" : "warning",
      );
      return;
    }

    if (name === "failure") {
      await setFaults({
        failed_nodes: ["Worker-RX-A"],
        partitions: state.faults.partitions,
      });
      setScenarioStatus("RX-A marked unavailable. Sending a zone-a sensing request...", "running");
      const result = await sense("P001", "resting");
      const fallback = result.routing?.degraded_fallback;
      const outcome = fallback
        ? `${fallback.after_receiver} failed, was retried once (${fallback.after_attempts} attempts total), then ${fallback.receiver} handled the request as a degraded fallback.`
        : `Request status ${result.status}.`;
      setScenarioStatus(
        `Node failure demonstrated: ${outcome}`,
        "warning",
      );
      return;
    }

    if (name === "partition") {
      await setFaults({ failed_nodes: [], partitions: state.faults.partitions });
      const key = "telemetry:demo:bed-a:occupancy";
      await writeTelemetry(key, "in bed", "Worker-RX-A");
      await setFaults({
        failed_nodes: [],
        partitions: withReplicaPartition(state.faults.partitions),
      });
      const write = await writeTelemetry(key, "out of bed", "Worker-RX-B");
      const replicas = await readReplicas(key);
      updateLatest(replicas, write.trace_id);
      setScenarioStatus(
        `Partition active. A local write succeeded with ${write.data.replication_status}; inspect the two versions before recovery.`,
        replicas.diverged ? "warning" : "success",
      );
      return;
    }

    if (name === "recovery") {
      const result = await api("/api/v1/simulation/recover", jsonOptions("POST", {}));
      const replicas = await readReplicas("telemetry:demo:bed-a:occupancy");
      updateLatest(replicas);
      setScenarioStatus(
        `Recovery ${result.status.toLowerCase()}: ${result.converged_keys.length} key(s) converged; ${result.pending_replications.length} remain pending.`,
        result.status === "RECOVERED" ? "success" : "warning",
      );
      return;
    }

    if (name === "replication") {
      await setFaults({ failed_nodes: [], partitions: state.faults.partitions });
      const shard = await api(
        "/api/v1/write",
        jsonOptions("POST", {
          key: "zone-b:occupancy",
          value: "in bed",
          is_sensitive: false,
        }),
      );
      const key = "telemetry:demo:bed-b:occupancy";
      const telemetryWrite = await writeTelemetry(key, "in bed", "Worker-RX-A");
      const replicas = await readReplicas(key);
      updateLatest(replicas);
      if (telemetryWrite.data.replication_status === "REPLICATED" && !replicas.diverged) {
        setScenarioStatus(
          `Zone-b shard routed to ${shard.node_id}; telemetry copied to both edge replicas.`,
          "success",
        );
      } else {
        const replicaState = replicas.diverged
          ? "diverged"
          : replicas.consistent
            ? "matching"
            : "unavailable for comparison";
        setScenarioStatus(
          `Zone-b shard routed to ${shard.node_id}; telemetry write ${telemetryWrite.data.replication_status.toLowerCase()}, replicas ${replicaState}.`,
          "warning",
        );
      }
      return;
    }
  } catch (error) {
    setScenarioStatus(`Scenario failed: ${error.message}`, "error");
  } finally {
    setScenarioButtonsDisabled(false);
    await refreshState();
  }
}

function formatTime(timestamp) {
  const value = new Date(timestamp);
  return Number.isNaN(value.getTime())
    ? "--:--:--"
    : value.toLocaleTimeString([], { hour12: false });
}

function safeClass(value) {
  return String(value || "").toLowerCase().replace(/[^a-z0-9_-]/g, "_");
}

function createEventRow(event, traceRow = false) {
  const row = document.createElement("li");
  row.className = traceRow ? "trace-event" : "event-row";

  if (traceRow) {
    const node = document.createElement("span");
    node.className = "trace-node";
    node.textContent = event.node || event.service || "system";
    const content = document.createElement("div");
    content.className = "trace-event-content";
    const title = document.createElement("strong");
    title.textContent = `${event.event_type} - ${event.status}`;
    const message = document.createElement("span");
    message.textContent = event.message;
    content.append(title, message);
    if (event.duration_ms !== undefined) {
      const duration = document.createElement("span");
      duration.textContent = `Duration: ${Number(event.duration_ms).toFixed(2)} ms`;
      content.append(duration);
    }
    row.append(node, content);
    return row;
  }

  const time = document.createElement("span");
  time.className = "event-time";
  time.textContent = `${Number(event.simulation_time_s || 0).toFixed(2)}s`;

  const content = document.createElement("div");
  content.className = "event-content";
  const metadata = document.createElement("div");
  metadata.className = "event-meta";
  const type = document.createElement("span");
  type.className = "event-type";
  type.textContent = event.event_type;
  const status = document.createElement("span");
  status.className = `event-status ${safeClass(event.status)}`;
  status.textContent = event.status;
  const node = document.createElement("span");
  node.className = "event-node";
  node.textContent = event.node;
  metadata.append(type, status, node);

  const message = document.createElement("div");
  message.className = "event-message";
  message.textContent = event.message;
  content.append(metadata, message);
  row.append(time, content);
  row.title = `Trace ${event.trace_id} | Request ${event.request_id}`;
  row.addEventListener("click", () => selectTrace(event.trace_id, event.request_id));
  return row;
}

function addEvent(event) {
  state.events.push(event);
  if (state.events.length > 2000) {
    state.events.splice(0, state.events.length - 2000);
  }
  state.traces.set(event.trace_id, event.request_id);
  state.latestTrace = event.trace_id;
  state.cursor = Math.max(state.cursor, event.sequence || 0);

  if (!elements.traceSelect.querySelector(`option[value="${CSS.escape(event.trace_id)}"]`)) {
    const option = document.createElement("option");
    option.value = event.trace_id;
    option.textContent = `${event.trace_id} - ${event.request_id}`;
    elements.traceSelect.insertBefore(option, elements.traceSelect.options[1] || null);
  }

  if (!state.traceFilter || event.trace_id === state.traceFilter) {
    elements.eventList.querySelector(".event-empty")?.remove();
    elements.eventList.append(createEventRow(event));
    while (elements.eventList.children.length > 300) {
      elements.eventList.firstElementChild.remove();
    }
  }

  if (event.event_type === "processing.started" && event.node !== "Coordinator") {
    state.busyNodes.add(event.node);
  }
  if (event.event_type === "processing.completed") {
    state.busyNodes.delete(event.node);
  }
  if (event.event_type === "recovery.started") {
    state.recovering = true;
  }
  if (event.event_type === "recovery.completed") {
    state.recovering = false;
  }
  if (event.event_type === "message.sent") {
    animateMessage(event);
  }
  if (event.event_type === "request.completed" || event.event_type === "request.failed") {
    if (event.trace_id) {
      state.latestTrace = event.trace_id;
      elements.inspectLatestButton.disabled = false;
    }
  }
}

function edgeFor(source, destination) {
  const pair = new Set([source, destination]);
  if (pair.has("Coordinator") && pair.has("Worker-TX")) {
    return document.getElementById("edge-coordinator-tx");
  }
  if (pair.has("Coordinator") && pair.has("Worker-RX-A")) {
    return document.getElementById("edge-coordinator-rxa");
  }
  if (pair.has("Coordinator") && pair.has("Worker-RX-B")) {
    return document.getElementById("edge-coordinator-rxb");
  }
  if (pair.has("Worker-RX-A") && pair.has("Worker-RX-B")) {
    return document.getElementById("edge-rxa-rxb");
  }
  return null;
}

function animateMessage(event) {
  if (!event.source || !event.destination || event.destination === "Client") {
    return;
  }
  const path = edgeFor(event.source, event.destination);
  if (!path) {
    return;
  }

  const particle = document.createElementNS("http://www.w3.org/2000/svg", "circle");
  particle.setAttribute("r", "5");
  particle.setAttribute("class", "particle");
  if (path.id === "edge-rxa-rxb") {
    particle.classList.add("replication");
  }
  if (state.faults.failed_nodes.includes(event.destination)) {
    particle.classList.add("failed");
  }
  elements.particles.append(particle);

  const totalLength = path.getTotalLength();
  const durationSimulationMs = 800;
  let progress = 0;
  let previousTime = null;
  function step(now) {
    if (previousTime === null) {
      previousTime = now;
    }
    if (!state.paused) {
      progress += ((now - previousTime) * state.speed) / durationSimulationMs;
    }
    previousTime = now;
    const point = path.getPointAtLength(Math.min(1, progress) * totalLength);
    particle.setAttribute("cx", point.x.toFixed(2));
    particle.setAttribute("cy", point.y.toFixed(2));
    if (progress < 1) {
      requestAnimationFrame(step);
    } else {
      particle.remove();
    }
  }
  requestAnimationFrame(step);
}

async function selectTrace(traceId, requestId) {
  if (!traceId) {
    return;
  }
  state.selectedTrace = traceId;
  elements.traceSelect.value = traceId;
  try {
    const suffix = requestId ? `?request_id=${encodeURIComponent(requestId)}` : "";
    const trace = await api(`/api/v1/traces/${encodeURIComponent(traceId)}${suffix}`);
    elements.traceSummary.textContent =
      `${trace.event_count} event(s) | Trace ${trace.trace_id} | Request ${trace.request_id || requestId || "unknown"}`;
    elements.traceEvents.replaceChildren(...trace.events.map((event) => createEventRow(event, true)));
  } catch (error) {
    elements.traceSummary.textContent = error.message;
    elements.traceEvents.replaceChildren();
  }
}

async function pollEvents() {
  try {
    const response = await api(`/api/v1/events?after=${state.cursor}&limit=250`);
    if (state.generation !== null && response.generation !== state.generation) {
      clearLocalEventState();
    }
    state.generation = response.generation;
    elements.generationLabel.textContent = `Generation ${response.generation}`;
    response.events.forEach(addEvent);
    if (elements.eventList.children.length === 0) {
      const empty = document.createElement("li");
      empty.className = "event-empty";
      empty.textContent = "No events yet. Start a scenario to populate the trace stream.";
      elements.eventList.append(empty);
    }
    updateNodeClasses();
  } catch (error) {
    setClusterStatus("DEGRADED", error.message);
  }
}

function clearLocalEventState() {
  state.cursor = 0;
  state.events = [];
  state.traces.clear();
  state.busyNodes.clear();
  state.latestTrace = "";
  state.selectedTrace = "";
  elements.eventList.replaceChildren();
  elements.traceEvents.replaceChildren();
  elements.traceSelect.replaceChildren(new Option("Select a trace", ""));
  elements.traceSummary.textContent = "Select a trace or click an event row.";
  elements.inspectLatestButton.disabled = true;
  elements.latestStatus.textContent = "NO REQUEST";
  elements.latestStatus.className = "result-badge";
  elements.latestSummary.textContent = "Run a sensing scenario to inspect a request.";
  renderMetrics({
    requests_completed: 0,
    requests_failed: 0,
    average_latency_ms: 0,
    queue_depth: 0,
    retry_count: 0,
    requests_in_flight: 0,
  });
}

function renderMetrics(metrics) {
  elements.metricCompleted.textContent = metrics.requests_completed ?? 0;
  elements.metricFailed.textContent = metrics.requests_failed ?? 0;
  elements.metricLatency.innerHTML = `${Number(metrics.average_latency_ms || 0).toFixed(0)}<span>ms</span>`;
  elements.metricQueue.textContent = metrics.queue_depth ?? 0;
  elements.metricRetries.textContent = metrics.retry_count ?? 0;
  elements.metricInFlight.textContent = metrics.requests_in_flight ?? 0;
}

function setClusterStatus(status, detail = "") {
  const normalized = status === "HEALTHY" ? "healthy" : status === "DEGRADED" ? "degraded" : "failed";
  elements.clusterStatus.className = `status-pill status-${normalized}`;
  elements.clusterStatus.textContent = status;
  if (detail) {
    elements.clusterStatus.title = detail;
  }
}

async function refreshState() {
  try {
    const [simulation, metrics, health] = await Promise.all([
      api("/api/v1/simulation/state"),
      api("/api/v1/metrics"),
      api("/cluster/health"),
    ]);
    state.paused = Boolean(simulation.simulation.paused);
    state.speed = Number(simulation.simulation.speed || 1);
    state.faults = simulation.faults;
    elements.speedSelect.value = String(state.speed);
    elements.networkLatency.value = simulation.simulation.network_latency_ms;
    elements.processingLatency.value = simulation.simulation.processing_latency_ms;
    elements.simulationTime.textContent = `SIM ${Number(simulation.simulation.simulation_time_s).toFixed(3)} s`;
    elements.pauseButton.textContent = state.paused ? "Resume" : "Pause";
    renderMetrics(metrics);

    state.health.clear();
    Object.values(health.cluster_status || {}).forEach((node) => {
      state.health.set(node.node_id, node.status);
    });
    const values = Array.from(state.health.values());
    if (state.faults.partitions.length > 0) {
      setClusterStatus("DEGRADED");
      elements.topologyState.textContent = "Network partition active";
    } else if (values.every((value) => value === "HEALTHY")) {
      setClusterStatus("HEALTHY");
      elements.topologyState.textContent = "All processes reachable";
    } else if (values.some((value) => value === "UNREACHABLE" || value === "SIMULATED_DOWN")) {
      setClusterStatus("DEGRADED");
      elements.topologyState.textContent = "Failure or simulated node outage";
    } else {
      setClusterStatus("DEGRADED");
      elements.topologyState.textContent = "Network partition active";
    }
    updateNodeClasses();
  } catch (error) {
    setClusterStatus("DEGRADED", error.message);
  }
}

function updateNodeClasses() {
  for (const nodeId of NODE_IDS) {
    const element = document.getElementById(`node-${nodeId}`);
    if (!element) {
      continue;
    }
    element.classList.remove("healthy", "busy", "failed", "isolated", "recovering");
    if (nodeId !== "Coordinator" && state.faults.failed_nodes.includes(nodeId)) {
      element.classList.add("failed");
    } else if (
      state.faults.partitions.some((pair) => pair.includes(nodeId))
    ) {
      element.classList.add("isolated");
    } else if (state.recovering && nodeId !== "Coordinator") {
      element.classList.add("recovering");
    } else if (state.busyNodes.has(nodeId)) {
      element.classList.add("busy");
    } else {
      element.classList.add("healthy");
    }
  }
  const replicaEdge = document.getElementById("edge-rxa-rxb");
  const isReplicaPartitioned = state.faults.partitions.some(
    (pair) => pair.includes("Worker-RX-A") && pair.includes("Worker-RX-B"),
  );
  replicaEdge.classList.toggle("partitioned", isReplicaPartitioned);
}

async function refreshMetrics() {
  try {
    renderMetrics(await api("/api/v1/metrics"));
  } catch (error) {
    setClusterStatus("DEGRADED", error.message);
  }
}

async function applyControls() {
  if (state.controlsBusy) {
    return;
  }
  setClockControlsBusy(true);
  try {
    const result = await api(
      "/api/v1/simulation/control",
      jsonOptions("POST", {
        paused: state.paused,
        speed: Number(elements.speedSelect.value),
        network_latency_ms: Number(elements.networkLatency.value),
        processing_latency_ms: Number(elements.processingLatency.value),
      }),
    );
    state.paused = result.simulation.paused;
    state.speed = Number(result.simulation.speed);
    setScenarioStatus("Clock, network latency, and processing latency updated.", "success");
    await refreshState();
  } catch (error) {
    setScenarioStatus(`Could not update controls: ${error.message}`, "error");
  } finally {
    setClockControlsBusy(false);
  }
}

async function togglePause() {
  if (state.controlsBusy) {
    return;
  }
  setClockControlsBusy(true);
  try {
    const result = await api(
      "/api/v1/simulation/control",
      jsonOptions("POST", { paused: !state.paused }),
    );
    state.paused = result.simulation.paused;
    elements.pauseButton.textContent = state.paused ? "Resume" : "Pause";
    setScenarioStatus(
      state.paused ? "Simulation time paused; in-flight simulated waits are held." : "Simulation resumed.",
      "success",
    );
  } catch (error) {
    setScenarioStatus(`Could not change pause state: ${error.message}`, "error");
  } finally {
    setClockControlsBusy(false);
  }
}

async function resetSimulation() {
  if (state.controlsBusy) {
    return;
  }
  setClockControlsBusy(true);
  try {
    const result = await api("/api/v1/simulation/reset", jsonOptions("POST", {}));
    clearLocalEventState();
    state.generation = result.generation;
    state.faults = { failed_nodes: [], partitions: [] };
    state.paused = false;
    state.speed = 1;
    state.recovering = false;
    elements.generationLabel.textContent = `Generation ${result.generation}`;
    setScenarioStatus(
      `Reset complete. Cleared ${result.cleared.join(", ")}.`,
      "success",
    );
    await refreshState();
  } catch (error) {
    setScenarioStatus(`Reset failed: ${error.message}`, "error");
  } finally {
    setClockControlsBusy(false);
  }
}

elements.startButton.addEventListener("click", () => runPreset("normal"));
elements.pauseButton.addEventListener("click", togglePause);
elements.resetButton.addEventListener("click", resetSimulation);
elements.applyControlButton.addEventListener("click", applyControls);
document.querySelectorAll(".scenario-card").forEach((button) => {
  button.addEventListener("click", () => runPreset(button.dataset.scenario));
});
elements.inspectLatestButton.addEventListener("click", () => {
  if (state.latestTrace) {
    selectTrace(state.latestTrace, state.traces.get(state.latestTrace));
  }
});
elements.traceSelect.addEventListener("change", () => {
  const traceId = elements.traceSelect.value;
  if (traceId) {
    selectTrace(traceId, state.traces.get(traceId));
  }
});
elements.filterTraceButton.addEventListener("click", () => {
  state.traceFilter = elements.traceFilter.value.trim();
  elements.eventList.replaceChildren();
  state.events
    .filter((event) => !state.traceFilter || event.trace_id === state.traceFilter)
    .slice(-300)
    .forEach((event) => elements.eventList.append(createEventRow(event)));
  if (state.traceFilter) {
    selectTrace(state.traceFilter, state.traces.get(state.traceFilter));
  }
});
elements.clearFilterButton.addEventListener("click", () => {
  state.traceFilter = "";
  elements.traceFilter.value = "";
  elements.eventList.replaceChildren();
  state.events.slice(-300).forEach((event) => {
    elements.eventList.append(createEventRow(event));
  });
});
elements.speedSelect.addEventListener("change", applyControls);

async function poll() {
  await Promise.all([pollEvents(), refreshState()]);
  window.setTimeout(poll, 800);
}

poll();
