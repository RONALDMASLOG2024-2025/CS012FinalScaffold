from __future__ import annotations

import asyncio
import time
import uuid
from collections import deque
from datetime import datetime, timezone
from statistics import mean
from typing import Any


class SimulationClock:
    def __init__(
        self,
        *,
        speed: float = 1.0,
        network_latency_ms: int = 20,
        processing_latency_ms: int = 250,
    ) -> None:
        self.speed = speed
        self.network_latency_ms = network_latency_ms
        self.processing_latency_ms = processing_latency_ms
        self.paused = False
        self._simulation_time_s = 0.0
        self._last_wall_time = time.monotonic()
        self._resumed = asyncio.Event()
        self._resumed.set()

    def now(self) -> float:
        wall_time = time.monotonic()
        if not self.paused:
            self._simulation_time_s += (wall_time - self._last_wall_time) * self.speed
        self._last_wall_time = wall_time
        return self._simulation_time_s

    def configure(
        self,
        *,
        paused: bool | None = None,
        speed: float | None = None,
        network_latency_ms: int | None = None,
        processing_latency_ms: int | None = None,
    ) -> None:
        self.now()
        if speed is not None:
            self.speed = speed
        if network_latency_ms is not None:
            self.network_latency_ms = network_latency_ms
        if processing_latency_ms is not None:
            self.processing_latency_ms = processing_latency_ms
        if paused is not None:
            self.paused = paused
        if self.paused:
            self._resumed.clear()
        else:
            self._resumed.set()

    async def sleep(self, duration_s: float) -> None:
        if duration_s <= 0:
            return

        target = self.now() + duration_s
        while self.now() < target:
            if self.paused:
                await self._resumed.wait()
                continue
            remaining = target - self.now()
            await asyncio.sleep(min(0.05, remaining / self.speed))

    def reset(self) -> None:
        self.speed = 1.0
        self.network_latency_ms = 20
        self.processing_latency_ms = 250
        self.paused = False
        self._simulation_time_s = 0.0
        self._last_wall_time = time.monotonic()
        self._resumed.set()

    def snapshot(self) -> dict[str, int | float | bool]:
        return {
            "simulation_time_s": round(self.now(), 3),
            "paused": self.paused,
            "speed": self.speed,
            "network_latency_ms": self.network_latency_ms,
            "processing_latency_ms": self.processing_latency_ms,
        }


def create_event(
    event_type: str,
    *,
    trace_id: str,
    request_id: str,
    span_id: str | None,
    parent_span_id: str | None,
    generation: int,
    service: str,
    node: str,
    operation: str,
    status: str,
    message: str,
    source: str | None = None,
    destination: str | None = None,
    duration_ms: float | None = None,
    attributes: dict[str, Any] | None = None,
) -> dict[str, Any]:
    event: dict[str, Any] = {
        "event_id": uuid.uuid4().hex,
        "event_type": event_type,
        "timestamp": datetime.now(timezone.utc).isoformat(timespec="milliseconds"),
        "trace_id": trace_id,
        "request_id": request_id,
        "span_id": span_id,
        "parent_span_id": parent_span_id,
        "source": source,
        "destination": destination,
        "service": service,
        "node": node,
        "operation": operation,
        "status": status,
        "message": message,
        "generation": generation,
        "attributes": attributes or {},
    }
    if duration_ms is not None:
        event["duration_ms"] = round(duration_ms, 2)
    return event


class EventStore:
    def __init__(self, clock: SimulationClock, max_events: int = 5000) -> None:
        self.clock = clock
        self._events: deque[dict[str, Any]] = deque(maxlen=max_events)
        self._sequence = 0
        self.generation = 0

    def record(self, event: dict[str, Any]) -> dict[str, Any] | None:
        if int(event.get("generation", self.generation)) != self.generation:
            return None

        stored = dict(event)
        self._sequence += 1
        stored["sequence"] = self._sequence
        stored["simulation_time_s"] = round(self.clock.now(), 3)
        self._events.append(stored)
        return stored

    def query(
        self,
        *,
        after: int = 0,
        trace_id: str | None = None,
        request_id: str | None = None,
        limit: int = 500,
    ) -> list[dict[str, Any]]:
        filtered = [
            event
            for event in self._events
            if event["sequence"] > after
            and (trace_id is None or event["trace_id"] == trace_id)
            and (request_id is None or event["request_id"] == request_id)
        ]
        return filtered[-limit:]

    def reset(self) -> int:
        self.generation += 1
        self._events.clear()
        self._sequence = 0
        return self.generation

    def metrics(self, *, queue_depth: int = 0) -> dict[str, int | float]:
        events = list(self._events)
        terminal = [
            event
            for event in events
            if event["event_type"] in {"request.completed", "request.failed"}
        ]
        durations = [
            float(event["duration_ms"])
            for event in terminal
            if "duration_ms" in event
        ]
        started = {
            event["request_id"]
            for event in events
            if event["event_type"] == "request.received"
        }
        finished = {event["request_id"] for event in terminal}
        return {
            "requests_received": sum(
                event["event_type"] == "request.received" for event in events
            ),
            "requests_completed": sum(
                event["event_type"] == "request.completed" for event in events
            ),
            "requests_failed": sum(
                event["event_type"] == "request.failed" for event in events
            ),
            "requests_in_flight": len(started - finished),
            "average_latency_ms": round(mean(durations), 2) if durations else 0.0,
            "retry_count": sum(
                event["event_type"] == "retry.scheduled" for event in events
            ),
            "timeout_count": sum(
                event["event_type"] == "timeout" for event in events
            ),
            "queue_depth": queue_depth,
            "events_total": len(events),
        }
