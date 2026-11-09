import asyncio
import unittest

from common.simulation import EventStore, SimulationClock, create_event


class SimulationClockTests(unittest.IsolatedAsyncioTestCase):
    async def test_pause_holds_simulation_time_until_resume(self):
        clock = SimulationClock()
        clock.configure(paused=True)
        paused_at = clock.now()
        sleeping = asyncio.create_task(clock.sleep(0.02))

        await asyncio.sleep(0.04)
        self.assertAlmostEqual(clock.now(), paused_at, delta=0.002)
        self.assertFalse(sleeping.done())

        clock.configure(paused=False)
        await asyncio.wait_for(sleeping, timeout=0.5)
        self.assertGreaterEqual(clock.now(), paused_at + 0.02)

    async def test_speed_changes_simulated_sleep_duration(self):
        clock = SimulationClock(speed=4.0)
        started = clock.now()
        await clock.sleep(0.08)
        self.assertGreaterEqual(clock.now() - started, 0.08)


class EventStoreTests(unittest.TestCase):
    def test_metrics_filters_and_reset_generation(self):
        clock = SimulationClock()
        store = EventStore(clock)
        received = create_event(
            "request.received",
            trace_id="trace-a",
            request_id="request-a",
            span_id="span-a",
            parent_span_id=None,
            generation=0,
            service="Coordinator",
            node="Coordinator",
            operation="POST /api/v1/sense",
            status="received",
            message="Request received.",
        )
        completed = create_event(
            "request.completed",
            trace_id="trace-a",
            request_id="request-a",
            span_id="span-a",
            parent_span_id=None,
            generation=0,
            service="Coordinator",
            node="Coordinator",
            operation="POST /api/v1/sense",
            status="success",
            message="Request completed.",
            duration_ms=12.5,
        )
        store.record(received)
        store.record(completed)

        self.assertEqual(len(store.query(trace_id="trace-a")), 2)
        self.assertEqual(store.metrics()["requests_completed"], 1)
        self.assertEqual(store.metrics()["average_latency_ms"], 12.5)

        new_generation = store.reset()
        stale = create_event(
            "request.failed",
            trace_id="trace-old",
            request_id="request-old",
            span_id="span-old",
            parent_span_id=None,
            generation=0,
            service="Coordinator",
            node="Coordinator",
            operation="POST /api/v1/sense",
            status="failure",
            message="Stale event.",
        )
        self.assertEqual(new_generation, 1)
        self.assertIsNone(store.record(stale))
        self.assertEqual(store.query(), [])
        self.assertEqual(store.metrics()["events_total"], 0)


if __name__ == "__main__":
    unittest.main()
