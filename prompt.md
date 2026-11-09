
Act as a senior distributed systems engineer, Python developer, and technical presentation designer. Help me complete my final project by extending my existing code into a complete, working, animated simulation of a cloud-based distributed architecture.

Use my thesis and existing implementation as the foundation. I will provide them below or attach them to this conversation.

## Project requirements

Redesign the architecture of our thesis into a cloud-based distributed system that demonstrates:

1. Network topology.
2. CAP theorem and the system’s actual behavior during a network partition.
3. Sharding or replication where appropriate.
4. Cloud components and a clearly identified cloud deployment model.
5. Observability, distributed tracing, and system resilience.
6. A Python simulation showing how data flows between nodes and features.
7. An event and visible log entry for every meaningful processing step.

Cloud deployment and Docker are optional. The simulation must run locally without paid cloud services or cloud credentials.

The project will be assessed using:

- Architecture design: 25 points.
- Distributed implementation and routing: 25 points.
- Cloud simulation: 20 points.
- Event tracing and logging: 20 points.
- Presentation: 10 points.

We must also submit a repository, a 15-minute presentation video, and individual reflections.

## Your first steps

Before implementing:

- Inspect all provided code and understand the thesis problem, users, features, and current data flow.
- Identify what already works, what can be reused, and what is missing.
- Preserve working functionality and follow the existing project structure where practical.
- If you have workspace access, inspect the actual files instead of asking me to paste information already available.
- Ask only essential questions that cannot be answered from the materials. Do not invent thesis details.
- If a nonessential detail is missing, state a reasonable assumption and proceed.
- Explain the proposed architecture briefly, then implement it. Do not stop after giving a plan.

## Architecture and simulation

Choose an architecture appropriate to our thesis rather than adding components just to make the diagram look complex.

Where relevant, include:

- Clients or request generators.
- An API gateway or load balancer.
- Application service nodes.
- A message queue and background workers.
- Database shards or replicas.
- A cache, only if it serves a meaningful purpose.
- An observability component for logs, metrics, and traces.

Explain which components represent cloud services, which cloud deployment model we use, and how the logical topology maps to a possible cloud deployment.

Implement meaningful distributed behavior in Python. A local asynchronous simulation is acceptable if it accurately models independent nodes, communication delays, routing, failures, and recovery. Clearly describe which parts are simulated and which parts actually execute.

The simulation should support:

- Request generation using realistic thesis-related operations.
- Routing decisions visible in both the animation and logs.
- Configurable node processing and network latency.
- Concurrent requests.
- Node health and availability.
- Timeouts and bounded retries.
- At least one meaningful resilience mechanism, such as failover, a circuit breaker, or queue-based recovery.
- Reproducible runs using a configurable random seed.
- Graceful shutdown and clear startup instructions.

Implement replication or sharding only where appropriate. If using replication, demonstrate replication delay, read/write behavior, and replica recovery. If using sharding, demonstrate the partition key, shard routing, and how requests reach the correct shard.

For CAP theorem:

- Identify the consistency and availability tradeoff for the relevant data operations during a partition.
- Explain the specific consistency guarantee being simulated.
- Implement a real partition scenario that affects communication between selected nodes.
- Show the observable result, such as rejected operations, stale reads, or divergent replicas.
- Demonstrate recovery after the partition heals.
- Do not describe CAP as simply “pick any two,” and do not claim strong consistency unless the implementation supports that claim.

## Animated interface

Although a UI is optional in the assignment, I want an animated demonstration interface that makes the architecture easy to understand in our video.

Choose a lightweight approach that fits the existing project. Prefer a Python-based interface if practical. Avoid introducing an unnecessary frontend framework.

Include:

- A clearly labeled network topology diagram.
- Animated messages or particles moving along the actual communication paths.
- Distinct visual states for healthy, busy, failed, isolated, and recovering nodes.
- A legend explaining colors and symbols.
- A live event log.
- A request inspector or trace view showing one request’s path through the system.
- A metrics panel with useful measures such as completed requests, failed requests, latency, queue depth, and retry count.
- Clear feedback when a scenario begins, changes state, and ends.
- Start, pause, resume, reset, and simulation-speed controls where practical.
- Controls or presets for node failure, network partition, and recovery.

Use a single simulation state and event stream to drive the interface, logs, metrics, and animation. Do not create decorative animations that claim work happened when no corresponding operation occurred.

Pausing and changing simulation speed must remain consistent with simulation time. Resetting must clear pending work, events, metrics, and fault states predictably.

Keep the interface readable while recording a presentation. Use clear labels, readable text, and restrained animations.

## Event logging and observability

Every meaningful process must emit a structured event, including:

- Request received.
- Routing decision.
- Message sent and received.
- Processing started and completed.
- Database read or write.
- Replication or shard routing.
- Queue enqueue and dequeue.
- Timeout.
- Retry.
- Failure detection.
- Failover or other resilience action.
- Recovery.
- Final request success or failure.

Use fields such as:

- Timestamp or simulation time.
- Event ID.
- Trace ID.
- Span ID and parent span ID where appropriate.
- Request ID.
- Source and destination.
- Service or node.
- Operation.
- Event type.
- Status.
- Duration where applicable.
- A readable message.

Do not manufacture duration values or emit events for operations that did not execute.

Provide:

- Human-readable live logs.
- Exportable structured logs, preferably JSON Lines.
- Trace filtering by request or trace ID.
- Metrics derived from actual simulation events.
- A way to inspect at least one complete end-to-end request.

Ensure the animation, event logs, trace view, and metrics agree with one another.

## Required demonstration scenarios

Implement repeatable scenario presets suitable for a recorded demonstration:

1. **Normal operation:** A thesis-related request flows successfully through the architecture.
2. **Concurrent workload:** Multiple requests demonstrate routing and parallel processing.
3. **Node failure:** A node fails while requests are running, revealing the implemented resilience behavior.
4. **Network partition:** The system visibly demonstrates its CAP tradeoff.
5. **Recovery:** Communication or node health is restored, and the system demonstrates its recovery behavior.
6. **Replication or sharding:** Demonstrate the selected data distribution strategy, if applicable.

For each scenario, explain:

- What action starts it.
- What should appear in the animation.
- Which logs and metrics provide evidence.
- What distributed systems concept it demonstrates.
- What limitations remain.

Prefer a small number of convincing, fully working scenarios over many shallow features.

## Implementation workflow

Work in manageable stages:

1. Inspect the thesis and existing code.
2. Propose the architecture and explain key decisions.
3. Implement or repair the simulation engine.
4. Add structured events, tracing, and metrics.
5. Build the animated interface using those real events.
6. Add scenario presets and fault controls.
7. Run appropriate checks and repair failures.
8. Prepare documentation and presentation materials.

Complete the actual implementation if you can edit the workspace. If you cannot edit files, provide complete files with their exact paths and commands, in manageable batches.

Do not use placeholder functions, unfinished core features, or pseudocode as the final implementation. Do not claim that something was tested unless you actually ran it. Report any environment limitations clearly.

Verify at minimum:

- A normal request completes.
- Concurrent requests retain distinct trace identities.
- Fault injection actually changes behavior.
- Retries are bounded.
- Partition behavior matches the stated CAP tradeoff.
- Recovery restores the expected operation.
- Reset removes old scenario state.
- Logs and metrics correspond to actual events.

## Deliverables

Produce:

1. Complete runnable source code.
2. A dependency file and clear setup instructions.
3. A README explaining the thesis context, architecture, assumptions, and simulation boundaries.
4. An architecture diagram, using Mermaid if practical.
5. An explanation of topology, CAP behavior, replication or sharding, cloud mapping, observability, and resilience.
6. Demonstration instructions for every scenario.
7. Sample logs generated from an actual run, if execution is available.
8. A rubric mapping that points to concrete implemented features and evidence.
9. A 15-minute presentation outline following:
   - Overview: 1 minute.
   - Architecture: 2 minutes.
   - Demonstration: 10 minutes.
   - Conclusion: 2 minutes.
10. A recording script with exact actions, expected visual results, and brief speaking notes.
11. Individual reflection questions and a customizable reflection outline. Do not invent personal experiences or claim contributions we have not described.
12. A concise list of known limitations and future improvements.

Keep the scope realistic for a student MVP. Prioritize correct behavior, clear evidence, reproducible demonstrations, and ease of running the project.

Start by reviewing the materials below.

### Thesis description

[Insert thesis title, problem, users, features, and current architecture.]

### Existing code

[Attach the project files or paste the existing code.]

### Additional preferences

[Optional: required libraries, operating system, existing UI, team constraints, or features that must be preserved.]
