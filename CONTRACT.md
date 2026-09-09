# conveyor-graph v1 semantic contract

Normative. Implementation and tests must not reintroduce the ambiguities below.

## Execution scope

In-process, memory-resident runtime. No durable delivery, no crash recovery,
no replay. A process exit drops in-flight agents. Handler I/O is at-most-once.

## Topology

Graphs are mutable only in phase `building`. `seal()` / `start()` validate
then freeze topology. Later `define` / `connect` / `link` throw.

Phases: `building → sealed → draining → stopped`.

## Cycles

Forbidden. `validate()` / `seal()` return or throw an actionable path
(`a -> b -> a`), not only "cycle detected".

## Admission

`write(vertexId, id, payload): boolean` is Node-style pressure: the chunk is
accepted; `false` means do not write again until `'drain'`.

`send(vertexId, id, payload, { signal, timeoutMs })` waits until the item is
admitted into that ingress vertex buffer. It does not wait for handlers
or downstream vertices. `timeoutMs` bounds admission only.

## Capacity

Edge `capacity` is a strict total queued-item limit on that edge
(items admitted and not yet handed to the sink), not merely a Node
highWaterMark hint. Admit is refused once `queued >= capacity`.

## Required path

A matching `delivery: "required"` edge must not silently drop. It blocks
(`overflow: "block"`) or yields an explicit `error` outcome
(`overflow: "error"`). `required` + `overflow: "drop"` is invalid at `seal()`.

## Best-effort path

A matching `delivery: "bestEffort"` edge may drop only via its configured
overflow policy. Drops increment `dropCount` and set `lastDropReason`.

## Fan-out

Vertex `fanout: "all"` (default): every matching outgoing edge gets an
independent branch envelope.

`fanout: "first"`: only the first matching edge in `connect` order.

## Branch isolation

Each branch gets its own StreamAgent envelope (id, settled, signal,
timeoutMs). Payload is shared by reference and is not cloned. Treat
payload as read-only across branches, or copy it in the handler.

## Ordering

Not guaranteed globally. A serial vertex (parallel unset) preserves
per-vertex FIFO of handler start. `parallel > 1` does not. Per-edge
delivery is FIFO of successful admits. No partition-key ordering.

## Predicate failure

`when()` throwing is not "no match". It is an `error` outcome routed to
`graph/error`. False from `when()` is a filter (`filterCount++`).

## Outcomes

| Cause | Disposition | Counted on |
|---|---|---|
| return value / forward | forward | vertex objectCount; each admitted edge objectCount |
| skip | skip | vertex ignoreCount; item to graph/skip |
| throw, timeout, abort, error | error | vertex errorCount; item to graph/error |
| terminate or no outgoing edges | terminate | vertex objectCount; no edge admit |

## Timeout

A vertex/agent `timeoutMs` is an error outcome. The runtime aborts
`agent.signal` so a cooperative handler can stop.

## Cancellation

These abort `agent.signal` and reject pending `send()` waiters:

- caller AbortSignal on send / seal
- vertex timeout
- stop({ force: true })

## Completion (separate APIs)

| Meaning | API |
|---|---|
| Ingress admission | send() / write() |
| Builtin terminal counts | waitUntilProcessed(n) |
| Graph idle | drain() |

## Draining

drain() resolves only when vertex inFlight, vertex buffers, edge queued,
and pending send() waiters are all zero.

## Stop

Graceful stop(): refuse admission, drain(), destroy streams.
Forced stop({ force: true }): abort in-flight/pending work, reject
admission waiters, destroy streams.

## Observability

- depth / queued — current occupancy
- highWaterMark — peak occupancy since last stats reset
- blockedMs, dropCount, lastDropReason, inFlight, timedOut — measured
