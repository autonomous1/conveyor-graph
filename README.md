# conveyor-graph

`conveyor-graph` subsumes the common object-mode use cases of `through2` and `parallel-transform`, with **zero runtime dependencies**, while adding graph topology, schema-validated construction, routing, bounded per-edge queues, explicit overload policy, fan-in/fan-out, lifecycle controls, and real-time observability.

It moves individual items through named vertices over bounded edges. Backpressure, overflow (`block` / `error` / `drop`), per-item `when` routing, controlled fan-out, and live occupancy counters are part of the graph, not wrappers around Node streams.

It is not `stream.pipeline()`: use that for a simple linear stream. It is not Temporal: this package does not persist workflows, recover after process failure, or coordinate work across hosts. Use it when you need an inspectable routing graph inside one Node process.

## Related packages

This runtime has **no runtime dependencies**. Construction, demos, and network fixtures live in siblings so those heavier deps stay off the published graph.

| Package | Role |
|---|---|
| `conveyor-graph` | In-process runtime: vertices, bounded edges, seal / drain / stop, live counters. |
| `conveyor-graph-model` | JSON Schema, `VertexRegistry`, `loadGraph`, builtins (`identity`, `log.print`, `sql.query`, `chunk.frame` / `order` / `collect`). |
| `conveyor-graph-simulator` | Virtual-time harness and S-series scenarios (backpressure, overflow, routing, chunking). |
| `conveyor-graph-ssh-simulator` | T-series on real loopback SSH via `ssh_tunnel_proxy` (optional; skip when SSH deps are absent). |

Declarative documents and typed vertices: `loadGraph` in `conveyor-graph-model`. Proof-by-experiment: the two simulator packages.

## Model

`proto/agentgraph/v1/graph.proto` is the cross-language source of truth. `src/model.ts` is a hand-written TypeScript projection. The runtime does **not** depend on protobuf. A drift test fails if the two disagree on message names or builtin ids.

## Execution contract

`CONTRACT.md` is the normative v1 spec. The section below is the short public guarantee.

## Scope and guarantees

This is an **in-process, memory-resident runtime**. Agents live in the Node process that constructed the graph. It is not a message broker, not a distributed workflow engine, and not a job queue.

There is **no persistence and no recovery**. If the process dies, in-flight agents are gone. Handler I/O that already ran is not rolled back. Delivery is at-most-once.

**Backpressure and overflow are per-edge.** `capacity` is a strict queued-item limit on that edge. A required edge must `block` or `error`; it cannot silently drop. A best-effort edge may drop only under its `overflow` policy, and drops are counted.

| Call | Guarantee |
|---|---|
| `write()` | Admit into the named vertex now. Returns Node-style pressure (`false` = wait for drain before writing again). Does not mean the handler finished. |
| `send()` | Wait until that ingress admission succeeds (or abort/timeout). Does **not** wait for downstream vertices. |
| `drain()` | Graph is idle: no handlers, no vertex/edge buffers, no admission waiters. |
| `stop()` | Refuse new input, then drain, then destroy streams. |
| `stop({ force: true })` | Abort cooperative handlers and pending `send()` waits, then destroy streams. |

**Ordering.** A serial vertex starts handlers in arrival order. `parallel > 1` does not preserve order. There is no global or partition-key order.

**Fan-out.** `fanout: "all"` (default) gives each matching edge its own `StreamAgent` envelope (id, settled, signal, timeoutMs). `fanout: "first"` uses the first matching edge in `connect` order.

The payload value is **shared by reference**. The runtime does not clone it. If branch A assigns into a field of `agent.payload`, branch B sees that change. Copy the payload in the handler when branches must not share mutations.

**Cancellation and timeout.** `send({ signal })`, `seal({ signal })`, vertex `timeoutMs`, and forced stop abort `agent.signal`. A timeout is an error outcome. Cooperative handlers must watch the signal; a bare `await` without a signal is not killed.

**Metrics.** `depth` / `queued` is current occupancy. `highWaterMark` is the peak since last reset. `blockedMs` is wall-clock time an edge sat at capacity. `itemWaitMs` is time items spent waiting for an edge slot. `dropCount`, `filterCount`, `timedOut`, and `inFlight` are measured counts, not inferred from Sankey diffs.

## Usage

```ts
import { ConveyorGraph, GraphAgent } from "conveyor-graph";

const graph = new ConveyorGraph("gateway")
  .define("decode", { parallel: 4 }, decode)
  .define("classify", classify)
  .define("store", { parallel: 8, timeoutMs: 5_000 }, store)
  .define("alerts", { parallel: 2 }, notify)
  .connect("decode", "classify", { capacity: 128, delivery: "required", overflow: "block" })
  .connect("classify", "store", {
    when: (agent) => agent.payload.kind === "record",
    capacity: 1_000,
    delivery: "required",
    overflow: "block",
  })
  .connect("classify", "alerts", {
    when: (agent) => agent.payload.severity === "critical",
    capacity: 50,
    delivery: "bestEffort",
    overflow: "drop",
  })
  .seal();

const agent = new GraphAgent("session", {}, {}, graph);
await agent.send("decode", "event-42", payload, { signal, timeoutMs: 2_000 });
```

`send` admits the item into `decode`. It does not wait for `store` or `alerts`. Use `graph.drain()` when you need the graph idle.

`forwardFrom(id)` pushes through that vertex’s outgoing edges. `routeTo(id)` enqueues into that vertex’s handler. `link(...ids)` is only a default-edge shortcut.

## Design choices

- One package, subpath exports: `.`, `./utils`, `./subscriber`
- `ConveyorGraph` is constructed, never stored on `global`
- No agent object pool
- No LevelDB; `Subscriber` / `SubscriberRegistry` are in-memory
- MQTT is an injected `MessageBus`
- Domain counters (e.g. Woo product totals) belong in `MonitorHooks.extraFrameFields`

## Build

Plain `tsc` — ESM in `dist/`, CJS in `dist/cjs/`. No tsup.

```
rm -rf node_modules package-lock.json
npm install
npx tsc -p tsconfig.json
npm run build
npm test
```

If `npm run build` cannot find `tsc`, use `npx tsc -p tsconfig.json && npx tsc -p tsconfig.cjs.json`.

## Example: WordPress posts → CSV

`examples/wp-posts-csv.ts` is a four-vertex graph:

`connect` → `query` → `csv` → `print` → `graph/log`

- `connect` opens MariaDB at `127.0.0.1:13306` / `wordpress_db`
- `query` runs `SELECT * FROM wp_posts LIMIT 20` on that connection
- `csv` serializes the rows
- `print` writes the CSV to stdout

```
npm install
MYSQL_HOST=127.0.0.1 MYSQL_PORT=13306 MYSQL_USER=root MYSQL_PASSWORD= \
  MYSQL_DATABASE=wordpress_db npm run example:wp-posts
```
