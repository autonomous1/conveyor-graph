import { VertexStream, type ClassicTransform, type VertexHandler } from "./VertexStream.js";
import { EdgeStream, type EdgeConfig } from "./EdgeStream.js";
import { BUILTIN } from "./model.js";
import type { GraphDef, GraphPhase, GraphValidation, LinkStats, NodeStats, SealOptions, StopOptions, ValidationIssue, VertexOptions } from "./model.js";
import { StreamAgent, type TransformCallback } from "./StreamAgent.js";
import { composeSignals } from "./abort.js";

export class ConveyorGraph {
  readonly id: string;
  readonly vertex: Record<string, VertexStream> = {};
  readonly edge: Record<string, EdgeStream> = {};
  private initialized = false;
  private phase: GraphPhase = "building";
  private signal?: AbortSignal;
  private readonly cancel = new AbortController();
  readonly admissionWaiters = new Set<() => void>();
  pendingEdgeWaits = 0;
  private readonly idleWaiters = new Set<() => void>();
  private readonly outgoing = new Map<string, EdgeStream[]>();

  constructor(id = "default") {
    this.id = id;
  }

  get node(): Record<string, VertexStream> {
    return this.vertex;
  }
  get links(): Record<string, EdgeStream> {
    return this.edge;
  }
  get isSealed(): boolean {
    return this.phase !== "building";
  }
  get status(): GraphPhase {
    return this.phase;
  }
  get abortSignal(): AbortSignal {
    return composeSignals(this.signal, this.cancel.signal);
  }

  get GRAPH_LOG(): string {
    return BUILTIN.log;
  }
  get GRAPH_SKIP(): string {
    return BUILTIN.skip;
  }
  get GRAPH_ERROR(): string {
    return BUILTIN.error;
  }

  get(id: string): VertexStream | undefined {
    return this.vertex[id];
  }

  define(id: string, opts?: VertexOptions | ClassicTransform | VertexHandler, fn?: ClassicTransform | VertexHandler): this {
    this.assertMutable("define");
    const transform = typeof opts === "function" ? opts : fn;
    const options: VertexOptions = typeof opts === "function" || !opts ? {} : opts;

    let vertex = this.vertex[id];
    if (!vertex) {
      vertex = new VertexStream(id, options);
      this.vertex[id] = vertex;
    } else if (Object.keys(options).length) {
      Object.assign(vertex.options, options);
      if (options.publish !== undefined) vertex.publish = options.publish;
      if (options.external !== undefined) vertex.external = options.external;
    }

    if (transform) {
      if (transform.length >= 3) {
        vertex.Transform = transform as ClassicTransform;
      } else {
        vertex.handler = transform as VertexHandler;
      }
    }
    return this;
  }

  connect(sourceId: string, sinkId: string, config: EdgeConfig = {}): this {
    this.assertMutable("connect");
    const id = EdgeStream.id(sourceId, sinkId);
    if (this.edge[id]) return this;
    this.define(sourceId);
    this.define(sinkId);
    const source = this.vertex[sourceId]!;
    const sink = this.vertex[sinkId]!;
    const edge = new EdgeStream(sourceId, sinkId, id, config);
    edge.attach(sink);
    this.edge[id] = edge;
    const list = this.outgoing.get(sourceId) ?? [];
    list.push(edge);
    this.outgoing.set(sourceId, list);
    source.sourceCount++;
    sink.sinkCount++;
    return this;
  }

  /** Convenience: default `connect` along a path. Prefer `connect` when an edge has policy. */
  link(...ids: string[]): this {
    this.assertMutable("link");
    for (let i = 1; i < ids.length; i++) {
      this.connect(ids[i - 1]!, ids[i]!);
    }
    return this;
  }

  edgesFrom(vertexId: string): EdgeStream[] {
    return this.outgoing.get(vertexId) ?? [];
  }

  deliver(nodeId: string, agent: StreamAgent, payload: unknown, cb: TransformCallback): void {
    agent.payload = payload;
    const vertex = this.vertex[nodeId];
    const edges = this.edgesFrom(nodeId);
    const fanout = vertex?.options.fanout ?? "all";
    const chosen: EdgeStream[] = [];
    for (const edge of edges) {
      let matched = false;
      try {
        matched = edge.matches(agent);
      } catch (err) {
        agent.error(nodeId, err instanceof Error ? err : new Error(String(err)), cb);
        return;
      }
      if (!matched) {
        edge.noteFiltered();
        continue;
      }
      chosen.push(edge);
      if (fanout === "first") break;
    }

    if (chosen.length === 0) {
      cb(null, null);
      this.notifyIdle();
      return;
    }

    const admitOne = async (edge: EdgeStream, packet: StreamAgent): Promise<void> => {
      for (;;) {
        if (agent.signal?.aborted) throw new Error("aborted");
        const result = edge.admit(packet);
        if (result !== "full") return;
        this.pendingEdgeWaits++;
        try {
          await edge.waitForSpace(agent.signal);
        } finally {
          this.pendingEdgeWaits = Math.max(0, this.pendingEdgeWaits - 1);
        }
      }
    };

    Promise.all(
      chosen.map((edge) => {
        const packet =
          chosen.length > 1
            ? Object.assign(new StreamAgent(agent.id, payload, agent.graphAgent), {
                signal: agent.signal,
                timeoutMs: agent.timeoutMs,
              })
            : agent;
        return admitOne(edge, packet);
      }),
    )
      .then(() => {
        cb(null, null);
        this.notifyIdle();
      })
      .catch((err: Error) => {
        agent.error(nodeId, err, cb);
        this.notifyIdle();
      });
  }

  initGraph(): this {
    if (this.initialized) return this;
    this.assertMutable("initGraph");

    this.define(BUILTIN.log, { publish: true }, (agent, _enc, cb) => {
      agent.forwardFrom(BUILTIN.log, agent.payload, cb);
    });

    this.define(BUILTIN.skip, { publish: false }, (agent, _enc, cb) => {
      agent.forwardFrom(BUILTIN.skip, agent.payload, cb);
    });

    this.define(BUILTIN.error, { publish: true }, (agent, _enc, cb) => {
      agent.forwardFrom(BUILTIN.error, agent.payload, cb);
    });

    this.initialized = true;
    return this;
  }

  validate(): GraphValidation {
    const issues: ValidationIssue[] = [];
    for (const [edgeId, edge] of Object.entries(this.edge)) {
      if (!this.vertex[edge.sourceId]) {
        issues.push({ code: "undefined_vertex", message: `edge ${edgeId} source missing`, edgeId, vertexId: edge.sourceId });
      }
      if (!this.vertex[edge.targetId]) {
        issues.push({ code: "undefined_vertex", message: `edge ${edgeId} target missing`, edgeId, vertexId: edge.targetId });
      }
      if (edge.capacity < 1) {
        issues.push({ code: "invalid_policy", message: `edge ${edgeId} capacity must be >= 1`, edgeId });
      }
      if (edge.delivery !== "required" && edge.delivery !== "bestEffort") {
        issues.push({ code: "invalid_policy", message: `edge ${edgeId} invalid delivery`, edgeId });
      }
      if (!["block", "drop", "error"].includes(edge.overflow)) {
        issues.push({ code: "invalid_policy", message: `edge ${edgeId} invalid overflow`, edgeId });
      }
      if (edge.delivery === "required" && edge.overflow === "drop") {
        issues.push({
          code: "invalid_policy",
          message: `edge ${edgeId} required path cannot use overflow=drop`,
          edgeId,
        });
      }
    }
    for (const cycle of this.findCycles()) {
      issues.push({
        code: "cycle",
        message: `cycle forbidden: ${cycle.join(" -> ")}`,
        vertexId: cycle[0],
      });
    }
    return { ok: issues.length === 0, issues };
  }

  seal(options: SealOptions = {}): this {
    const result = this.validate();
    if (!result.ok) {
      const detail = result.issues.map((i) => i.message).join("; ");
      throw new Error(`ConveyorGraph.seal failed: ${detail}`);
    }
    this.phase = "sealed";
    this.signal = options.signal;
    if (this.signal?.aborted) this.phase = "stopped";
    this.signal?.addEventListener("abort", () => {
      void this.stop({ force: true });
    });
    return this;
  }

  start(options: SealOptions = {}): this {
    return this.seal(options);
  }

  totalInFlight(): number {
    let n = 0;
    for (const vertex of Object.values(this.vertex)) n += vertex.inFlight;
    return n;
  }

  occupancy(): number {
    let n = this.totalInFlight() + this.admissionWaiters.size + this.pendingEdgeWaits;
    for (const vertex of Object.values(this.vertex)) n += vertex.bufferDepth();
    for (const edge of Object.values(this.edge)) n += edge.queued;
    return n;
  }

  rejectAdmissionWaiters(): void {
    for (const reject of this.admissionWaiters) reject();
    this.admissionWaiters.clear();
  }

  notifyIdle(): void {
    if (this.occupancy() > 0) return;
    const waiters = [...this.idleWaiters];
    this.idleWaiters.clear();
    for (const w of waiters) w();
  }

  whenIdle(): Promise<void> {
    if (this.occupancy() === 0) return Promise.resolve();
    return new Promise((resolve) => this.idleWaiters.add(resolve));
  }

  assertAccepting(op: string): void {
    if (this.phase === "stopped" || this.phase === "draining") {
      throw new Error(`ConveyorGraph is ${this.phase}; ${op} is not allowed`);
    }
  }

  async drain(timeoutMs = 30_000): Promise<void> {
    if (this.phase === "building") this.seal();
    if (this.phase === "stopped") return;
    this.phase = "draining";
    this.notifyIdle();
    if (this.occupancy() === 0) return;
    await new Promise<void>((resolve, reject) => {
      const timer = setTimeout(() => {
        this.idleWaiters.delete(onIdle);
        reject(new Error(`ConveyorGraph.drain timed out after ${timeoutMs}ms`));
      }, timeoutMs);
      const onAbort = () => {
        clearTimeout(timer);
        this.idleWaiters.delete(onIdle);
        resolve();
      };
      const onIdle = () => {
        clearTimeout(timer);
        this.cancel.signal.removeEventListener("abort", onAbort);
        resolve();
      };
      this.idleWaiters.add(onIdle);
      this.cancel.signal.addEventListener("abort", onAbort);
    });
  }

  async stop(options: StopOptions = {}): Promise<void> {
    if (this.phase === "stopped") return;
    if (options.force) {
      this.cancel.abort();
      this.rejectAdmissionWaiters();
    } else if (this.phase !== "draining") {
      try {
        await this.drain();
      } catch {
        this.cancel.abort();
        this.rejectAdmissionWaiters();
      }
    }
    this.phase = "stopped";
    for (const vertex of Object.values(this.vertex)) {
      vertex.stream.destroy();
    }
    for (const edge of Object.values(this.edge)) {
      edge.stream.destroy();
    }
  }

  getGraph(): GraphDef & { nodes: Record<string, NodeStats>; links: Record<string, LinkStats> } {
    const nodes: Record<string, NodeStats> = {};
    const links: Record<string, LinkStats> = {};
    for (const key of Object.keys(this.vertex)) {
      nodes[key] = this.vertex[key]!.spec;
    }
    for (const key of Object.keys(this.edge)) {
      links[key] = this.edge[key]!.spec;
    }
    return { id: this.id, vertices: {}, edges: {}, nodes, links };
  }

  private assertMutable(op: string): void {
    if (this.phase !== "building") throw new Error(`ConveyorGraph is sealed; ${op} is not allowed`);
  }

  private findCycles(): string[][] {
    const adj = new Map<string, string[]>();
    for (const id of Object.keys(this.vertex)) adj.set(id, []);
    for (const edge of Object.values(this.edge)) {
      const list = adj.get(edge.sourceId) ?? [];
      list.push(edge.targetId);
      adj.set(edge.sourceId, list);
    }

    const cycles: string[][] = [];
    const visiting = new Set<string>();
    const visited = new Set<string>();
    const stack: string[] = [];

    const dfs = (node: string): void => {
      visiting.add(node);
      stack.push(node);
      for (const next of adj.get(node) ?? []) {
        if (visiting.has(next)) {
          const start = stack.indexOf(next);
          cycles.push([...stack.slice(start), next]);
          continue;
        }
        if (!visited.has(next)) dfs(next);
      }
      stack.pop();
      visiting.delete(node);
      visited.add(node);
    };

    for (const id of adj.keys()) {
      if (!visited.has(id)) dfs(id);
    }
    return cycles;
  }
}
