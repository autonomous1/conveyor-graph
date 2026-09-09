import { EdgeStream } from "./EdgeStream.js";
import { StreamAgent } from "./StreamAgent.js";
import { BUILTIN, DEFAULT_TOPICS } from "./model.js";
import type { LayoutFrame, LinkStats, MessageBus, NodeStats, SessionContext } from "./model.js";
import type { ConveyorGraph } from "./ConveyorGraph.js";
import { toPayloadString } from "./utils.js";
import { composeSignals } from "./abort.js";

export interface MonitorHooks {
  extraFrameFields?: (nodes: Record<string, NodeStats>) => Record<string, unknown>;
  layoutTopic?: string;
  intervalMs?: number;
}

export class GraphAgent {
  readonly id: string;
  readonly auth: unknown;
  readonly context: SessionContext;
  readonly streamGraph: ConveyorGraph;
  private _graph: { nodes: Record<string, NodeStats>; links: Record<string, LinkStats> };
  private _mqClient?: MessageBus;
  private _cmd?: unknown;
  private _totalPacketsExpected?: number;
  private _resolveCB?: (() => void) | null;
  private _interval?: ReturnType<typeof setInterval>;
  private _frameId = 0;
  private readonly hooks: MonitorHooks;

  constructor(
    id: string,
    auth: unknown,
    context: SessionContext,
    streamGraph: ConveyorGraph,
    hooks: MonitorHooks = {},
  ) {
    this.id = id;
    this.auth = auth;
    this.context = context;
    this.streamGraph = streamGraph;
    this._graph = streamGraph.getGraph();
    this.hooks = hooks;
  }

  get node(): Record<string, NodeStats> {
    return this._graph.nodes;
  }
  get link(): Record<string, LinkStats> {
    return this._graph.links;
  }

  set cmd(cmd: unknown) {
    this._cmd = cmd;
  }
  get cmd(): unknown {
    return this._cmd;
  }

  set graph(graph: { nodes: Record<string, NodeStats>; links: Record<string, LinkStats> }) {
    this._graph = graph;
  }

  set mqClient(client: MessageBus | undefined) {
    this._mqClient = client;
  }
  get mqClient(): MessageBus | undefined {
    return this._mqClient;
  }

  set totalPacketsExpected(total: number) {
    this._totalPacketsExpected = total;
  }
  set resolveCB(resolve: () => void) {
    this._resolveCB = resolve;
  }

  write(
    node: string | { write: (agent: StreamAgent) => boolean },
    id: string,
    payload: unknown,
    streamAgent?: StreamAgent,
    opts: { internal?: boolean } = {},
  ): boolean {
    const nodeId = typeof node === "string" ? node : undefined;
    const terminal = nodeId === BUILTIN.error || nodeId === BUILTIN.skip || nodeId === BUILTIN.log;
    if (!opts.internal && !terminal) this.streamGraph.assertAccepting("write");
    if (this.streamGraph.status === "stopped") return false;
    const writeNode = typeof node === "string" ? this.streamGraph.vertex[node] : node;
    if (!writeNode) throw new Error(`write: unknown vertex ${String(node)}`);
    const agent = streamAgent ?? new StreamAgent(id, payload, this);
    agent.id = id;
    agent.payload = payload;
    agent.signal = composeSignals(agent.signal, this.streamGraph.abortSignal);
    return writeNode.write(agent);
  }

  async send(
    node: string,
    id: string,
    payload: unknown,
    streamAgentOrOpts?: StreamAgent | import("./model.js").SendOptions,
    maybeOpts?: import("./model.js").SendOptions,
  ): Promise<void> {
    this.streamGraph.assertAccepting("send");
    const vertex = this.streamGraph.vertex[node];
    if (!vertex) throw new Error(`send: unknown vertex ${node}`);
    const isAgent = streamAgentOrOpts instanceof StreamAgent;
    const agent = isAgent ? streamAgentOrOpts : new StreamAgent(id, payload, this);
    const opts = isAgent ? maybeOpts : streamAgentOrOpts;
    agent.signal = composeSignals(opts?.signal, agent.signal, this.streamGraph.abortSignal);
    if (agent.signal?.aborted) throw new Error("send aborted");
    const accepted = this.write(vertex, id, payload, agent);
    if (accepted) return;
    await new Promise<void>((resolve, reject) => {
      const finish = (err?: Error) => {
        this.streamGraph.admissionWaiters.delete(onAbort);
        clearTimeout(timer);
        opts?.signal?.removeEventListener("abort", onAbort);
        if (err) reject(err);
        else resolve();
      };
      const onAbort = () => finish(new Error("send aborted"));
      this.streamGraph.admissionWaiters.add(onAbort);
      const timer = opts?.timeoutMs
        ? setTimeout(() => finish(new Error(`send timed out after ${opts.timeoutMs}ms`)), opts.timeoutMs)
        : undefined;
      opts?.signal?.addEventListener("abort", onAbort);
      void vertex.onceDrain().then(() => finish());
    });
  }

  publish(nodeId: string, payload: unknown): void {
    const topic =
      nodeId === BUILTIN.error
        ? (this.context.errorTopic ?? DEFAULT_TOPICS.error)
        : (this.context.logTopic ?? DEFAULT_TOPICS.log);
    this._mqClient?.publish(topic, toPayloadString(payload));
  }

  logError(id: string, err: Error, data: unknown, streamAgent?: StreamAgent): void {
    const message = { id, error: err.message, data };
    this.write(BUILTIN.error, id, message, streamAgent, { internal: true });
    this.newLink(id, BUILTIN.error);
    const node = this.node[id];
    if (node) node.errorCount++;
  }

  newLink(...ids: string[]): void {
    for (let i = 1; i < ids.length; i++) {
      const sourceId = ids[i - 1]!;
      const sinkId = ids[i]!;
      const id = EdgeStream.id(sourceId, sinkId);
      if (!this.link[id]) {
        this.link[id] = EdgeStream.newSpec(sourceId, sinkId);
      }
    }
  }

  clearStats(): void {
    this._frameId = 0;
    for (const key of Object.keys(this.node)) {
      const live = this.streamGraph.vertex[key];
      if (live) this.node[key] = live.spec;
    }
  }

  startStreamMonitor(): void {
    this.clearStats();
    const ms = this.hooks.intervalMs ?? 500;
    this._interval = setInterval(() => this.monitorFrame(), ms);
  }

  stopStreamMonitor(): void {
    if (this._interval) {
      clearInterval(this._interval);
      this._interval = undefined;
    }
  }

  publishStats(statsId: string, logTopic: string): void {
    const node = this.node[statsId];
    if (!node) return;
    (node as NodeStats & { logTopic?: string }).logTopic = logTopic;
    this.write(BUILTIN.log, statsId, node);
  }

  waitUntilProcessed(total: number): Promise<void> {
    this._totalPacketsExpected = total;
    return new Promise((resolve) => {
      this._resolveCB = resolve;
    });
  }

  private processNodeStats(
    node: NodeStats,
    totals: { source: number; sink: number; process: number; maxDelta: number },
  ): NodeStats | null {
    if (!node.objectCount && !node.ignoreCount && !node.errorCount) return null;
    const nodeDef: NodeStats = { ...node, title: `${node.id} (${node.objectCount}) ` };
    if (node.publish) {
      if (node.sourceCount === 0) totals.source += nodeDef.objectCount;
      if (node.sinkCount === 0) totals.sink += nodeDef.objectCount;
    } else {
      totals.process += nodeDef.objectCount;
    }
    return nodeDef;
  }

  monitorFrame(): void {
    if (this._totalPacketsExpected && this._resolveCB) {
      const log = this.node[BUILTIN.log]?.objectCount ?? 0;
      const skip = this.node[BUILTIN.skip]?.objectCount ?? 0;
      const err = this.node[BUILTIN.error]?.objectCount ?? 0;
      if (log + skip + err >= this._totalPacketsExpected) {
        this._resolveCB();
        this._resolveCB = null;
      }
    }

    if (!this._mqClient) return;

    const extra = this.hooks.extraFrameFields?.(this.node) ?? {};
    const frame: LayoutFrame = {
      sequence: ++this._frameId,
      timestamp: new Date().toISOString(),
      nodes: [],
      links: [],
      extra,
    };

    const active: Record<string, true> = {};
    const totals = { source: 0, sink: 0, process: 0, maxDelta: 0 };
    for (const key of Object.keys(this.node)) {
      const live = this.streamGraph.vertex[key];
      const snap = this.node[key]!;
      if (live) {
        snap.inFlight = live.inFlight;
        snap.accepted = live.accepted;
        snap.highwaterMark = live.bufferDepth();
      }
      const def = this.processNodeStats(snap, totals);
      if (def || (live && (live.inFlight > 0 || live.bufferDepth() > 0))) {
        frame.nodes.push(def ?? { ...snap, title: `${snap.id} (${snap.objectCount}) ` });
        active[key] = true;
      }
    }

    for (const linkKey of Object.keys(this.streamGraph.edge)) {
      const live = this.streamGraph.edge[linkKey]!;
      live.refreshSpec();
      this.link[linkKey] = live.spec;
      if (active[live.sourceId] && active[live.targetId]) {
        frame.links.push(live.spec);
      }
    }

    if (frame.nodes.length > 1 && frame.links.length) {
      const topic = this.hooks.layoutTopic ?? DEFAULT_TOPICS.layout;
      this._mqClient.publish(topic, JSON.stringify(frame));
    }
  }
}
