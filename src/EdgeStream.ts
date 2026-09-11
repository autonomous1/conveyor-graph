import { Transform } from "node:stream";
import type { Delivery, EdgeOptions, LinkStats, Overflow } from "./model.js";
import type { StreamAgent } from "./StreamAgent.js";
import type { VertexStream } from "./VertexStream.js";

export type EdgeWhen = (agent: StreamAgent) => boolean;
export type OverflowHook = (info: { edgeId: string; reason: string; agent: StreamAgent }) => void;
export type AdmitResult = "ok" | "full" | "dropped" | "errored";

const inflightEdges = new WeakMap<StreamAgent, EdgeStream[]>();

export interface EdgeConfig extends EdgeOptions {
  when?: EdgeWhen;
  onOverflow?: OverflowHook;
}

export class EdgeStream {
  readonly sourceId: string;
  readonly targetId: string;
  readonly id: string;
  readonly capacity: number;
  readonly delivery: Delivery;
  readonly overflow: Overflow;
  readonly when: EdgeWhen;
  readonly onOverflow?: OverflowHook;
  objectCount = 0;
  dropCount = 0;
  filterCount = 0;
  errorCount = 0;
  blockedMs = 0;
  itemWaitMs = 0;
  queued = 0;
  peakQueued = 0;
  lastDropReason: string | null = null;
  lastError: string | null = null;
  private dest: Transform | null = null;
  private pressureStarted: number | null = null;
  private readonly spaceWaiters: Array<() => void> = [];
  spec: LinkStats;

  constructor(sourceId: string, targetId: string, id?: string, config: EdgeConfig = {}) {
    this.sourceId = sourceId;
    this.targetId = targetId;
    this.id = id ?? EdgeStream.id(sourceId, targetId);
    this.capacity = config.capacity ?? 16;
    this.delivery = config.delivery ?? "required";
    this.overflow = config.overflow ?? (this.delivery === "bestEffort" ? "drop" : "block");
    this.when = config.when ?? (() => true);
    this.onOverflow = config.onOverflow;
    this.spec = this.snapshot();
  }

  /**
   * Edge map key. Hyphen-free vertex ids keep the historical `source-sink` form
   * so existing lookups continue to work. If either id contains `-`, the pair is
   * JSON-encoded so `"a-b"→"c"` cannot collide with `"a"→"b-c"`.
   */
  static id(sourceId: string, sinkId: string): string {
    if (sourceId.includes("-") || sinkId.includes("-")) {
      return JSON.stringify([sourceId, sinkId]);
    }
    return `${sourceId}-${sinkId}`;
  }

  static newSpec(sourceId: string, sinkId: string): LinkStats {
    return {
      id: EdgeStream.id(sourceId, sinkId),
      source: sourceId,
      target: sinkId,
      value: 0,
      objectCount: 0,
      errorCount: 0,
      highwaterMark: 0,
      timestamp: null,
      dropCount: 0,
      filterCount: 0,
      capacity: 16,
      depth: 0,
    };
  }

  get blocks(): boolean {
    return this.overflow === "block";
  }

  get depth(): number {
    return this.queued;
  }

  matches(agent: StreamAgent): boolean {
    return this.when(agent);
  }

  noteFiltered(): void {
    this.filterCount++;
    this.refreshSpec();
  }

  attach(sink: VertexStream): void {
    this.dest = sink.stream;
    this.dest.on("error", (err) => this.noteError("dest", err));
  }

  private noteError(where: string, err: unknown): void {
    const message = err instanceof Error ? err.message : String(err);
    this.errorCount++;
    this.lastError = `${where}: ${message}`;
    this.lastDropReason = this.lastError;
    this.refreshSpec();
    console.error(`[conveyor-graph edge ${this.id}] ${this.lastError}`);
  }

  admit(agent: StreamAgent): AdmitResult {
    if (this.queued >= this.capacity) {
      if (this.overflow === "block") {
        this.markPressure();
        return "full";
      }
      if (this.overflow === "drop") {
        this.dropCount++;
        this.lastDropReason = "overflow";
        this.onOverflow?.({ edgeId: this.id, reason: "overflow", agent });
        this.refreshSpec();
        return "dropped";
      }
      this.errorCount++;
      this.lastDropReason = "overflow";
      this.onOverflow?.({ edgeId: this.id, reason: "overflow", agent });
      this.refreshSpec();
      agent.error(this.sourceId, new Error(`edge ${this.id} overflow`));
      return "errored";
    }
    if (!this.dest || this.dest.destroyed || this.dest.writableEnded) {
      this.noteError(
        "admit",
        new Error(
          `edge ${this.id} dest ${!this.dest ? "missing" : this.dest.destroyed ? "destroyed" : "ended"}`,
        ),
      );
      return "errored";
    }
    this.queued++;
    this.peakQueued = Math.max(this.peakQueued, this.queued);
    this.objectCount++;
    const held = inflightEdges.get(agent) ?? [];
    held.push(this);
    inflightEdges.set(agent, held);
    try {
      this.dest.write(agent);
    } catch (err) {
      this.noteError("admit", err);
      this.release();
      const held = inflightEdges.get(agent);
      if (held) {
        const i = held.lastIndexOf(this);
        if (i >= 0) held.splice(i, 1);
        if (held.length === 0) inflightEdges.delete(agent);
      }
      return "errored";
    }
    if (this.queued >= this.capacity) this.markPressure();
    this.refreshSpec();
    return "ok";
  }

  release(): void {
    this.queued = Math.max(0, this.queued - 1);
    this.emitSpace();
    this.refreshSpec();
  }

  static releaseAgent(agent: StreamAgent, vertexId?: string): void {
    const held = inflightEdges.get(agent);
    if (!held || held.length === 0) return;
    const i = vertexId
      ? held.findIndex((edge) => edge.targetId === vertexId)
      : held.length - 1;
    if (i < 0) return;
    const [edge] = held.splice(i, 1);
    edge?.release();
    if (held.length === 0) inflightEdges.delete(agent);
  }

  waitForSpace(signal?: AbortSignal): Promise<void> {
    if (this.queued < this.capacity) return Promise.resolve();
    const started = Date.now();
    this.markPressure();
    return new Promise((resolve, reject) => {
      const done = (err?: Error) => {
        this.itemWaitMs += Date.now() - started;
        signal?.removeEventListener("abort", onAbort);
        const i = this.spaceWaiters.indexOf(onSpace);
        if (i >= 0) this.spaceWaiters.splice(i, 1);
        if (err) reject(err);
        else resolve();
      };
      const onSpace = () => done();
      const onAbort = () => done(new Error("aborted"));
      this.spaceWaiters.push(onSpace);
      if (signal?.aborted) {
        onAbort();
        return;
      }
      signal?.addEventListener("abort", onAbort);
    });
  }

  private markPressure(): void {
    if (this.pressureStarted === null) this.pressureStarted = Date.now();
  }

  private emitSpace(): void {
    if (this.queued < this.capacity && this.pressureStarted !== null) {
      this.blockedMs += Date.now() - this.pressureStarted;
      this.pressureStarted = null;
    }
    if (this.queued < this.capacity) {
      const waiters = this.spaceWaiters.splice(0);
      for (const w of waiters) w();
    }
    this.refreshSpec();
  }

  snapshot(ts?: string): LinkStats {
    return {
      id: this.id,
      source: this.sourceId,
      target: this.targetId,
      value: Math.max(this.queued, 1),
      objectCount: this.objectCount,
      errorCount: this.errorCount,
      highwaterMark: this.peakQueued,
      timestamp: ts ?? new Date().toISOString(),
      dropCount: this.dropCount,
      filterCount: this.filterCount,
      capacity: this.capacity,
      depth: this.queued,
      blockedMs: this.blockedMs,
      lastDropReason: this.lastDropReason ?? undefined,
      itemWaitMs: this.itemWaitMs,
    };
  }

  refreshSpec(ts?: string): void {
    this.spec = this.snapshot(ts);
  }

  resetPeaks(): void {
    this.peakQueued = this.queued;
    this.refreshSpec();
  }
}
