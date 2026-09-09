import { Transform } from "node:stream";
import type { Delivery, EdgeOptions, LinkStats, Overflow } from "./model.js";
import type { StreamAgent } from "./StreamAgent.js";
import type { VertexStream } from "./VertexStream.js";

export type EdgeWhen = (agent: StreamAgent) => boolean;
export type OverflowHook = (info: { edgeId: string; reason: string; agent: StreamAgent }) => void;
export type AdmitResult = "ok" | "full" | "dropped" | "errored";

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
  readonly stream: Transform;
  objectCount = 0;
  dropCount = 0;
  filterCount = 0;
  errorCount = 0;
  blockedMs = 0;
  itemWaitMs = 0;
  queued = 0;
  peakQueued = 0;
  lastDropReason: string | null = null;
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
    this.stream = new Transform({
      objectMode: true,
      highWaterMark: 1,
      transform: (chunk, _enc, cb) => {
        this.queued = Math.max(0, this.queued - 1);
        this.refreshSpec();
        this.emitSpace();
        cb(null, chunk);
      },
    });
    this.spec = this.snapshot();
  }

  static id(sourceId: string, sinkId: string): string {
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
    this.stream.pipe(sink.stream);
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
    this.stream.write(agent);
    this.queued++;
    this.peakQueued = Math.max(this.peakQueued, this.queued);
    this.objectCount++;
    if (this.queued >= this.capacity) this.markPressure();
    this.refreshSpec();
    return "ok";
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

  snapshot(): LinkStats {
    return {
      id: this.id,
      source: this.sourceId,
      target: this.targetId,
      value: Math.max(this.queued, 1),
      objectCount: this.objectCount,
      errorCount: this.errorCount,
      highwaterMark: this.peakQueued,
      timestamp: new Date().toISOString(),
      dropCount: this.dropCount,
      filterCount: this.filterCount,
      capacity: this.capacity,
      depth: this.queued,
      blockedMs: this.blockedMs,
      lastDropReason: this.lastDropReason ?? undefined,
      itemWaitMs: this.itemWaitMs,
    };
  }

  refreshSpec(): void {
    this.spec = this.snapshot();
  }

  resetPeaks(): void {
    this.peakQueued = this.queued;
    this.refreshSpec();
  }
}
