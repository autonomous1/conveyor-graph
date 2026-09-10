import { Transform } from "node:stream";
import type { NodeStats, VertexOptions } from "./model.js";
import type { StreamAgent, TransformCallback } from "./StreamAgent.js";
import { normalizeOutcome } from "./outcome.js";
import { composeSignals } from "./abort.js";
import { EdgeStream } from "./EdgeStream.js";

export type ClassicTransform = (
  agent: StreamAgent,
  enc: BufferEncoding,
  cb: TransformCallback,
) => void;

/**
 * Preferred vertex handler: do I/O, then return a payload (forward),
 * throw (error), or call agent.skip / agent.forward yourself.
 */
export type VertexHandler = (agent: StreamAgent) => unknown | Promise<unknown>;

export class VertexStream {
  readonly id: string;
  readonly options: VertexOptions;
  private _stream: Transform;
  private _transform: ClassicTransform | null = null;
  private _handler: VertexHandler | null = null;
  private _updateDataStats?: (payload: unknown, data: Record<string, unknown>) => void;
  private _resetDataStats?: () => Record<string, unknown>;
  sourceCount = 0;
  sinkCount = 0;
  publish: boolean;
  external: boolean;
  inFlight = 0;
  accepted = 0;
  lastHandlerMs = 0;
  totalHandlerMs = 0;
  timedOut = 0;
  peakDepth = 0;

  constructor(id: string, opts: VertexOptions = {}, stream?: Transform) {
    this.id = id;
    this.options = opts;
    this.publish = Boolean(opts.publish);
    this.external = Boolean(opts.external);
    this._stream = stream ?? this.initStream(opts);
  }

  private initStream(opts: VertexOptions): Transform {
    const parallel = opts.parallel && opts.parallel > 0 ? opts.parallel : 1;
    let active = 0;
    const queued: Array<() => void> = [];
    return new Transform({
      objectMode: true,
      highWaterMark: Math.max(8, parallel * 2),
      transform: (chunk, enc, done) => {
        const agent = chunk as StreamAgent;
        const run = (release: () => void): void => {
          active++;
          this.dispatch(agent, enc, (err, out) => {
            active--;
            if (!err && out != null) this._stream.push(out);
            release();
            queued.shift()?.();
          });
        };
        if (parallel <= 1) {
          this.dispatch(agent, enc, (err, out) => {
            if (!err && out != null) done(null, out);
            else done();
          });
          return;
        }
        if (active < parallel) {
          run(() => undefined);
          done();
          return;
        }
        queued.push(() => {
          run(() => undefined);
          done();
        });
      },
    });
  }

  private dispatch(
    agent: StreamAgent,
    encOrCb: BufferEncoding | TransformCallback,
    maybeCb?: TransformCallback,
  ): void {
    const cb: TransformCallback = typeof encOrCb === "function" ? encOrCb : maybeCb ?? (() => undefined);
    const enc: BufferEncoding = typeof encOrCb === "function" ? "utf8" : encOrCb;
    agent.settled = false;
    this.inFlight++;
    const done: TransformCallback = (err, next) => {
      this.inFlight = Math.max(0, this.inFlight - 1);
      EdgeStream.releaseAgent(agent, this.id);
      cb(err, next);
      agent.graphAgent.streamGraph.notifyIdle();
    };

    if (this._handler) {
      const started = Date.now();
      const timeoutMs = agent.timeoutMs ?? this.options.timeoutMs;
      const tickAbort = new AbortController();
      agent.signal = composeSignals(tickAbort.signal, agent.signal, agent.graphAgent.streamGraph.abortSignal);
      const run = Promise.resolve().then(() => {
        if (agent.signal?.aborted) throw new Error("aborted");
        return this._handler!(agent);
      });
      let timer: ReturnType<typeof setTimeout> | undefined;
      const work =
        timeoutMs && timeoutMs > 0
          ? Promise.race([
              run,
              new Promise((_, reject) => {
                timer = setTimeout(() => {
                  this.timedOut++;
                  tickAbort.abort();
                  reject(new Error(`vertex ${this.id} timed out after ${timeoutMs}ms`));
                }, timeoutMs);
              }),
            ])
          : run;
      work
        .then((result) => {
          if (timer) clearTimeout(timer);
          this.lastHandlerMs = Date.now() - started;
          this.totalHandlerMs += this.lastHandlerMs;
          if (agent.settled) return;
          this.applyOutcome(agent, normalizeOutcome(result, agent.payload), done);
        })
        .catch((err: Error) => {
          if (timer) clearTimeout(timer);
          this.lastHandlerMs = Date.now() - started;
          this.totalHandlerMs += this.lastHandlerMs;
          this.applyOutcome(agent, { disposition: "error", errorMessage: err.message }, done, err);
        });
      return;
    }
    if (this._transform) {
      this._transform(agent, enc, done);
      return;
    }
    this.applyOutcome(agent, { disposition: "forward", payload: agent.payload }, done);
  }

  private applyOutcome(
    agent: StreamAgent,
    outcome: { disposition: string; payload?: unknown; errorMessage?: string },
    cb: TransformCallback,
    err?: Error,
  ): void {
    switch (outcome.disposition) {
      case "skip":
        agent.skip(this.id, cb);
        return;
      case "error":
        agent.error(this.id, err ?? new Error(outcome.errorMessage ?? "vertex error"), cb);
        return;
      case "terminate":
        agent.terminate(this.id, outcome.payload, cb);
        return;
      case "forward":
      default:
        agent.forward(this.id, outcome.payload, cb);
    }
  }

  get stream(): Transform {
    return this._stream;
  }

  set stream(stream: Transform) {
    this._stream = stream;
  }

  /** Classic (agent, enc, cb) transform. */
  set Transform(transform: ClassicTransform) {
    this._transform = transform;
    this._handler = null;
  }

  get Transform(): ClassicTransform | null {
    return this._transform;
  }

  /** Sync / async I/O handler. Return the next payload to forward. */
  set handler(fn: VertexHandler) {
    this._handler = fn;
    this._transform = null;
  }

  get handler(): VertexHandler | null {
    return this._handler;
  }

  get spec(): NodeStats {
    const stats = this.resetStats();
    stats.sourceCount = this.sourceCount;
    stats.sinkCount = this.sinkCount;
    stats.publish = this.publish;
    stats.external = this.external;
    stats.updateDataStats = this._updateDataStats;
    return stats;
  }

  resetStats(): NodeStats {
    return {
      id: this.id,
      objectCount: 0,
      errorCount: 0,
      ignoreCount: 0,
      sourceCount: this.sourceCount,
      sinkCount: this.sinkCount,
      publish: this.publish,
      external: this.external,
      highwaterMark: Math.max(this.peakDepth, this.bufferDepth()),
      timestamp: null,
      inFlight: this.inFlight,
      accepted: this.accepted,
      lastHandlerMs: this.lastHandlerMs,
      totalHandlerMs: this.totalHandlerMs,
      timedOut: this.timedOut,
      data: this._resetDataStats ? this._resetDataStats() : {},
      updateDataStats: this._updateDataStats,
    };
  }

  bufferDepth(): number {
    const s = this._stream as Transform & { writableLength?: number };
    // Occupancy is admitted-but-unconsumed input, not the readable side.
    // Counting readableLength left drain() stuck after the last item.
    const depth = s.writableLength ?? 0;
    this.peakDepth = Math.max(this.peakDepth, depth, this.inFlight);
    return depth;
  }

  resetPeaks(): void {
    this.peakDepth = this.bufferDepth();
  }

  set updateDataStats(fn: ((payload: unknown, data: Record<string, unknown>) => void) | undefined) {
    this._updateDataStats = fn;
  }
  get updateDataStats(): ((payload: unknown, data: Record<string, unknown>) => void) | undefined {
    return this._updateDataStats;
  }

  set resetDataStats(fn: () => Record<string, unknown>) {
    this._resetDataStats = fn;
  }

  write(data: unknown): boolean {
    this.accepted++;
    return this._stream.write(data);
  }

  onceDrain(): Promise<void> {
    return new Promise((resolve) => {
      this._stream.once("drain", resolve);
    });
  }
}
