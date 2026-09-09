import { BUILTIN } from "./model.js";
import type { GraphAgent } from "./GraphAgent.js";
import type { NodeStats } from "./model.js";

export type TransformCallback = (error: Error | null, agent?: StreamAgent | null) => void;

export class StreamAgent {
  private _id: string;
  private _payload: unknown;
  readonly graphAgent: GraphAgent;
  /** True after forward / skip / error has consumed this transform tick. */
  settled = false;
  signal?: AbortSignal;
  timeoutMs?: number;
  private reportingError = false;

  constructor(id: string, payload: unknown, graphAgent: GraphAgent) {
    this._id = id;
    this._payload = payload;
    this.graphAgent = graphAgent;
  }

  get id(): string {
    return this._id;
  }
  set id(newId: string) {
    this._id = newId;
  }

  get payload(): unknown {
    return this._payload;
  }
  set payload(value: unknown) {
    this._payload = value;
  }

  get context() {
    return this.graphAgent.context;
  }

  get node(): Record<string, NodeStats> {
    return this.graphAgent.node;
  }

  /**
   * Offer `payload` through the outgoing edges of `nodeId`.
   * Does not enqueue into `nodeId`'s handler. See CONTRACT.md.
   */
  forwardFrom(nodeId: string, payload: unknown, cb: TransformCallback = () => undefined): void {
    this.settled = true;
    const node = this.graphAgent.node[nodeId];
    if (!node) {
      this.graphAgent.streamGraph.deliver(nodeId, this, payload, cb);
      return;
    }

    if (node.external) {
      this.graphAgent.publish(nodeId, payload);
      this.recordStats(node, payload);
      cb(null, null);
      return;
    }

    if (node.publish) this.graphAgent.publish(nodeId, payload);
    this.recordStats(node, payload);

    if (node.sourceCount === 0) {
      cb(null, null);
      return;
    }
    this.graphAgent.streamGraph.deliver(nodeId, this, payload, cb);
  }

  /** @deprecated use forwardFrom — alias kept for classic through2 callbacks */
  forward(nodeId: string, payload: unknown, cb: TransformCallback = () => undefined): void {
    this.forwardFrom(nodeId, payload, cb);
  }

  /**
   * Admit this agent into `nodeId`'s handler queue (ingress to that vertex).
   */
  routeTo(nodeId: string, payload: unknown, cb: TransformCallback = () => undefined): void {
    this.settled = true;
    this.payload = payload;
    this.graphAgent.write(nodeId, this.id, payload, this);
    cb(null, null);
  }

  skip(nodeId: string, cb: TransformCallback = () => undefined): void {
    this.settled = true;
    const node = this.graphAgent.node[nodeId];
    if (node) node.ignoreCount++;
    cb(null, null);
    this.graphAgent.newLink(nodeId, BUILTIN.skip);
    this.graphAgent.write(BUILTIN.skip, this.id, { skip: this.id, node: nodeId }, this);
  }

  error(nodeId: string, err: Error, cb: TransformCallback = () => undefined): void {
    this.settled = true;
    cb(null, null);
    if (this.reportingError || nodeId === BUILTIN.error) return;
    this.reportingError = true;
    this.graphAgent.logError(nodeId, err, {}, this);
  }

  terminate(nodeId: string, payload: unknown, cb: TransformCallback = () => undefined): void {
    this.settled = true;
    this.payload = payload;
    const node = this.graphAgent.node[nodeId];
    if (node) this.recordStats(node, payload);
    cb(null, null);
  }

  private recordStats(node: NodeStats, payload: unknown): void {
    if (node.updateDataStats) node.updateDataStats(payload, node.data ?? {});
    node.objectCount++;
    node.timestamp = new Date().toISOString();
  }
}
