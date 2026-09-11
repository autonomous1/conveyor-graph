import { timingSafeEqual } from "node:crypto";
import { GraphAgent, type MonitorHooks } from "./GraphAgent.js";
import type { ConveyorGraph } from "./ConveyorGraph.js";
import type { SessionContext, SubscriberRecord } from "./model.js";

function sameAuth(stored: unknown, presented: unknown): boolean {
  if (stored == null || presented == null) return false;
  if (typeof stored === "string" && typeof presented === "string") {
    const a = Buffer.from(stored);
    const b = Buffer.from(presented);
    return a.length === b.length && timingSafeEqual(a, b);
  }
  if (typeof stored !== "object" || typeof presented !== "object") {
    return Object.is(stored, presented);
  }
  try {
    return JSON.stringify(stored) === JSON.stringify(presented);
  } catch {
    return false;
  }
}

/**
 * In-memory subscriber session. Persistence is the application's job;
 * this package does not depend on LevelDB or any other store.
 */
export class Subscriber implements SubscriberRecord {
  readonly id: string;
  auth: unknown;
  context: SessionContext;
  private readonly streamGraph: ConveyorGraph;
  private readonly hooks: MonitorHooks;
  private _graphAgent?: GraphAgent;

  constructor(
    id: string,
    auth: unknown,
    context: SessionContext,
    streamGraph: ConveyorGraph,
    hooks: MonitorHooks = {},
  ) {
    this.id = id;
    this.auth = auth;
    this.context = { ...context };
    this.streamGraph = streamGraph;
    this.hooks = hooks;
  }

  get Id(): string {
    return this.id;
  }
  get Context(): SessionContext {
    return this.context;
  }
  set Context(update: SessionContext) {
    Object.assign(this.context, update);
  }

  get graphAgent(): GraphAgent {
    return (this._graphAgent ??= new GraphAgent(this.id, this.auth, this.context, this.streamGraph, this.hooks));
  }

  /** Equality check against the stored session secret. Not a password hash. */
  verifyCredentials(testAuth: unknown): boolean {
    return sameAuth(this.auth, testAuth);
  }

  reset(): void {
    this.context = {};
  }
}

export class SubscriberRegistry {
  private readonly sessions = new Map<string, Subscriber>();

  put(subscriber: Subscriber): Subscriber {
    this.sessions.set(subscriber.id, subscriber);
    return subscriber;
  }

  get(id: string): Subscriber | undefined {
    return this.sessions.get(id);
  }

  login(id: string, auth: unknown): Subscriber {
    const existing = this.sessions.get(id);
    if (!existing || !existing.verifyCredentials(auth)) {
      throw new Error("invalid credentials for subscriber");
    }
    return existing;
  }

  logout(id: string): void {
    this.sessions.delete(id);
  }
}
