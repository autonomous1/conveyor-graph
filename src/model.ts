/**
 * Hand-written TypeScript projection of proto/agentgraph/v1/graph.proto.
 * A drift test compares exported names / builtin ids against the proto file.
 * The runtime does not import protobuf.
 */

export const PROTO_PATH = "proto/agentgraph/v1/graph.proto";

export const BUILTIN = {
  log: "graph/log",
  skip: "graph/skip",
  error: "graph/error",
} as const;

export const DEFAULT_TOPICS = {
  layout: "graph/layout",
  log: "graph/log",
  error: "graph/error",
} as const;

export type BuiltinVertexId = (typeof BUILTIN)[keyof typeof BUILTIN];

export type FanoutMode = "all" | "first";

export interface VertexOptions {
  publish?: boolean;
  external?: boolean;
  parallel?: number;
  labels?: Record<string, string>;
  /** Wave B honors "first"; Wave A pipes to every downstream edge. */
  fanout?: FanoutMode;
  timeoutMs?: number;
}

export interface VertexDef {
  id: string;
  options?: VertexOptions;
}

export type Delivery = "required" | "bestEffort";
export type Overflow = "block" | "drop" | "error";

export interface EdgeOptions {
  capacity?: number;
  delivery?: Delivery;
  overflow?: Overflow;
}

export interface EdgeDef {
  id: string;
  source: string;
  target: string;
  capacity?: number;
  delivery?: Delivery;
  overflow?: Overflow;
}

export interface GraphDef {
  id?: string;
  vertices: Record<string, VertexDef>;
  edges: Record<string, EdgeDef>;
}

export type VertexDisposition =
  | "unspecified"
  | "forward"
  | "skip"
  | "error"
  | "terminate";

export interface VertexOutcome<T = unknown> {
  disposition: VertexDisposition;
  payload?: T;
  errorMessage?: string;
}

export interface NodeStats {
  id: string;
  objectCount: number;
  errorCount: number;
  ignoreCount: number;
  sourceCount: number;
  sinkCount: number;
  publish: boolean;
  external: boolean;
  highwaterMark: number;
  timestamp: string | null;
  inFlight: number;
  accepted: number;
  lastHandlerMs?: number;
  totalHandlerMs?: number;
  timedOut?: number;
  title?: string;
  data?: Record<string, unknown>;
  updateDataStats?: (payload: unknown, data: Record<string, unknown>) => void;
}

export interface LinkStats {
  id: string;
  source: string;
  target: string;
  value: number;
  objectCount: number;
  errorCount: number;
  highwaterMark: number;
  timestamp: string | null;
  dropCount?: number;
  filterCount?: number;
  capacity?: number;
  depth?: number;
  blockedMs?: number;
  lastDropReason?: string;
  itemWaitMs?: number;
}

export interface LayoutFrame {
  sequence: number;
  timestamp: string;
  nodes: NodeStats[];
  links: LinkStats[];
  extra?: Record<string, unknown>;
}

export interface MessageBus {
  publish(topic: string, payload: string): void;
}

export interface SessionContext {
  logTopic?: string;
  errorTopic?: string;
  [key: string]: unknown;
}

export interface SubscriberRecord {
  id: string;
  auth?: unknown;
}

export interface ValidationIssue {
  code: string;
  message: string;
  vertexId?: string;
  edgeId?: string;
}

export type GraphPhase = "building" | "sealed" | "draining" | "stopped";

export interface SealOptions {
  signal?: AbortSignal;
}

export interface StopOptions {
  force?: boolean;
}

export interface SendOptions {
  signal?: AbortSignal;
  timeoutMs?: number;
}

export interface GraphValidation {
  ok: boolean;
  issues: ValidationIssue[];
}

export const PROTO_MESSAGE_NAMES = [
  "VertexOptions",
  "Vertex",
  "Edge",
  "Graph",
  "BuiltinVertices",
  "Agent",
  "VertexDisposition",
  "VertexOutcome",
  "NodeStats",
  "LinkStats",
  "LayoutFrame",
  "BusTopics",
  "BusMessage",
  "Subscriber",
  "SessionContext",
  "ValidationIssue",
  "GraphValidation",
] as const;
