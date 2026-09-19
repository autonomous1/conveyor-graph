import { GraphAgent } from "../src/GraphAgent.js";
import { ConveyorGraph } from "../src/ConveyorGraph.js";
import { EdgeStream } from "../src/EdgeStream.js";
import type { StreamAgent } from "../src/StreamAgent.js";

export function eid(source: string, sink: string): string {
  return EdgeStream.id(source, sink);
}

export type { StreamAgent };

export function wait(ms: number): Promise<void> {
  return new Promise((r) => setTimeout(r, ms));
}

export function agent(graph: ConveyorGraph): GraphAgent {
  return new GraphAgent("test", {}, {}, graph);
}
