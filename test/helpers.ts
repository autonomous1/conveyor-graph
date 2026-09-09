import { GraphAgent, ConveyorGraph, type StreamAgent } from "../src/index.js";

export type { StreamAgent };

export function wait(ms: number): Promise<void> {
  return new Promise((r) => setTimeout(r, ms));
}

export function agent(graph: ConveyorGraph): GraphAgent {
  return new GraphAgent("test", {}, {}, graph);
}
