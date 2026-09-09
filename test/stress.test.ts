import { describe, expect, it } from "vitest";
import { GraphAgent, ConveyorGraph, type StreamAgent } from "../src/index.js";

function wait(ms: number): Promise<void> {
  return new Promise((r) => setTimeout(r, ms));
}

function rand(max: number): number {
  return Math.floor(Math.random() * max);
}

describe("randomized overload and shutdown stress", () => {
  it("keeps required-edge queues within capacity under mixed load", async () => {
    const cap = 2 + rand(3);
    const graph = new ConveyorGraph("stress").initGraph();
    graph.define("src", { fanout: "all" }, (a: StreamAgent) => a.payload);
    graph.define("slow", async (a: StreamAgent) => {
      await wait(2 + rand(8));
      return a.payload;
    });
    graph.define("drop", async (a: StreamAgent) => {
      await wait(15);
      return a.payload;
    });
    graph.connect("src", "slow", { capacity: cap, delivery: "required", overflow: "block" });
    graph.connect("src", "drop", { capacity: 1, delivery: "bestEffort", overflow: "drop" });
    graph.seal();
    const ga = new GraphAgent("s", {}, {}, graph);
    const n = 20 + rand(20);
    for (let i = 0; i < n; i++) ga.write("src", String(i), { i });
    await wait(50);
    expect(graph.edge["src-slow"]!.queued).toBeLessThanOrEqual(cap);
    expect(graph.edge["src-slow"]!.peakQueued).toBeLessThanOrEqual(cap);
    await graph.stop({ force: true });
    expect(graph.status).toBe("stopped");
  });

  it("force-stop aborts handlers and rejects further writes", async () => {
    const seenAbort: boolean[] = [];
    const graph = new ConveyorGraph("stress").initGraph();
    graph.define("hold", async (a: StreamAgent) => {
      await new Promise<void>((resolve, reject) => {
        const t = setTimeout(resolve, 2_000);
        a.signal?.addEventListener("abort", () => {
          clearTimeout(t);
          seenAbort.push(true);
          reject(new Error("aborted"));
        });
      });
      return a.payload;
    });
    graph.seal();
    const ga = new GraphAgent("s", {}, {}, graph);
    ga.write("hold", "1", {});
    await wait(15);
    await graph.stop({ force: true });
    expect(graph.status).toBe("stopped");
    expect(() => ga.write("hold", "2", {})).toThrow(/stopped|draining/);
    expect(seenAbort.length).toBeGreaterThanOrEqual(1);
  });

  it("repeated drain and stop are idempotent after idle", async () => {
    const graph = new ConveyorGraph("stress").initGraph().define("n", (a: StreamAgent) => a.payload).seal();
    new GraphAgent("s", {}, {}, graph).write("n", "1", 1);
    await graph.drain(1_000);
    await graph.drain(1_000);
    await graph.stop();
    await graph.stop({ force: true });
    expect(graph.status).toBe("stopped");
  });
});
