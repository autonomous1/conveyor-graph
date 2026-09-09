import { describe, expect, it } from "vitest";
import { ConveyorGraph, type StreamAgent } from "../src/index.js";
import { agent, wait } from "./helpers.js";

describe("contract: lifecycle and failure", () => {
  it("drain waits for handlers to finish", async () => {
    const graph = new ConveyorGraph("g").initGraph();
    graph.define("job", async (a: StreamAgent) => {
      await wait(30);
      return a.payload;
    });
    graph.seal();
    agent(graph).write("job", "1", 1);
    await graph.drain(500);
    expect(graph.totalInFlight()).toBe(0);
    expect(graph.occupancy()).toBe(0);
  });

  it("draining and stopped graphs reject new input", async () => {
    const graph = new ConveyorGraph("g").initGraph().define("a", (a: StreamAgent) => a.payload).seal();
    const ga = agent(graph);
    await graph.stop({ force: true });
    expect(() => ga.write("a", "1", {})).toThrow(/stopped|draining/);
  });

  it("forced stop rejects pending admissions and is idempotent", async () => {
    const graph = new ConveyorGraph("g").initGraph().define("a", (a: StreamAgent) => a.payload).seal();
    await graph.stop({ force: true });
    await graph.stop({ force: true });
    await graph.drain(100);
    expect(graph.status).toBe("stopped");
  });

  it("graph-level abort force-stops", async () => {
    const ac = new AbortController();
    const graph = new ConveyorGraph("g").initGraph().define("a", (a: StreamAgent) => a.payload).seal({ signal: ac.signal });
    ac.abort();
    await wait(20);
    expect(graph.status).toBe("stopped");
  });

  it("vertex timeout records metrics and errors", async () => {
    const graph = new ConveyorGraph("g").initGraph();
    graph.define("slow", { timeoutMs: 20 }, async (a: StreamAgent) => {
      await wait(80);
      return a.payload;
    });
    const ga = agent(graph);
    ga.write("slow", "1", {});
    await wait(50);
    expect(graph.vertex.slow!.timedOut).toBeGreaterThanOrEqual(1);
    expect(ga.node.slow!.errorCount).toBeGreaterThanOrEqual(1);
  });

  it("late handler completion after timeout does not double-forward", async () => {
    let late = 0;
    const graph = new ConveyorGraph("g").initGraph();
    graph.define("slow", { timeoutMs: 15 }, async (a: StreamAgent) => {
      await wait(50);
      late++;
      return a.payload;
    });
    graph.define("dst", (a: StreamAgent) => {
      late += 10;
      return a.payload;
    });
    graph.connect("slow", "dst");
    agent(graph).write("slow", "1", "x");
    await wait(80);
    expect(graph.edge["slow-dst"]!.objectCount).toBe(0);
  });

  it("error accounting holds under fan-out", async () => {
    const graph = new ConveyorGraph("g").initGraph();
    graph.define("src", { fanout: "all" }, () => {
      throw new Error("boom");
    });
    graph.define("left", (a: StreamAgent) => a.payload);
    graph.define("right", (a: StreamAgent) => a.payload);
    graph.connect("src", "left").connect("src", "right");
    const ga = agent(graph);
    ga.write("src", "1", "x");
    await wait(30);
    expect(ga.node.src!.errorCount).toBeGreaterThanOrEqual(1);
    expect(graph.edge["src-left"]!.objectCount).toBe(0);
    expect(graph.edge["src-right"]!.objectCount).toBe(0);
  });
});
