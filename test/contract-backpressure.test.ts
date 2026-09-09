import { describe, expect, it } from "vitest";
import { ConveyorGraph, type StreamAgent } from "../src/index.js";
import { agent, wait } from "./helpers.js";

describe("contract: backpressure and limits", () => {
  it("send resolves at admission, not downstream completion", async () => {
    let done = false;
    const graph = new ConveyorGraph("g")
      .initGraph()
      .define("src", (a: StreamAgent) => a.payload)
      .define("slow", async (a: StreamAgent) => {
        await wait(40);
        done = true;
        return a.payload;
      })
      .connect("src", "slow")
      .seal();
    const started = Date.now();
    await agent(graph).send("src", "1", "x");
    expect(Date.now() - started).toBeLessThan(30);
    expect(done).toBe(false);
    await wait(60);
    expect(done).toBe(true);
  });

  it("send rejects on abort", async () => {
    const graph = new ConveyorGraph("g").initGraph().define("src", async (a: StreamAgent) => {
      await wait(200);
      return a.payload;
    });
    const ac = new AbortController();
    ac.abort();
    await expect(agent(graph).send("src", "1", "x", { signal: ac.signal })).rejects.toThrow(/abort/i);
  });

  it("required blocking edge does not grow past capacity", async () => {
    const graph = new ConveyorGraph("g").initGraph();
    graph.define("src", (a: StreamAgent) => a.payload);
    graph.define("slow", async (a: StreamAgent) => {
      await wait(40);
      return a.payload;
    });
    graph.connect("src", "slow", { capacity: 1, delivery: "required", overflow: "block" });
    const ga = agent(graph);
    ga.write("src", "1", 1);
    ga.write("src", "2", 2);
    ga.write("src", "3", 3);
    await wait(20);
    expect(graph.edge["src-slow"]!.queued).toBeLessThanOrEqual(1);
    expect(graph.edge["src-slow"]!.peakQueued).toBeLessThanOrEqual(1);
  });

  it("best-effort drop does not stall a required sibling", async () => {
    const required: unknown[] = [];
    const graph = new ConveyorGraph("g").initGraph();
    graph.define("src", { fanout: "all" }, (a: StreamAgent) => a.payload);
    graph.define("req", (a: StreamAgent) => {
      required.push(a.payload);
      return a.payload;
    });
    graph.define("opt", async (a: StreamAgent) => {
      await wait(80);
      return a.payload;
    });
    graph.connect("src", "req", { capacity: 8, delivery: "required", overflow: "block" });
    graph.connect("src", "opt", { capacity: 1, delivery: "bestEffort", overflow: "drop" });
    const ga = agent(graph);
    for (let i = 0; i < 5; i++) ga.write("src", String(i), i);
    await wait(50);
    expect(required.length).toBeGreaterThanOrEqual(5);
    expect(graph.edge["src-opt"]!.dropCount).toBeGreaterThanOrEqual(1);
  });

  it("overflow-error produces one error outcome", async () => {
    const graph = new ConveyorGraph("g").initGraph();
    graph.define("src", (a: StreamAgent) => a.payload);
    graph.define("dst", async (a: StreamAgent) => {
      await wait(80);
      return a.payload;
    });
    graph.connect("src", "dst", { capacity: 1, delivery: "required", overflow: "error" });
    const ga = agent(graph);
    ga.write("src", "1", 1);
    ga.write("src", "2", 2);
    await wait(30);
    expect(graph.edge["src-dst"]!.errorCount).toBeGreaterThanOrEqual(1);
    expect(ga.node.src!.errorCount).toBeGreaterThanOrEqual(1);
  });

  it("serial graph preserves input order", async () => {
    const order: number[] = [];
    const graph = new ConveyorGraph("g").initGraph();
    graph.define("src", (a: StreamAgent) => a.payload);
    graph.define("dst", (a: StreamAgent) => {
      order.push(a.payload as number);
      return a.payload;
    });
    graph.connect("src", "dst");
    const ga = agent(graph);
    for (let i = 0; i < 5; i++) ga.write("src", String(i), i);
    await wait(40);
    expect(order).toEqual([0, 1, 2, 3, 4]);
  });

  it("parallel vertex stays within configured concurrency", async () => {
    let current = 0;
    let peak = 0;
    const graph = new ConveyorGraph("g").initGraph();
    graph.define("work", { parallel: 2 }, async (a: StreamAgent) => {
      current++;
      peak = Math.max(peak, current);
      await wait(25);
      current--;
      return a.payload;
    });
    const ga = agent(graph);
    for (let i = 0; i < 6; i++) ga.write("work", String(i), i);
    await wait(120);
    expect(peak).toBeLessThanOrEqual(2);
  });
});
