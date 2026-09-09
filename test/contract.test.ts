import { describe, expect, it } from "vitest";
import { GraphAgent, ConveyorGraph, type StreamAgent } from "../src/index.js";

function wait(ms: number): Promise<void> {
  return new Promise((r) => setTimeout(r, ms));
}

describe("v1 execution contract", () => {
  it("supports fluent define/connect/seal", () => {
    const graph = new ConveyorGraph("gateway")
      .initGraph()
      .define("decode", { parallel: 4 }, (agent: StreamAgent) => agent.payload)
      .define("classify", (agent: StreamAgent) => agent.payload)
      .define("store", { parallel: 8, timeoutMs: 5_000 }, (agent: StreamAgent) => agent.payload)
      .define("alerts", { parallel: 2 }, (agent: StreamAgent) => agent.payload)
      .connect("decode", "classify", { capacity: 128, delivery: "required", overflow: "block" })
      .connect("classify", "store", {
        when: (agent: StreamAgent) => (agent.payload as { kind?: string }).kind === "record",
        capacity: 1000,
        delivery: "required",
        overflow: "block",
      })
      .connect("classify", "alerts", {
        when: (agent: StreamAgent) => (agent.payload as { severity?: string }).severity === "critical",
        capacity: 50,
        delivery: "bestEffort",
        overflow: "drop",
      })
      .seal();

    expect(graph.status).toBe("sealed");
    expect(graph.get("decode")).toBeDefined();
    expect(graph.edge["decode-classify"]!.capacity).toBe(128);
    expect(graph.edge["classify-alerts"]!.delivery).toBe("bestEffort");
  });

  it("send admits to ingress and does not wait for downstream handlers", async () => {
    let storeRan = false;
    const graph = new ConveyorGraph("g")
      .initGraph()
      .define("decode", (agent: StreamAgent) => agent.payload)
      .define("store", async (agent: StreamAgent) => {
        await wait(40);
        storeRan = true;
        return agent.payload;
      })
      .connect("decode", "store")
      .seal();

    const agent = new GraphAgent("t", {}, {}, graph);
    const started = Date.now();
    await agent.send("decode", "event-42", { kind: "record" });
    expect(Date.now() - started).toBeLessThan(30);
    expect(storeRan).toBe(false);
    await wait(60);
    expect(storeRan).toBe(true);
  });

  it("forwardFrom uses outgoing edges; routeTo enters a vertex handler", async () => {
    const viaEdge: unknown[] = [];
    const viaRoute: unknown[] = [];
    const graph = new ConveyorGraph("g").initGraph();
    graph.define("src", (agent: StreamAgent) => agent.payload);
    graph.define("edgeSink", (agent: StreamAgent) => {
      viaEdge.push(agent.payload);
      return agent.payload;
    });
    graph.define("direct", (agent: StreamAgent) => {
      viaRoute.push(agent.payload);
      return agent.payload;
    });
    graph.connect("src", "edgeSink");
    const ga = new GraphAgent("t", {}, {}, graph);

    ga.write("src", "1", "from-edge");
    await wait(30);
    expect(viaEdge).toContain("from-edge");
    expect(viaRoute).toEqual([]);

    const hopper = new (await import("../src/StreamAgent.js")).StreamAgent("2", "jumped", ga);
    hopper.routeTo("direct", "jumped");
    await wait(30);
    expect(viaRoute).toContain("jumped");
  });

  it("rejects required + drop at seal", () => {
    const graph = new ConveyorGraph("g")
      .initGraph()
      .define("a", (agent: StreamAgent) => agent.payload)
      .define("b", (agent: StreamAgent) => agent.payload)
      .connect("a", "b", { delivery: "required", overflow: "drop" });
    expect(() => graph.seal()).toThrow(/required path cannot use overflow=drop/);
  });

  it("routes when() exceptions to graph/error", async () => {
    const graph = new ConveyorGraph("g").initGraph();
    graph.define("src", (agent: StreamAgent) => agent.payload);
    graph.define("dst", (agent: StreamAgent) => agent.payload);
    graph.connect("src", "dst", {
      when: () => {
        throw new Error("bad predicate");
      },
    });
    const ga = new GraphAgent("t", {}, {}, graph);
    ga.write("src", "1", {});
    await wait(30);
    expect(ga.node.src!.errorCount).toBeGreaterThanOrEqual(1);
  });

  it("refuses define after seal and write after stop", async () => {
    const graph = new ConveyorGraph("g").initGraph().define("a", (agent: StreamAgent) => agent.payload).seal();
    expect(() => graph.define("b", (agent: StreamAgent) => agent.payload)).toThrow(/sealed/);
    await graph.stop({ force: true });
    const ga = new GraphAgent("t", {}, {}, graph);
    expect(() => ga.write("a", "1", {})).toThrow(/stopped/);
  });
});
