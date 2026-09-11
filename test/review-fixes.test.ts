import { describe, expect, it } from "vitest";
import { ConveyorGraph, EdgeStream, Subscriber, type StreamAgent } from "../src/index.js";
import { agent, wait } from "./helpers.js";

describe("review fixes", () => {
  it("creates builtin vertices without an explicit initGraph()", async () => {
    const graph = new ConveyorGraph("g");
    expect(graph.vertex["graph/error"]).toBeDefined();
    expect(graph.vertex["graph/skip"]).toBeDefined();
    graph.define("job", () => {
      throw new Error("boom");
    });
    const ga = agent(graph);
    ga.write("job", "1", {});
    await wait(20);
    expect(ga.node.job!.errorCount).toBeGreaterThanOrEqual(1);
  });

  it("keeps hyphenated vertex pair ids distinct", () => {
    const graph = new ConveyorGraph("g");
    graph.connect("a-b", "c");
    graph.connect("a", "b-c");
    expect(EdgeStream.id("a-b", "c")).not.toBe(EdgeStream.id("a", "b-c"));
    expect(graph.edge[EdgeStream.id("a-b", "c")]).toBeDefined();
    expect(graph.edge[EdgeStream.id("a", "b-c")]).toBeDefined();
    expect(Object.keys(graph.edge)).toHaveLength(2);
  });

  it("does not reset custom data stats when reading spec", () => {
    const graph = new ConveyorGraph("g");
    let resets = 0;
    graph.define("job", (a: StreamAgent) => a.payload);
    graph.vertex.job!.resetDataStats = () => {
      resets++;
      return { bytes: 0 };
    };
    void graph.vertex.job!.spec;
    const afterFirstRead = resets;
    void graph.getGraph();
    void graph.vertex.job!.spec;
    expect(resets).toBe(afterFirstRead);
    graph.vertex.job!.resetStats();
    expect(resets).toBe(afterFirstRead + 1);
  });

  it("keeps per-vertex data across getGraph reads until clearStats", async () => {
    const graph = new ConveyorGraph("g");
    graph.define("job", (a: StreamAgent) => a.payload);
    graph.vertex.job!.resetDataStats = () => ({ bytes: 0 });
    graph.vertex.job!.updateDataStats = (payload, data) => {
      data.bytes = (Number(data.bytes) || 0) + Buffer.byteLength(String(payload));
    };
    graph.vertex.job!.resetStats();
    const ga = agent(graph);
    ga.write("job", "1", "abcd");
    await wait(20);
    const live = graph.getGraph().nodes.job!.data as { bytes: number };
    expect(live.bytes).toBeGreaterThan(0);
    expect(graph.getGraph().nodes.job!.data).toBe(live);
    ga.clearStats();
    expect((graph.getGraph().nodes.job!.data as { bytes: number }).bytes).toBe(0);
  });

  it("caches Subscriber.graphAgent", () => {
    const graph = new ConveyorGraph("g");
    const sub = new Subscriber("u", "secret", {}, graph);
    expect(sub.graphAgent).toBe(sub.graphAgent);
  });

  it("waitUntilProcessed resolves from whenIdle without a monitor", async () => {
    const graph = new ConveyorGraph("g");
    graph.define("job", async (a: StreamAgent) => {
      await wait(20);
      return a.payload;
    });
    const ga = agent(graph);
    ga.write("job", "1", 1);
    await ga.waitUntilProcessed(1);
    expect(graph.occupancy()).toBe(0);
  });

  it("onceDrain settles if the stream is destroyed", async () => {
    const graph = new ConveyorGraph("g");
    graph.define("job", (a: StreamAgent) => a.payload);
    const pending = graph.vertex.job!.onceDrain();
    graph.vertex.job!.stream.destroy();
    await expect(pending).resolves.toBeUndefined();
  });
});
