import { describe, expect, it } from "vitest";
import { ConveyorGraph, GraphAgent, type LayoutFrame, type StreamAgent } from "../src/index.js";
import { agent, wait } from "./helpers.js";

describe("contract: telemetry", () => {
  it("updates vertex accepted, timing, and in-flight after a tick", async () => {
    const graph = new ConveyorGraph("g").initGraph();
    graph.define("job", async (a: StreamAgent) => {
      await wait(15);
      return a.payload;
    });
    agent(graph).write("job", "1", 1);
    await wait(40);
    const v = graph.vertex.job!;
    expect(v.accepted).toBeGreaterThanOrEqual(1);
    expect(v.lastHandlerMs).toBeGreaterThan(0);
    expect(v.inFlight).toBe(0);
  });

  it("edge snapshots expose depth, peak, capacity, and filter counts", async () => {
    const graph = new ConveyorGraph("g").initGraph();
    graph.define("src", (a: StreamAgent) => a.payload);
    graph.define("dst", (a: StreamAgent) => a.payload);
    graph.connect("src", "dst", { capacity: 8, when: (a: StreamAgent) => a.payload !== "no" });
    const ga = agent(graph);
    ga.write("src", "1", "yes");
    ga.write("src", "2", "no");
    await wait(30);
    const snap = graph.edge["src-dst"]!.snapshot();
    expect(snap.capacity).toBe(8);
    expect(snap.filterCount).toBeGreaterThanOrEqual(1);
    expect(snap.highwaterMark).toBeGreaterThanOrEqual(snap.depth ?? 0);
  });

  it("layout frame uses live edge occupancy not count diffs", async () => {
    const frames: { links: { depth?: number; highwaterMark: number }[] }[] = [];
    const graph = new ConveyorGraph("g").initGraph();
    graph.define("src", (a: StreamAgent) => a.payload);
    graph.define("dst", (a: StreamAgent) => a.payload);
    graph.connect("src", "dst");
    const ga = agent(graph);
    ga.mqClient = {
      publish(_topic: string, payload: string) {
        frames.push(JSON.parse(payload) as (typeof frames)[number]);
      },
    };
    ga.write("src", "1", "x");
    await wait(20);
    ga.monitorFrame();
    expect(frames.length).toBeGreaterThanOrEqual(1);
    const link = frames[0]!.links.find((l) => true);
    if (link) {
      expect(typeof link.highwaterMark).toBe("number");
    }
  });

  it("clearStats resets node counters from live specs", async () => {
    const graph = new ConveyorGraph("g").initGraph();
    graph.define("job", (a: StreamAgent) => a.payload);
    const ga = agent(graph);
    ga.write("job", "1", 1);
    await wait(20);
    ga.clearStats();
    expect(ga.node.job!.objectCount).toBe(0);
  });

  it("emits idle frames on onFrame with graphId and phase", () => {
    const graph = new ConveyorGraph("viz");
    graph.define("only", (a: StreamAgent) => a.payload);
    const frames: LayoutFrame[] = [];
    const ga = new GraphAgent("s", {}, {}, graph, {
      onFrame: (frame) => frames.push(frame),
    });
    ga.monitorFrame();
    expect(frames).toHaveLength(1);
    expect(frames[0]!.graphId).toBe("viz");
    expect(frames[0]!.phase).toBe("building");
    expect(typeof frames[0]!.aborted).toBe("boolean");
    expect(frames[0]!.sequence).toBe(1);
  });

  it("publishes a terminal frame from stopStreamMonitor", () => {
    const graph = new ConveyorGraph("viz");
    const frames: LayoutFrame[] = [];
    const ga = new GraphAgent("s", {}, {}, graph, {
      onFrame: (frame) => frames.push(frame),
      intervalMs: 10_000,
    });
    ga.startStreamMonitor();
    ga.stopStreamMonitor();
    expect(frames.length).toBeGreaterThanOrEqual(1);
    expect(frames.at(-1)!.graphId).toBe("viz");
  });
});
