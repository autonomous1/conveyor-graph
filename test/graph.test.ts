import { describe, expect, it } from "vitest";
import { GraphAgent, ConveyorGraph, Subscriber, type StreamAgent } from "../src/index.js";

function wait(ms: number): Promise<void> {
  return new Promise((r) => setTimeout(r, ms));
}

describe("ConveyorGraph", () => {
  it("is injectable — two graphs do not share vertices", () => {
    const a = new ConveyorGraph("a").initGraph();
    const b = new ConveyorGraph("b").initGraph();
    a.define("only-a", () => "x");
    expect(a.vertex["only-a"]).toBeDefined();
    expect(b.vertex["only-a"]).toBeUndefined();
    expect(a.vertex).not.toBe(b.vertex);
  });

  it("wires builtin log/skip/error nodes", () => {
    const g = new ConveyorGraph().initGraph();
    expect(g.vertex[g.GRAPH_LOG]).toBeDefined();
    expect(g.vertex[g.GRAPH_SKIP]).toBeDefined();
    expect(g.vertex[g.GRAPH_ERROR]).toBeDefined();
  });

  it("links vertices and increments source/sink counts", () => {
    const g = new ConveyorGraph().initGraph();
    g.define("start", (agent: StreamAgent) => agent.payload);
    g.define("mid", (agent: StreamAgent) => agent.payload);
    g.define("end", { publish: true }, (agent: StreamAgent) => agent.payload);
    g.link("start", "mid", "end");
    expect(g.vertex.start!.sourceCount).toBe(1);
    expect(g.vertex.mid!.sinkCount).toBe(1);
    expect(g.vertex.mid!.sourceCount).toBe(1);
    expect(g.vertex.end!.sinkCount).toBe(1);
    expect(g.vertex.end!.sourceCount).toBe(0);
    expect(g.edge["start-mid"]).toBeDefined();
    expect(g.edge["mid-end"]).toBeDefined();
  });

  it("runs a sync handler and forwards payload", async () => {
    const g = new ConveyorGraph().initGraph();
    const seen: unknown[] = [];
    g.define("src", (agent: StreamAgent) => Number(agent.payload) + 1);
    g.define("dst", { publish: true }, (agent: StreamAgent) => {
      seen.push(agent.payload);
      return agent.payload;
    });
    g.link("src", "dst");

    const agent = new GraphAgent("t", {}, { logTopic: "graph/log" }, g);
    agent.write("src", "item-1", 10);
    await wait(30);
    expect(seen).toContain(11);
    expect(agent.node.src!.objectCount).toBeGreaterThanOrEqual(1);
  });

  it("runs an async handler before hand-off", async () => {
    const g = new ConveyorGraph().initGraph();
    const seen: unknown[] = [];
    g.define("src", async (agent: StreamAgent) => {
      await wait(10);
      return `ok:${agent.payload}`;
    });
    g.define("dst", { publish: true }, (agent: StreamAgent) => {
      seen.push(agent.payload);
      return agent.payload;
    });
    g.link("src", "dst");

    const ga = new GraphAgent("t", {}, {}, g);
    ga.write("src", "1", "n");
    await wait(40);
    expect(seen).toContain("ok:n");
  });

  it("reuses the agent across three vertices", async () => {
    const g = new ConveyorGraph().initGraph();
    const seen: string[] = [];
    g.define("a", async (agent: StreamAgent) => {
      seen.push("a");
      return { n: Number(agent.payload) + 1 };
    });
    g.define("b", async (agent: StreamAgent) => {
      seen.push("b");
      const n = (agent.payload as { n: number }).n;
      return { n: n + 1 };
    });
    g.define("c", { publish: true }, (agent: StreamAgent) => {
      seen.push("c");
      return agent.payload;
    });
    g.link("a", "b", "c");

    const ga = new GraphAgent("t", {}, {}, g);
    ga.write("a", "1", 0);
    await wait(40);
    expect(seen).toEqual(["a", "b", "c"]);
    expect((ga.node.c && seen.includes("c")) || seen[2] === "c").toBe(true);
  });

  it("skip routes to graph/skip", async () => {
    const g = new ConveyorGraph().initGraph();
    g.define("src", (agent: StreamAgent) => {
      agent.skip("src");
    });
    g.link("src", "graph/skip");

    const ga = new GraphAgent("t", {}, {}, g);
    ga.write("src", "1", {});
    await wait(30);
    expect(ga.node.src!.ignoreCount).toBeGreaterThanOrEqual(1);
  });
});

describe("validate and seal", () => {
  it("rejects cycles", () => {
    const g = new ConveyorGraph().initGraph();
    g.define("a", (agent: StreamAgent) => agent.payload);
    g.define("b", (agent: StreamAgent) => agent.payload);
    g.link("a", "b");
    g.link("b", "a");
    const result = g.validate();
    expect(result.ok).toBe(false);
    expect(result.issues.some((i) => i.code === "cycle")).toBe(true);
    expect(() => g.seal()).toThrow(/cycle/);
  });

  it("forbids define after seal", () => {
    const g = new ConveyorGraph().initGraph();
    g.define("a", (agent: StreamAgent) => agent.payload);
    g.seal();
    expect(() => g.define("b", (agent: StreamAgent) => agent.payload)).toThrow(/sealed/);
  });

  it("seals an acyclic graph", () => {
    const g = new ConveyorGraph().initGraph();
    g.define("a", (agent: StreamAgent) => agent.payload);
    g.define("b", (agent: StreamAgent) => agent.payload);
    g.link("a", "b");
    g.seal();
    expect(g.isSealed).toBe(true);
  });
});

describe("admission", () => {
  it("send resolves after write", async () => {
    const g = new ConveyorGraph().initGraph();
    const seen: unknown[] = [];
    g.define("src", (agent: StreamAgent) => agent.payload);
    g.define("dst", { publish: true }, (agent: StreamAgent) => {
      seen.push(agent.payload);
      return agent.payload;
    });
    g.link("src", "dst");
    g.seal();
    const ga = new GraphAgent("t", {}, {}, g);
    await ga.send("src", "1", "hello");
    await wait(30);
    expect(seen).toContain("hello");
  });

  it("honors explicit VertexOutcome from a handler", async () => {
    const g = new ConveyorGraph().initGraph();
    g.define("src", () => ({ disposition: "skip" as const }));
    g.link("src", "graph/skip");
    const ga = new GraphAgent("t", {}, {}, g);
    ga.write("src", "1", {});
    await wait(30);
    expect(ga.node.src!.ignoreCount).toBeGreaterThanOrEqual(1);
  });
});

describe("edges and fan-out", () => {
  it("broadcasts to every matching edge by default", async () => {
    const g = new ConveyorGraph().initGraph();
    const left: unknown[] = [];
    const right: unknown[] = [];
    g.define("src", (agent: StreamAgent) => agent.payload);
    g.define("left", (agent: StreamAgent) => {
      left.push(agent.payload);
      return agent.payload;
    });
    g.define("right", (agent: StreamAgent) => {
      right.push(agent.payload);
      return agent.payload;
    });
    g.connect("src", "left");
    g.connect("src", "right");
    const ga = new GraphAgent("t", {}, {}, g);
    ga.write("src", "1", "x");
    await wait(40);
    expect(left).toContain("x");
    expect(right).toContain("x");
  });

  it("fanout first uses only the first matching edge", async () => {
    const g = new ConveyorGraph().initGraph();
    const left: unknown[] = [];
    const right: unknown[] = [];
    g.define("src", { fanout: "first" }, (agent: StreamAgent) => agent.payload);
    g.define("left", (agent: StreamAgent) => {
      left.push(agent.payload);
      return agent.payload;
    });
    g.define("right", (agent: StreamAgent) => {
      right.push(agent.payload);
      return agent.payload;
    });
    g.connect("src", "left", { when: () => true });
    g.connect("src", "right", { when: () => true });
    const ga = new GraphAgent("t", {}, {}, g);
    ga.write("src", "1", "x");
    await wait(40);
    expect(left).toContain("x");
    expect(right).toEqual([]);
  });

  it("filters an edge with when()", async () => {
    const g = new ConveyorGraph().initGraph();
    const kept: unknown[] = [];
    const dropped: unknown[] = [];
    g.define("src", (agent: StreamAgent) => agent.payload);
    g.define("keep", (agent: StreamAgent) => {
      kept.push(agent.payload);
      return agent.payload;
    });
    g.define("drop", (agent: StreamAgent) => {
      dropped.push(agent.payload);
      return agent.payload;
    });
    g.connect("src", "keep", { when: (agent: StreamAgent) => agent.payload === "yes" });
    g.connect("src", "drop", { when: (agent: StreamAgent) => agent.payload === "no" });
    const ga = new GraphAgent("t", {}, {}, g);
    ga.write("src", "1", "yes");
    await wait(40);
    expect(kept).toContain("yes");
    expect(dropped).toEqual([]);
    expect(g.edge["src-drop"]!.filterCount).toBeGreaterThanOrEqual(1);
  });
});

describe("Subscriber", () => {
  it("creates a GraphAgent bound to the injected graph", () => {
    const g = new ConveyorGraph("sess").initGraph();
    const sub = new Subscriber("u1", { token: "x" }, { logTopic: "graph/log" }, g);
    const ga = sub.graphAgent;
    expect(ga.id).toBe("u1");
    expect(ga.streamGraph).toBe(g);
    expect(sub.verifyCredentials({ token: "x" })).toBe(true);
  });
});

describe("lifecycle", () => {
  it("times out a slow vertex handler", async () => {
    const g = new ConveyorGraph().initGraph();
    g.define("slow", { timeoutMs: 20 }, async () => {
      await wait(80);
      return "late";
    });
    const ga = new GraphAgent("t", {}, {}, g);
    ga.write("slow", "1", {});
    await wait(60);
    expect(g.vertex.slow!.timedOut).toBeGreaterThanOrEqual(1);
  });

  it("rejects write after stop", async () => {
    const g = new ConveyorGraph().initGraph();
    g.define("a", (agent: StreamAgent) => agent.payload);
    g.seal();
    await g.stop({ force: true });
    const ga = new GraphAgent("t", {}, {}, g);
    expect(() => ga.write("a", "1", {})).toThrow(/stopped/);
  });

  it("drain waits until in-flight work finishes", async () => {
    const g = new ConveyorGraph().initGraph();
    g.define("job", async (agent: StreamAgent) => {
      await wait(30);
      return agent.payload;
    });
    g.seal();
    const ga = new GraphAgent("t", {}, {}, g);
    ga.write("job", "1", 1);
    await g.drain(500);
    expect(g.totalInFlight()).toBe(0);
  });
});
