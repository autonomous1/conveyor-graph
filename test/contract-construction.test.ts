import { describe, expect, it } from "vitest";
import { ConveyorGraph, type StreamAgent } from "../src/index.js";

describe("contract: construction", () => {
  it("defines vertices and edges through the fluent API", () => {
    const graph = new ConveyorGraph("gateway")
      .initGraph()
      .define("decode", { parallel: 4 }, (a: StreamAgent) => a.payload)
      .define("classify", (a: StreamAgent) => a.payload)
      .connect("decode", "classify", { capacity: 128, delivery: "required", overflow: "block" })
      .seal();
    expect(graph.get("decode")).toBeDefined();
    expect(graph.edge["decode-classify"]!.capacity).toBe(128);
    expect(graph.status).toBe("sealed");
  });

  it("rejects mutation after seal", () => {
    const graph = new ConveyorGraph("g").initGraph().define("a", (a: StreamAgent) => a.payload).seal();
    expect(() => graph.define("b", (a: StreamAgent) => a.payload)).toThrow(/sealed/);
    expect(() => graph.connect("a", "b")).toThrow(/sealed/);
    expect(() => graph.link("a", "b")).toThrow(/sealed/);
  });

  it("reports invalid policies", () => {
    const graph = new ConveyorGraph("g")
      .initGraph()
      .define("a", (a: StreamAgent) => a.payload)
      .define("b", (a: StreamAgent) => a.payload)
      .connect("a", "b", { delivery: "required", overflow: "drop" });
    const result = graph.validate();
    expect(result.ok).toBe(false);
    expect(result.issues.some((i) => i.code === "invalid_policy")).toBe(true);
  });

  it("rejects self-cycles with a readable path", () => {
    const graph = new ConveyorGraph("g").initGraph().define("a", (a: StreamAgent) => a.payload);
    graph.connect("a", "a");
    const result = graph.validate();
    expect(result.issues.some((i) => i.code === "cycle" && i.message.includes("a"))).toBe(true);
  });

  it("rejects multi-node cycles with a readable path", () => {
    const graph = new ConveyorGraph("g").initGraph();
    graph.define("a", (a: StreamAgent) => a.payload).define("b", (a: StreamAgent) => a.payload).define("c", (a: StreamAgent) => a.payload);
    graph.connect("a", "b").connect("b", "c").connect("c", "a");
    const result = graph.validate();
    expect(result.ok).toBe(false);
    const cycle = result.issues.find((i) => i.code === "cycle");
    expect(cycle?.message).toMatch(/a|b|c/);
    expect(cycle?.message).toMatch(/->/);
  });

  it("preserves connect order as first-match priority", () => {
    const graph = new ConveyorGraph("g").initGraph();
    graph.define("src", { fanout: "first" }, (a: StreamAgent) => a.payload);
    graph.connect("src", "second", { when: () => true });
    graph.connect("src", "first-defined", { when: () => true });
    const edges = graph.edgesFrom("src");
    expect(edges[0]!.targetId).toBe("second");
    expect(edges[1]!.targetId).toBe("first-defined");
  });
});
