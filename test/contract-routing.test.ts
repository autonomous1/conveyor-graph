import { describe, expect, it } from "vitest";
import { ConveyorGraph, type StreamAgent } from "../src/index.js";
import { agent, wait } from "./helpers.js";

describe("contract: routing and outcomes", () => {
  it("forwards ordinary return values", async () => {
    const seen: unknown[] = [];
    const graph = new ConveyorGraph("g")
      .initGraph()
      .define("src", (a: StreamAgent) => Number(a.payload) + 1)
      .define("dst", (a: StreamAgent) => {
        seen.push(a.payload);
        return a.payload;
      })
      .connect("src", "dst");
    agent(graph).write("src", "1", 10);
    await wait(30);
    expect(seen).toContain(11);
  });

  it("uses current payload when the handler returns undefined", async () => {
    const seen: unknown[] = [];
    const graph = new ConveyorGraph("g")
      .initGraph()
      .define("src", () => undefined)
      .define("dst", (a: StreamAgent) => {
        seen.push(a.payload);
        return a.payload;
      })
      .connect("src", "dst");
    agent(graph).write("src", "1", "keep-me");
    await wait(30);
    expect(seen).toContain("keep-me");
  });

  it("supports explicit skip, error, and terminate outcomes", async () => {
    const graph = new ConveyorGraph("g").initGraph();
    graph.define("skipper", () => ({ disposition: "skip" as const }));
    graph.define("ender", () => ({ disposition: "terminate" as const }));
    graph.define("fail", () => ({ disposition: "error" as const, errorMessage: "nope" }));
    graph.connect("skipper", "graph/skip");
    const ga = agent(graph);
    ga.write("skipper", "1", {});
    ga.write("ender", "2", {});
    ga.write("fail", "3", {});
    await wait(40);
    expect(ga.node.skipper!.ignoreCount).toBeGreaterThanOrEqual(1);
    expect(ga.node.fail!.errorCount).toBeGreaterThanOrEqual(1);
    expect(ga.node.ender!.objectCount).toBeGreaterThanOrEqual(1);
  });

  it("routes all matching edges in fanout all", async () => {
    const left: unknown[] = [];
    const right: unknown[] = [];
    const graph = new ConveyorGraph("g").initGraph();
    graph.define("src", { fanout: "all" }, (a: StreamAgent) => a.payload);
    graph.define("left", (a: StreamAgent) => {
      left.push(a.payload);
      return a.payload;
    });
    graph.define("right", (a: StreamAgent) => {
      right.push(a.payload);
      return a.payload;
    });
    graph.connect("src", "left").connect("src", "right");
    agent(graph).write("src", "1", "x");
    await wait(40);
    expect(left).toContain("x");
    expect(right).toContain("x");
  });

  it("selects the first matching edge in fanout first", async () => {
    const left: unknown[] = [];
    const right: unknown[] = [];
    const graph = new ConveyorGraph("g").initGraph();
    graph.define("src", { fanout: "first" }, (a: StreamAgent) => a.payload);
    graph.define("left", (a: StreamAgent) => {
      left.push(a.payload);
      return a.payload;
    });
    graph.define("right", (a: StreamAgent) => {
      right.push(a.payload);
      return a.payload;
    });
    graph.connect("src", "left", { when: () => true });
    graph.connect("src", "right", { when: () => true });
    agent(graph).write("src", "1", "x");
    await wait(40);
    expect(left).toContain("x");
    expect(right).toEqual([]);
  });

  it("gives fan-out branches separate envelope state", async () => {
    const ids: string[] = [];
    const graph = new ConveyorGraph("g").initGraph();
    graph.define("src", { fanout: "all" }, (a: StreamAgent) => a.payload);
    graph.define("left", (a: StreamAgent) => {
      a.id = "left-branch";
      ids.push(a.id);
      return a.payload;
    });
    graph.define("right", (a: StreamAgent) => {
      ids.push(a.id);
      return a.payload;
    });
    graph.connect("src", "left").connect("src", "right");
    agent(graph).write("src", "shared", "x");
    await wait(40);
    expect(ids).toContain("left-branch");
    expect(ids.some((id) => id !== "left-branch")).toBe(true);
  });

  it("treats predicate exceptions as visible errors", async () => {
    const graph = new ConveyorGraph("g").initGraph();
    graph.define("src", (a: StreamAgent) => a.payload);
    graph.define("dst", (a: StreamAgent) => a.payload);
    graph.connect("src", "dst", {
      when: () => {
        throw new Error("bad predicate");
      },
    });
    const ga = agent(graph);
    ga.write("src", "1", {});
    await wait(30);
    expect(ga.node.src!.errorCount).toBeGreaterThanOrEqual(1);
  });

  it("counts filtered items on non-matching edges", async () => {
    const graph = new ConveyorGraph("g").initGraph();
    graph.define("src", (a: StreamAgent) => a.payload);
    graph.define("keep", (a: StreamAgent) => a.payload);
    graph.define("other", (a: StreamAgent) => a.payload);
    graph.connect("src", "keep", { when: (a: StreamAgent) => a.payload === "yes" });
    graph.connect("src", "other", { when: (a: StreamAgent) => a.payload === "no" });
    agent(graph).write("src", "1", "yes");
    await wait(30);
    expect(graph.edge["src-other"]!.filterCount).toBeGreaterThanOrEqual(1);
    expect(graph.edge["src-keep"]!.objectCount).toBeGreaterThanOrEqual(1);
  });
});
