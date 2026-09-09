import { readFileSync } from "node:fs";
import { resolve } from "node:path";
import { describe, expect, it } from "vitest";
import { BUILTIN, DEFAULT_TOPICS, PROTO_MESSAGE_NAMES, PROTO_PATH } from "../src/model.js";

describe("proto drift", () => {
  const proto = readFileSync(resolve(process.cwd(), PROTO_PATH), "utf8");

  it("documents every message name exported by model.ts", () => {
    for (const name of PROTO_MESSAGE_NAMES) {
      expect(proto, `missing message ${name}`).toMatch(new RegExp(`\\b(message|enum)\\s+${name}\\b`));
    }
  });

  it("keeps builtin vertex ids in lockstep", () => {
    expect(proto).toContain("graph/log");
    expect(proto).toContain("graph/skip");
    expect(proto).toContain("graph/error");
    expect(BUILTIN.log).toBe("graph/log");
    expect(BUILTIN.skip).toBe("graph/skip");
    expect(BUILTIN.error).toBe("graph/error");
  });

  it("publishes item_wait_ms on LinkStats", () => {
    expect(proto).toMatch(/item_wait_ms/);
  });

  it("keeps default bus topics in lockstep", () => {
    expect(DEFAULT_TOPICS.layout).toBe("graph/layout");
    expect(proto).toContain("graph/layout");
  });
});
