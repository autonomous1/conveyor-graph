import type { VertexOutcome } from "./model.js";

const DISPOSITIONS = new Set(["unspecified", "forward", "skip", "error", "terminate"]);

export function isOutcome(value: unknown): value is VertexOutcome {
  return Boolean(
    value &&
      typeof value === "object" &&
      "disposition" in value &&
      DISPOSITIONS.has((value as VertexOutcome).disposition),
  );
}

export function normalizeOutcome(result: unknown, fallbackPayload: unknown): VertexOutcome {
  if (isOutcome(result)) return result;
  if (result === undefined) return { disposition: "forward", payload: fallbackPayload };
  return { disposition: "forward", payload: result };
}
