export function parseJson<T = unknown>(jsonString: string): T | null {
  try {
    return JSON.parse(jsonString) as T;
  } catch {
    return null;
  }
}

export function slug(text: string | null | undefined): string | null {
  if (!text) return null;
  return text
    .toLowerCase()
    .replace(/[^\w ]+/g, "")
    .replace(/ +/g, "-");
}

export function timestamp(): string {
  return new Date().toISOString();
}

export function toPayloadString(payload: unknown): string {
  if (typeof payload === "object" && payload !== null) {
    return JSON.stringify(payload, null, 2);
  }
  return String(payload);
}
