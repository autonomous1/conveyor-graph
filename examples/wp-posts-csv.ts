/**
 * Example graph:
 *   connect → query → csv → print
 *   * → graph/error (on throw)
 *
 *   MYSQL_HOST=127.0.0.1 MYSQL_PORT=13306 MYSQL_USER=root MYSQL_PASSWORD= \
 *   MYSQL_DATABASE=wordpress_db npm run example:wp-posts
 */
import mysql from "mysql2/promise";
import { GraphAgent, ConveyorGraph, type StreamAgent } from "../src/index.js";

type PostRow = Record<string, unknown>;

interface QueryPayload {
  rows: PostRow[];
}

interface CsvPayload extends QueryPayload {
  csv: string;
}

const IO_TIMEOUT_MS = Number(process.env.MYSQL_TIMEOUT_MS ?? 8000);

function csvEscape(value: unknown): string {
  if (value === null || value === undefined) return "";
  let text: string;
  if (value instanceof Date) {
    text = Number.isNaN(value.getTime()) ? "" : value.toISOString();
  } else if (typeof Buffer !== "undefined" && Buffer.isBuffer(value)) {
    text = value.toString("utf8");
  } else if (typeof value === "bigint") {
    text = value.toString();
  } else if (typeof value === "object") {
    text = JSON.stringify(value);
  } else {
    text = String(value);
  }
  if (/[",\n\r]/.test(text)) return `"${text.replace(/"/g, '""')}"`;
  return text;
}

function toCsv(rows: PostRow[]): string {
  if (!rows.length) return "";
  const columns = Object.keys(rows[0]!);
  const header = columns.map(csvEscape).join(",");
  const body = rows.map((row) => columns.map((col) => csvEscape(row[col])).join(","));
  return [header, ...body].join("\n");
}

function withTimeout<T>(promise: Promise<T>, label: string, ms = IO_TIMEOUT_MS): Promise<T> {
  let timer: ReturnType<typeof setTimeout> | undefined;
  const timeout = new Promise<T>((_, reject) => {
    timer = setTimeout(() => reject(new Error(`${label} timed out after ${ms}ms`)), ms);
  });
  return Promise.race([promise, timeout]).finally(() => {
    if (timer) clearTimeout(timer);
  });
}

async function main(): Promise<void> {
  const graph = new ConveyorGraph("wp-posts").initGraph();

  graph.define("connect", async (agent: StreamAgent) => {
    console.log("connect");
    const connection = await withTimeout(
      mysql.createConnection({
        host: process.env.MYSQL_HOST ?? "127.0.0.1",
        port: Number(process.env.MYSQL_PORT ?? 13306),
        user: process.env.MYSQL_USER ?? "root",
        password: process.env.MYSQL_PASSWORD ?? "",
        database: process.env.MYSQL_DATABASE ?? "wordpress_db",
        connectTimeout: IO_TIMEOUT_MS,
      }),
      "connect",
    );
    agent.context.db = connection;
    console.log("connected");
    return { connected: true };
  });

  graph.define("query", async (agent: StreamAgent) => {
    console.log("query");
    const connection = agent.context.db as mysql.Connection | undefined;
    if (!connection) {
      throw new Error("query: no database connection on agent.context");
    }
    const [rows] = await withTimeout(
      connection.query({ sql: "SELECT * FROM wp_posts LIMIT 20", timeout: IO_TIMEOUT_MS }),
      "query",
    );
    const list = rows as PostRow[];
    console.log("queried", Array.isArray(list) ? list.length : 0, "rows");
    return { rows: list } satisfies QueryPayload;
  });

  graph.define("csv", (agent: StreamAgent) => {
    console.log("csv");
    const { rows } = agent.payload as QueryPayload;
    if (!Array.isArray(rows)) {
      throw new Error("csv: payload.rows missing");
    }
    const csv = toCsv(rows);
    console.log("csv done", csv.split("\n").length, "lines");
    return { rows, csv } satisfies CsvPayload;
  });

  let resolveDone: (csv: string) => void;
  let rejectDone: (err: Error) => void;
  const finished = new Promise<string>((resolve, reject) => {
    resolveDone = resolve;
    rejectDone = reject;
  });

  graph.define("print", { publish: true }, (agent: StreamAgent) => {
    console.log("print");
    const payload = agent.payload as CsvPayload;
    console.log(payload.csv);
    resolveDone(payload.csv);
    return payload;
  });

  graph.define(graph.GRAPH_ERROR, { publish: true }, (agent: StreamAgent) => {
    const payload = agent.payload as { error?: string };
    console.error("graph/error", payload.error ?? JSON.stringify(payload, null, 2));
    rejectDone(new Error(String(payload.error ?? "graph/error")));
    return payload;
  });

  graph.link("connect", "query", "csv", "print", graph.GRAPH_LOG);

  const ga = new GraphAgent("wp-posts", {}, { logTopic: "graph/log", errorTopic: "graph/error" }, graph);
  ga.write("connect", "wp-posts-1", {});

  try {
    await withTimeout(finished, "pipeline", IO_TIMEOUT_MS * 3);
  } finally {
    const db = ga.context.db as mysql.Connection | undefined;
    if (db) {
      try {
        await db.end();
      } catch (err) {
        console.error("graph/error", err instanceof Error ? err.message : err);
      }
    }
  }
}

main().catch((err) => {
  console.error("graph/error", err instanceof Error ? err.message : err);
  process.exitCode = 1;
});
