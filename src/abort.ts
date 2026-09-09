/** Combine caller, graph, timeout, and force-stop signals into one AbortSignal. */
export function composeSignals(...signals: Array<AbortSignal | undefined>): AbortSignal {
  const live = signals.filter((s): s is AbortSignal => Boolean(s));
  if (live.length === 0) return new AbortController().signal;
  if (live.length === 1) return live[0]!;
  if (typeof AbortSignal.any === "function") return AbortSignal.any(live);
  const merged = new AbortController();
  const abort = () => merged.abort();
  for (const signal of live) {
    if (signal.aborted) {
      abort();
      break;
    }
    signal.addEventListener("abort", abort, { once: true });
  }
  return merged.signal;
}
