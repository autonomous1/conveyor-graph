const NEVER_ABORTED = new AbortController().signal;

/** Combine caller, graph, timeout, and force-stop signals into one AbortSignal. */
export function composeSignals(...signals: Array<AbortSignal | undefined>): AbortSignal {
  const live = signals.filter((s): s is AbortSignal => Boolean(s));
  if (live.length === 0) return NEVER_ABORTED;
  if (live.length === 1) return live[0]!;
  if (typeof AbortSignal.any === "function") return AbortSignal.any(live);

  const merged = new AbortController();
  const cleanups: Array<() => void> = [];
  const cleanup = (): void => {
    for (const fn of cleanups) fn();
    cleanups.length = 0;
  };
  const abort = (): void => {
    cleanup();
    if (!merged.signal.aborted) merged.abort();
  };
  for (const signal of live) {
    if (signal.aborted) {
      abort();
      break;
    }
    const onAbort = (): void => abort();
    signal.addEventListener("abort", onAbort, { once: true });
    cleanups.push(() => signal.removeEventListener("abort", onAbort));
  }
  if (!merged.signal.aborted) {
    merged.signal.addEventListener("abort", cleanup, { once: true });
  }
  return merged.signal;
}
