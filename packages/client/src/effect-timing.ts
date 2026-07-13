import { Effect } from "effect";

/** Sleeps inside an Effect program without keeping the Node.js process alive. */
export function sleepUnrefEffect(delayMs: number): Effect.Effect<void, never> {
  return Effect.async<void, never>((resume) => {
    const timeout = setTimeout(() => resume(Effect.void), delayMs);
    timeout.unref();
    return Effect.sync(() => clearTimeout(timeout));
  });
}
