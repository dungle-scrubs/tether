import { Effect, Fiber, Layer } from "effect";

import { ServerConfigLive } from "./config.js";
import { DatabaseLive } from "./db.js";
import { AppServerLive } from "./http.js";
import { SessionServiceEffectLive } from "./session-service.js";

const ServerLive = AppServerLive.pipe(
  Layer.provide(SessionServiceEffectLive),
  Layer.provide(DatabaseLive),
  Layer.provide(ServerConfigLive),
);

/**
 * Boots the HTTP/WebSocket service through Effect layers and interrupts the
 * runtime scope on process shutdown signals so scoped resources finalize.
 */
async function main(): Promise<void> {
  const fiber = Effect.runFork(Layer.launch(ServerLive));

  /**
   * Interrupts the live Effect layer so app-server and database finalizers run.
   */
  const shutdown = async (): Promise<void> => {
    await Effect.runPromise(Fiber.interrupt(fiber));
    process.exit(0);
  };
  process.once("SIGINT", () => {
    void shutdown();
  });
  process.once("SIGTERM", () => {
    void shutdown();
  });
  await Effect.runPromise(Fiber.join(fiber));
}

main().catch((error: unknown) => {
  console.error(error);
  process.exit(1);
});
