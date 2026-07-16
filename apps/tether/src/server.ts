import { Cause, Effect, Fiber, Layer, Option, Runtime } from "effect";

import { ServerConfigLive } from "./config.js";
import { DatabaseMigrationError, projectDatabaseMigrationFailure } from "./database-migration.js";
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
  const migrationError = findDatabaseMigrationError(error);
  if (migrationError === null) {
    console.error(error);
  } else {
    console.error(
      JSON.stringify({
        details: projectDatabaseMigrationFailure(migrationError),
        event: "database.migration_failed",
      }),
    );
  }
  process.exit(1);
});

/** Finds a recognized migration failure through Effect and Error cause wrappers. */
function findDatabaseMigrationError(error: unknown): DatabaseMigrationError | null {
  const failure = unwrapEffectFailure(error);
  if (failure instanceof DatabaseMigrationError) {
    return failure;
  }
  if (failure instanceof Error && failure.cause !== undefined) {
    return findDatabaseMigrationError(failure.cause);
  }
  return null;
}

/** Restores the typed failure value wrapped by Effect's Promise runtime. */
function unwrapEffectFailure(error: unknown): unknown {
  if (!Runtime.isFiberFailure(error)) {
    return error;
  }
  return Option.getOrUndefined(Cause.failureOption(error[Runtime.FiberFailureCauseId])) ?? error;
}
