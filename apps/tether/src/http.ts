import { createServer, type IncomingMessage, type ServerResponse } from "node:http";
import { URL } from "node:url";

import { Context, Effect, Layer } from "effect";
import { authorize } from "./auth/authorize.js";
import {
  type AuthRuntime,
  type AuthRuntimeDebugInfo,
  type AuthRuntimeLogger,
  type AuthRuntimeOptions,
  authErrorFromUnknown,
  authRuntimeOptionsFromConfig,
  createAuthRuntime,
} from "./auth/enforcement.js";
import type { AuthContext } from "./auth/token.js";
import { ServerConfigService } from "./config.js";
import { type DatabasePool, DatabaseService } from "./db.js";
import { HostPresenceRuntime } from "./host-presence.js";
import { handleClientBindingHttpRoute } from "./http-client-binding-route-handlers.js";
import { directHttpRoutes } from "./http-direct-routes.js";
import { matchHttpRoute } from "./http-route-spec.js";
import {
  applyCorsResponseHeaders,
  broadcastEvents,
  type CorsOptions,
  type HttpRouteErrorOptions,
  handleCorsPreflight,
  handleHttpRouteError,
  readCorsOptionsFromEnv,
  sendAuthError,
  sendJson,
} from "./http-route-runtime.js";
import { handleSessionDebugHttpRoute } from "./http-session-debug-route-handlers.js";
import { handleSessionHttpRoute } from "./http-session-route-handlers.js";
import { handleTaskHttpRoute } from "./http-task-route-handlers.js";
import { handleUiHttpRoute } from "./http-ui-route-handlers.js";
import { SubscriptionHub, type SubscriptionHubDebugInfo } from "./hub.js";
import {
  defaultResourceLimits,
  type ResourceLimitDebugInfo,
  ResourceLimitRuntime,
  type ResourceLimits,
} from "./resource-limits.js";
import type { RestControlPolicyDebugInfo } from "./rest-control-policy.js";
import { SessionEventFanout, type SessionEventFanoutDebugInfo } from "./session-event-fanout.js";
import {
  createSessionServiceEffect,
  type SessionServiceDebugInfo,
  type SessionServiceEffect,
  SessionServiceEffectService,
  type SessionServiceOptions,
} from "./session-service.js";
import {
  TaskClaimSweeper,
  type TaskClaimSweeperConfig,
  type TaskClaimSweeperDebugInfo,
} from "./task-claim-sweeper.js";
import { createParticipantWebSocketGateway } from "./websocket-participant-gateway.js";

/** Allows compact participant contract advertisements to fit in WebSocket URLs. */
const participantStreamMaxHeaderSizeBytes = 128 * 1024;
const consoleAuthRuntimeLogger: AuthRuntimeLogger = {
  warn: (event, details) => {
    console.warn(event, details);
  },
};

/**
 * Process-local HTTP/WebSocket server boundary.
 */
export interface AppServer {
  /** Stops WebSocket, scheduler, fanout, and HTTP resources. */
  readonly close: () => Promise<void>;
  /** Returns process-local diagnostics without mutating durable session state. */
  readonly debugInfo: () => AppServerDebugInfo;
  /** Starts Postgres fanout, HTTP/WebSocket serving, and task claim sweeping. */
  readonly listen: (port: number) => Promise<void>;
}

/**
 * Runtime diagnostics for one app server replica and its child modules.
 */
export interface AppServerDebugInfo {
  /** Authentication mode and accepted key diagnostics, without secrets. */
  readonly auth: AuthRuntimeDebugInfo;
  /** Cross-replica event fanout listener and catch-up diagnostics. */
  readonly eventFanout: SessionEventFanoutDebugInfo;
  /** Local WebSocket subscription hub diagnostics. */
  readonly hub: SubscriptionHubDebugInfo;
  /** Process-local resource limit configuration and hit counters. */
  readonly resourceLimits: ResourceLimitDebugInfo;
  /** Durable session service boundary diagnostics and configured TTLs. */
  readonly service: SessionServiceDebugInfo;
  /** Task claim expiration scheduler diagnostics. */
  readonly taskClaimSweeper: TaskClaimSweeperDebugInfo;
  /** Process-local Host-presence stream diagnostics. */
  readonly hostPresence: ReturnType<HostPresenceRuntime["debugInfo"]>;
}

/**
 * Optional app server configuration for process-local modules.
 */
export interface AppServerOptions {
  /** Authentication configuration for direct app-server construction. */
  readonly auth?: AuthRuntimeOptions;
  /** Cross-replica event fanout listener configuration. */
  readonly eventFanout?: {
    readonly catchUpPollIntervalMs?: number;
    readonly listenEnabled?: boolean;
  };
  /** Exact browser origins allowed to call REST routes with credentials. */
  readonly cors?: CorsOptions;
  /** Optional dependencies for generic HTTP route error redaction and logging. */
  readonly httpRouteErrors?: HttpRouteErrorOptions;
  /** Process-local resource limits for transports, replay, fanout, and hub sends. */
  readonly resourceLimits?: ResourceLimits;
  /** Structured logger for one compatibility-mode startup warning. */
  readonly restControlLogger?: {
    readonly warn: (
      event: "rest_control.compatibility_enabled",
      details: { readonly mode: "compatibility"; readonly warningCode: string },
    ) => void;
  };
  /** Durable session service configuration. */
  readonly sessionService?: SessionServiceOptions;
  /** Task claim expiration scheduler configuration. */
  readonly taskClaimSweeper?: TaskClaimSweeperConfig;
}

/**
 * Effect service tag for the live HTTP/WebSocket app server.
 */
export class AppServerService extends Context.Tag("tether/AppServer")<
  AppServerService,
  AppServer
>() {}

/**
 * Live HTTP/WebSocket app-server layer. It starts fanout, serving, and claim
 * sweeping when acquired and closes all app-server resources when interrupted.
 */
export const AppServerLive = Layer.scoped(
  AppServerService,
  Effect.gen(function* () {
    const config = yield* ServerConfigService;
    const pool = yield* DatabaseService;
    const service = yield* SessionServiceEffectService;
    const server = createAppServerWithSessionService(pool, service, {
      eventFanout: {
        catchUpPollIntervalMs: config.eventFanoutCatchUpPollMs,
      },
      resourceLimits: config.resourceLimits,
      taskClaimSweeper: {
        batchSize: config.taskClaimSweepBatchSize,
        intervalMs: config.taskClaimSweepMs,
      },
      auth: {
        ...authRuntimeOptionsFromConfig(config),
        logger: consoleAuthRuntimeLogger,
      },
    });
    yield* Effect.acquireRelease(
      Effect.tryPromise(async () => {
        await server.listen(config.port);
        console.log(`tether listening on :${config.port}`);
        return server;
      }),
      (appServer) => Effect.promise(() => appServer.close()),
    );
    return server;
  }),
);

type ReadAppServerDebugInfo = () => AppServerDebugInfo;

/**
 * Creates the HTTP and WebSocket server boundary around the durable session
 * service, subscription hub, and task claim sweeper.
 */
export function createAppServer(pool: DatabasePool, options: AppServerOptions = {}): AppServer {
  return createAppServerWithSessionService(
    pool,
    createDefaultSessionServiceEffect(pool, options),
    options,
  );
}

/**
 * Creates the HTTP and WebSocket server boundary around an already-constructed
 * durable session service. This is the Effect-layer seam for live wiring while
 * tests can still use createAppServer(pool).
 */
export function createAppServerWithSessionService(
  pool: DatabasePool,
  service: SessionServiceEffect,
  options: AppServerOptions = {},
): AppServer {
  const resourceLimitRuntime = new ResourceLimitRuntime(
    options.resourceLimits ?? defaultResourceLimits,
  );
  const hub = new SubscriptionHub({
    limits: resourceLimitRuntime.limits,
  });
  const corsOptions = options.cors ?? readCorsOptionsFromEnv();
  const hostPresence = new HostPresenceRuntime();
  const eventFanout = new SessionEventFanout({
    ...(options.eventFanout ?? {}),
    database: pool,
    eventBatchLimit: resourceLimitRuntime.limits.eventFanoutBatchLimit,
    hub,
    service,
  });
  const taskClaimSweeper = new TaskClaimSweeper({
    ...(options.taskClaimSweeper ?? {}),
    onEvents: (events) => broadcastEvents(hub, events),
    service,
  });
  const auth = createAuthRuntime(
    options.auth ?? {
      activeKid: "disabled",
      mode: "disabled",
      secrets: {},
    },
  );
  /**
   * Reads process-local diagnostics for the app server and its child modules.
   */
  const readDebugInfo = (): AppServerDebugInfo => ({
    auth: auth.debugInfo(),
    eventFanout: eventFanout.debugInfo(),
    hub: hub.debugInfo(),
    resourceLimits: resourceLimitRuntime.debugInfo(),
    service: service.debugInfo(),
    taskClaimSweeper: taskClaimSweeper.debugInfo(),
    hostPresence: hostPresence.debugInfo(),
  });
  if (service.debugInfo().restControl?.mode === "compatibility") {
    (options.restControlLogger ?? defaultRestControlLogger).warn(
      "rest_control.compatibility_enabled",
      {
        mode: "compatibility",
        warningCode: "REST_CONTROL_COMPATIBILITY_ENABLED",
      },
    );
  }
  const server = createServer(
    { maxHeaderSize: participantStreamMaxHeaderSizeBytes },
    (request, response) => {
      void Effect.runPromise(
        handleHttp(
          service,
          hub,
          auth,
          resourceLimitRuntime,
          readDebugInfo,
          corsOptions,
          hostPresence,
          request,
          response,
          options.httpRouteErrors,
        ),
      );
    },
  );
  const wsServer = createParticipantWebSocketGateway({
    auth,
    hub,
    resourceLimitRuntime,
    server,
    service,
    hostPresence,
  });

  return {
    close: async () => {
      await taskClaimSweeper.stop();
      await eventFanout.stop();
      auth.close();
      await new Promise<void>((resolve, reject) => {
        wsServer.close((wsError) => {
          if (wsError) {
            reject(wsError);
            return;
          }
          server.close((serverError) => {
            if (serverError) {
              reject(serverError);
              return;
            }
            resolve();
          });
        });
      });
    },
    debugInfo: readDebugInfo,
    listen: async (port) => {
      await eventFanout.start();
      await new Promise<void>((resolve) => {
        server.listen(port, resolve);
      });
      taskClaimSweeper.start();
    },
  };
}

/** Builds the default Effect session service used by the HTTP app boundary. */
function createDefaultSessionServiceEffect(
  pool: DatabasePool,
  options: AppServerOptions = {},
): SessionServiceEffect {
  return createSessionServiceEffect(pool, {
    ...options.sessionService,
  });
}

/**
 * Routes REST requests to session, participant, event, and task service
 * operations.
 */
function handleHttp(
  service: SessionServiceEffect,
  hub: SubscriptionHub,
  auth: AuthRuntime,
  resourceLimitRuntime: ResourceLimitRuntime,
  readAppServerDebugInfo: ReadAppServerDebugInfo,
  corsOptions: CorsOptions,
  hostPresence: HostPresenceRuntime,
  request: IncomingMessage,
  response: ServerResponse,
  errorOptions: HttpRouteErrorOptions = {},
): Effect.Effect<void, unknown> {
  return handleHttpRequest(
    service,
    hub,
    auth,
    resourceLimitRuntime,
    readAppServerDebugInfo,
    corsOptions,
    hostPresence,
    request,
    response,
  ).pipe(
    Effect.catchAll((error) =>
      Effect.sync(() =>
        handleHttpRouteError(response, error, {
          ...errorOptions,
          resourceLimitRuntime,
        }),
      ),
    ),
  );
}

/**
 * Routes one REST request as an Effect program while preserving the existing
 * response shape and error handling contract.
 */
function handleHttpRequest(
  service: SessionServiceEffect,
  hub: SubscriptionHub,
  auth: AuthRuntime,
  resourceLimitRuntime: ResourceLimitRuntime,
  readAppServerDebugInfo: ReadAppServerDebugInfo,
  corsOptions: CorsOptions,
  hostPresence: HostPresenceRuntime,
  request: IncomingMessage,
  response: ServerResponse,
): Effect.Effect<void, unknown> {
  return Effect.gen(function* () {
    const url = request.url ? new URL(request.url, "http://localhost") : null;
    if (!url) {
      sendJson(response, 400, { error: "Missing request URL" });
      return;
    }
    if (handleCorsPreflight(request, response, corsOptions)) {
      return;
    }
    applyCorsResponseHeaders(request, response, corsOptions);
    if (matchHttpRoute(directHttpRoutes.health, request.method, url.pathname)) {
      const restControl = readAppServerDebugInfo().service.restControl;
      sendJson(response, 200, projectHealthResponse(restControl));
      return;
    }
    const authContext = authenticateHttpRequest(auth, request, response, url);
    if (authContext === undefined) {
      return;
    }
    if (matchHttpRoute(directHttpRoutes.ui, request.method, url.pathname)) {
      if (!authorizeHttp(response, authContext, { action: "admin" })) {
        return;
      }
      handleUiHttpRoute({ request, response, url });
      return;
    }
    if (handleUiHttpRoute({ request, response, url })) {
      return;
    }
    if (matchHttpRoute(directHttpRoutes.serverDebug, request.method, url.pathname)) {
      if (!authorizeHttp(response, authContext, { action: "admin" })) {
        return;
      }
      sendJson(response, 200, { server: readAppServerDebugInfo() });
      return;
    }
    if (
      yield* handleClientBindingHttpRoute({
        authContext,
        hub,
        request,
        resourceLimits: resourceLimitRuntime.limits,
        response,
        service,
        url,
      })
    ) {
      return;
    }
    if (
      yield* handleSessionDebugHttpRoute({
        authContext,
        request,
        response,
        service,
        url,
      })
    ) {
      return;
    }
    if (
      yield* handleSessionHttpRoute({
        authContext,
        hub,
        request,
        resourceLimits: resourceLimitRuntime.limits,
        response,
        service,
        hostPresence,
        url,
      })
    ) {
      return;
    }

    if (
      yield* handleTaskHttpRoute({
        authContext,
        hub,
        request,
        resourceLimits: resourceLimitRuntime.limits,
        response,
        service,
        url,
      })
    ) {
      return;
    }

    sendJson(response, 404, { error: "Not found" });
  });
}

/** Projects REST control mode into a ready health response with bounded warnings. */
export function projectHealthResponse(
  restControl: RestControlPolicyDebugInfo | undefined,
): Record<string, unknown> {
  return {
    ok: true,
    ...(restControl?.mode === "compatibility"
      ? { warnings: ["REST_CONTROL_COMPATIBILITY_ENABLED"] }
      : {}),
  };
}

/** Default identity-free logger for the compatibility startup boundary. */
const defaultRestControlLogger = {
  warn: (
    event: "rest_control.compatibility_enabled",
    details: { readonly mode: "compatibility"; readonly warningCode: string },
  ): void => {
    process.stderr.write(`${JSON.stringify({ details, event })}\n`);
  },
};

/** Authenticates a REST request and writes the rejection response on failure. */
function authenticateHttpRequest(
  auth: AuthRuntime,
  request: IncomingMessage,
  response: ServerResponse,
  url: URL,
): AuthContext | null | undefined {
  try {
    return auth.authenticateHttpRequest(request, url);
  } catch (error) {
    sendAuthError(response, authErrorFromUnknown(error));
    return undefined;
  }
}

/** Applies one authorization decision and writes the rejection response on failure. */
function authorizeHttp(
  response: ServerResponse,
  context: AuthContext | null,
  input: {
    readonly action: Parameters<typeof authorize>[0]["action"];
    readonly sessionId?: string | undefined;
  },
): boolean {
  const denied = authorize({ context, ...input });
  if (!denied) {
    return true;
  }
  sendAuthError(response, denied);
  return false;
}
