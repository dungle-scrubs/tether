import { createServer, type IncomingMessage, type ServerResponse } from "node:http";
import { URL } from "node:url";

import type { SessionScalabilityHealthWarning } from "@dungle-scrubs/tether-protocol";
import { Context, Effect, Layer } from "effect";
import { authorize } from "./auth/authorize.js";
import { createAuthGrantLifecycle, type AuthGrantLifecycle } from "./auth/grant-lifecycle.js";
import { createAuthPersistenceStores } from "./auth/db-grant-stores.js";
import {
  type AuthRuntime,
  type AuthRuntimeDebugInfo,
  type AuthRuntimeLogger,
  type AuthRuntimeOptions,
  authErrorFromUnknown,
  authRuntimeOptionsFromConfig,
  createAuthRuntime,
} from "./auth/enforcement.js";
import {
  AuthGrantRevocationRuntime,
  type AuthGrantRevocationDebugInfo,
} from "./auth/grant-revocation-runtime.js";
import { AuthSocketRegistry, type AuthSocketRegistryDebugInfo } from "./auth/socket-registry.js";
import type { AuthContext } from "./auth/token.js";
import { createAuthTicketLifecycle, type AuthTicketLifecycle } from "./auth/ticket-lifecycle.js";
import { ServerConfigService } from "./config.js";
import type { RuntimeTopology } from "./config.js";
import { type DatabasePool, DatabaseService } from "./db.js";
import { HostPresenceRuntime } from "./host-presence.js";
import { handleClientBindingHttpRoute } from "./http-client-binding-route-handlers.js";
import { handleAuthGrantHttpRoute } from "./http-auth-grant-route-handlers.js";
import { handleAuthTicketHttpRoute } from "./http-auth-ticket-route-handlers.js";
import { directHttpRoutes } from "./http-direct-routes.js";
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
import { matchHttpRoute } from "./http-route-spec.js";
import { handleSessionDebugHttpRoute } from "./http-session-debug-route-handlers.js";
import { handleSessionHttpRoute } from "./http-session-route-handlers.js";
import { handleSessionSummaryHttpRoute } from "./http-session-summary-route-handlers.js";
import { handleTaskHttpRoute } from "./http-task-route-handlers.js";
import { handleUiHttpRoute } from "./http-ui-route-handlers.js";
import { SubscriptionHub, type SubscriptionHubDebugInfo } from "./hub.js";
import {
  defaultResourceLimits,
  type ResourceLimitDebugInfo,
  ResourceLimitRuntime,
  type ResourceLimits,
} from "./resource-limits.js";
import { projectReadiness, type ReadinessProjection } from "./readiness.js";
import type { RestControlPolicyDebugInfo } from "./rest-control-policy.js";
import {
  defaultEventFanoutCatchUpStaleMs,
  SessionEventFanout,
  type SessionEventFanoutDebugInfo,
} from "./session-event-fanout.js";
import {
  createSessionServiceEffect,
  type SessionServiceDebugInfo,
  type SessionServiceEffect,
  SessionServiceEffectService,
  type SessionServiceOptions,
} from "./session-service.js";
import { createSessionSummaryStore, type SessionSummaryStore } from "./session-summary-store.js";
import { sessionScalabilityBaselineWarnings } from "./session-scalability-diagnostics.js";
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
  /** Cross-replica parent-grant revocation propagation diagnostics. */
  readonly authRevocation: AuthGrantRevocationDebugInfo;
  /** Process-local authenticated socket counts and timer ownership. */
  readonly authSockets: AuthSocketRegistryDebugInfo;
  /** Cross-replica event fanout listener and catch-up diagnostics. */
  readonly eventFanout: SessionEventFanoutDebugInfo;
  /** Process-local Host-presence stream diagnostics. */
  readonly hostPresence: ReturnType<HostPresenceRuntime["debugInfo"]>;
  /** Local WebSocket subscription hub diagnostics. */
  readonly hub: SubscriptionHubDebugInfo;
  /** Opaque app-lifetime identity reused from the session service event source. */
  readonly replicaId: string;
  /** Process-local resource limit configuration and hit counters. */
  readonly resourceLimits: ResourceLimitDebugInfo;
  /** Explicit deployment topology used for Replica Scope safety decisions. */
  readonly runtimeTopology: RuntimeTopology;
  /** Durable session service boundary diagnostics and configured TTLs. */
  readonly service: SessionServiceDebugInfo;
  /** Task claim expiration scheduler diagnostics. */
  readonly taskClaimSweeper: TaskClaimSweeperDebugInfo;
}

/**
 * Optional app server configuration for process-local modules.
 */
export interface AppServerOptions {
  /** Authentication configuration for direct app-server construction. */
  readonly auth?: AuthRuntimeOptions;
  /** Parent-grant notification and bounded polling configuration. */
  readonly authRevocation?: {
    readonly listenEnabled?: boolean;
    readonly pollBatchLimit?: number;
    readonly pollIntervalMs?: number;
  };
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
  /** Process readiness thresholds. */
  readonly readiness?: {
    readonly fanoutStaleAfterMs?: number;
  };
  /** Declared deployment topology; direct programmatic construction defaults to single. */
  readonly runtimeTopology?: RuntimeTopology;
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
      readiness: { fanoutStaleAfterMs: config.eventFanoutCatchUpStaleMs },
      runtimeTopology: config.runtimeTopology,
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
type ReadAppReadiness = () => Promise<ReadinessProjection>;

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
  const replicaId = service.debugInfo().eventSourceId;
  const runtimeTopology = options.runtimeTopology ?? "single";
  const fanoutStaleAfterMs =
    options.readiness?.fanoutStaleAfterMs ?? defaultEventFanoutCatchUpStaleMs;
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
  const sessionSummaryStore = createSessionSummaryStore(pool.pool);
  const authOptions = options.auth ?? {
    activeKid: "disabled",
    issuer: null,
    mode: "disabled",
    secrets: {},
  };
  const authPersistenceStores = createAuthPersistenceStores(pool);
  const authGrantStore = authOptions.grantStore ?? authPersistenceStores.grants;
  const auth = createAuthRuntime({
    ...authOptions,
    grantStore: authGrantStore,
    ticketStore: authOptions.ticketStore ?? authPersistenceStores.tickets,
  });
  const authSocketRegistry = new AuthSocketRegistry();
  const authRevocation = new AuthGrantRevocationRuntime({
    database: pool,
    ...(options.authRevocation ?? {}),
    registry: authSocketRegistry,
    store: authPersistenceStores.grants,
  });
  const authGrantLifecycle = createAuthGrantLifecycle({
    activeKid: authOptions.activeKid,
    issuer: authOptions.issuer ?? null,
    issuanceEnabled: authOptions.preEnforcementGrantIssuanceEnabled ?? false,
    secrets: authOptions.secrets,
    stores: authPersistenceStores,
  });
  const authTicketLifecycle = createAuthTicketLifecycle({
    replicaId: `replica_${replicaId}`,
    store: authPersistenceStores.tickets,
  });
  /**
   * Reads process-local diagnostics for the app server and its child modules.
   */
  const readDebugInfo = (): AppServerDebugInfo => ({
    auth: auth.debugInfo(),
    authRevocation: authRevocation.debugInfo(),
    authSockets: authSocketRegistry.debugInfo(),
    eventFanout: eventFanout.debugInfo(),
    hostPresence: hostPresence.debugInfo(),
    hub: hub.debugInfo(),
    replicaId,
    resourceLimits: resourceLimitRuntime.debugInfo(),
    runtimeTopology,
    service: service.debugInfo(),
    taskClaimSweeper: taskClaimSweeper.debugInfo(),
  });
  const readReadiness: ReadAppReadiness = () =>
    projectReadiness({
      database: pool,
      fanout: eventFanout,
      fanoutStaleAfterMs,
      replicaId,
      runtimeTopology,
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
          authGrantLifecycle,
          authTicketLifecycle,
          resourceLimitRuntime,
          sessionSummaryStore,
          readDebugInfo,
          readReadiness,
          corsOptions,
          hostPresence,
          replicaId,
          runtimeTopology,
          request,
          response,
          options.httpRouteErrors,
        ),
      );
    },
  );
  const wsServer = createParticipantWebSocketGateway({
    auth,
    authSocketRegistry,
    hostPresence,
    hub,
    replicaId,
    resourceLimitRuntime,
    server,
    service,
  });

  return {
    close: async () => {
      await taskClaimSweeper.stop();
      await eventFanout.stop();
      await authRevocation.stop();
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
      await authRevocation.start();
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
  authGrantLifecycle: AuthGrantLifecycle,
  authTicketLifecycle: AuthTicketLifecycle,
  resourceLimitRuntime: ResourceLimitRuntime,
  sessionSummaryStore: SessionSummaryStore,
  readAppServerDebugInfo: ReadAppServerDebugInfo,
  readAppReadiness: ReadAppReadiness,
  corsOptions: CorsOptions,
  hostPresence: HostPresenceRuntime,
  replicaId: string,
  runtimeTopology: RuntimeTopology,
  request: IncomingMessage,
  response: ServerResponse,
  errorOptions: HttpRouteErrorOptions = {},
): Effect.Effect<void, unknown> {
  return handleHttpRequest(
    service,
    hub,
    auth,
    authGrantLifecycle,
    authTicketLifecycle,
    resourceLimitRuntime,
    sessionSummaryStore,
    readAppServerDebugInfo,
    readAppReadiness,
    corsOptions,
    hostPresence,
    replicaId,
    runtimeTopology,
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
  authGrantLifecycle: AuthGrantLifecycle,
  authTicketLifecycle: AuthTicketLifecycle,
  resourceLimitRuntime: ResourceLimitRuntime,
  sessionSummaryStore: SessionSummaryStore,
  readAppServerDebugInfo: ReadAppServerDebugInfo,
  readAppReadiness: ReadAppReadiness,
  corsOptions: CorsOptions,
  hostPresence: HostPresenceRuntime,
  replicaId: string,
  runtimeTopology: RuntimeTopology,
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
      const scalabilityWarnings =
        typeof service.readScalabilityHealthWarnings === "function"
          ? yield* service.readScalabilityHealthWarnings()
          : sessionScalabilityBaselineWarnings;
      sendJson(response, 200, projectHealthResponse(restControl, scalabilityWarnings));
      return;
    }
    if (matchHttpRoute(directHttpRoutes.readiness, request.method, url.pathname)) {
      const readiness = yield* Effect.promise(readAppReadiness);
      sendJson(response, readiness.status, { ...readiness.body });
      return;
    }
    const authContext = yield* Effect.promise(() =>
      authenticateHttpRequest(auth, request, response, url),
    );
    if (authContext === undefined) {
      return;
    }
    if (
      yield* handleAuthTicketHttpRoute({
        authContext,
        lifecycle: authTicketLifecycle,
        request,
        response,
        url,
      })
    ) {
      return;
    }
    if (
      yield* handleAuthGrantHttpRoute({
        authContext,
        lifecycle: authGrantLifecycle,
        maxBodyBytes: resourceLimitRuntime.limits.httpMaxBodyBytes,
        request,
        response,
        url,
      })
    ) {
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
        hostPresence,
        replicaId,
        request,
        resourceLimits: resourceLimitRuntime.limits,
        response,
        runtimeTopology,
        service,
        url,
      })
    ) {
      return;
    }

    if (
      yield* handleSessionSummaryHttpRoute({
        authContext,
        request,
        resourceLimits: resourceLimitRuntime.limits,
        response,
        store: sessionSummaryStore,
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
  scalabilityWarnings: readonly SessionScalabilityHealthWarning[] = sessionScalabilityBaselineWarnings,
): Record<string, unknown> {
  return {
    ok: true,
    warnings: [
      ...scalabilityWarnings,
      ...(restControl?.mode === "compatibility"
        ? (["REST_CONTROL_COMPATIBILITY_ENABLED"] as const)
        : []),
    ],
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
async function authenticateHttpRequest(
  auth: AuthRuntime,
  request: IncomingMessage,
  response: ServerResponse,
  url: URL,
): Promise<AuthContext | null | undefined> {
  try {
    return await auth.authenticateHttpRequest(request, url);
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
