import { Config, ConfigProvider, Context, Effect, Layer } from "effect";

import {
  defaultResourceLimits,
  parsePositiveResourceLimit,
  type ResourceLimits,
} from "./resource-limits.js";
import { defaultEventFanoutCatchUpPollMs } from "./session-event-fanout.js";
import { defaultTaskClaimSweepBatchSize, defaultTaskClaimSweepMs } from "./task-claim-sweeper.js";

const defaultDatabasePoolMax = 10;
const defaultPort = 3025;
const defaultAuthMode = "required";
const defaultAuthSigningKid = "default";

export type AuthMode = "disabled" | "required";

export interface ServerConfig {
  /** Additional accepted verification secrets keyed by signing key id. */
  readonly authAcceptedSigningSecrets: Readonly<Record<string, string>>;
  /** Whether HTTP and WebSocket auth enforcement is active. */
  readonly authMode: AuthMode;
  /** Active signing key id used by minting tools and diagnostics. */
  readonly authSigningKid: string;
  /** Active signing secret; null only when auth is explicitly disabled. */
  readonly authSigningSecret: string | null;
  /**
   * Whether a control-protected REST request missing its Control Epoch is
   * rejected. Enforcement is the safe default; explicit false selects the
   * temporary migration-release compatibility mode.
   */
  readonly controlEpochEnforcement: boolean;
  /** Maximum number of connections in the Postgres connection pool. */
  readonly databasePoolMax: number;
  readonly databaseUrl: string;
  readonly eventFanoutCatchUpPollMs: number;
  readonly port: number;
  readonly resourceLimits: ResourceLimits;
  readonly taskClaimSweepBatchSize: number;
  readonly taskClaimSweepMs: number;
}

/**
 * Effect service tag for process-level server configuration.
 */
export class ServerConfigService extends Context.Tag("tether/ServerConfig")<
  ServerConfigService,
  ServerConfig
>() {}

/**
 * Effect config descriptor for live server settings. Invalid scheduler values
 * intentionally fall back to defaults to preserve the existing startup
 * contract used by readConfig.
 */
export const serverConfigDescriptor = Config.all({
  authAcceptedSigningSecrets: Config.string("AUTH_ACCEPTED_SIGNING_SECRETS").pipe(
    Config.orElse(() => Config.succeed("{}")),
    Config.mapAttempt(parseAuthAcceptedSigningSecrets),
  ),
  authMode: Config.literal(
    "required",
    "disabled",
  )("AUTH_MODE").pipe(Config.orElse(() => Config.succeed(defaultAuthMode as AuthMode))),
  authSigningKid: Config.string("AUTH_SIGNING_KID").pipe(
    Config.orElse(() => Config.succeed(defaultAuthSigningKid)),
  ),
  authSigningSecret: Config.string("AUTH_SIGNING_SECRET").pipe(
    Config.orElse(() => Config.succeed("")),
  ),
  controlEpochEnforcement: Config.string("CONTROL_EPOCH_ENFORCEMENT").pipe(
    Config.orElse(() => Config.succeed("")),
    Config.mapAttempt(parseBooleanFlag),
  ),
  databasePoolMax: positiveIntegerConfig("DATABASE_POOL_MAX", defaultDatabasePoolMax),
  databaseUrl: Config.string("DATABASE_URL").pipe(Config.orElse(() => Config.succeed(""))),
  eventFanoutCatchUpPollMs: nonNegativeIntegerConfig(
    "EVENT_FANOUT_CATCH_UP_MS",
    defaultEventFanoutCatchUpPollMs,
  ),
  port: Config.integer("PORT").pipe(Config.orElse(() => Config.succeed(defaultPort))),
  resourceLimits: Config.all({
    eventFanoutBatchLimit: positiveIntegerConfig(
      "EVENT_FANOUT_BATCH_LIMIT",
      defaultResourceLimits.eventFanoutBatchLimit,
    ),
    eventListDefaultLimit: positiveIntegerConfig(
      "EVENT_LIST_DEFAULT_LIMIT",
      defaultResourceLimits.eventListDefaultLimit,
    ),
    eventListMaxLimit: positiveIntegerConfig(
      "EVENT_LIST_MAX_LIMIT",
      defaultResourceLimits.eventListMaxLimit,
    ),
    httpMaxBodyBytes: positiveIntegerConfig(
      "HTTP_MAX_BODY_BYTES",
      defaultResourceLimits.httpMaxBodyBytes,
    ),
    wsBackpressureBufferedBytes: positiveIntegerConfig(
      "WS_BACKPRESSURE_BUFFERED_BYTES",
      defaultResourceLimits.wsBackpressureBufferedBytes,
    ),
    wsMaxPayloadBytes: positiveIntegerConfig(
      "WS_MAX_PAYLOAD_BYTES",
      defaultResourceLimits.wsMaxPayloadBytes,
    ),
    wsMessageRateLimit: positiveIntegerConfig(
      "WS_MESSAGE_RATE_LIMIT",
      defaultResourceLimits.wsMessageRateLimit,
    ),
    wsMessageRateWindowMs: positiveIntegerConfig(
      "WS_MESSAGE_RATE_WINDOW_MS",
      defaultResourceLimits.wsMessageRateWindowMs,
    ),
    wsReplayMaxEvents: positiveIntegerConfig(
      "WS_REPLAY_MAX_EVENTS",
      defaultResourceLimits.wsReplayMaxEvents,
    ),
  }),
  taskClaimSweepBatchSize: positiveIntegerConfig(
    "TASK_CLAIM_SWEEP_BATCH_SIZE",
    defaultTaskClaimSweepBatchSize,
  ),
  taskClaimSweepMs: nonNegativeIntegerConfig("TASK_CLAIM_SWEEP_MS", defaultTaskClaimSweepMs),
}).pipe(Config.mapAttempt(normalizeServerConfig));

/**
 * Loads server configuration through Effect's ConfigProvider service.
 */
export const readConfigEffect: Effect.Effect<ServerConfig> = serverConfigDescriptor.pipe(
  Effect.orDie,
);

/**
 * Loads server configuration from an explicit environment map through Effect's
 * ConfigProvider machinery.
 */
export function readConfigEffectFromEnv(env: NodeJS.ProcessEnv): Effect.Effect<ServerConfig> {
  return readConfigEffect.pipe(Effect.withConfigProvider(ConfigProvider.fromMap(toConfigMap(env))));
}

/**
 * Builds a server config layer from an explicit environment map. Tests use this
 * to exercise the Effect service without mutating process.env.
 */
export function serverConfigLayerFromEnv(env: NodeJS.ProcessEnv): Layer.Layer<ServerConfigService> {
  return Layer.effect(ServerConfigService, readConfigEffectFromEnv(env));
}

/**
 * Live server configuration layer backed by process.env.
 */
export const ServerConfigLive = Layer.effect(ServerConfigService, readConfigEffect);

/**
 * Reads process-level server configuration.
 */
export function readConfig(env: NodeJS.ProcessEnv = process.env): ServerConfig {
  return normalizeServerConfig({
    authAcceptedSigningSecrets: parseAuthAcceptedSigningSecrets(
      env.AUTH_ACCEPTED_SIGNING_SECRETS ?? "{}",
    ),
    authMode: parseAuthMode(env.AUTH_MODE),
    authSigningKid: env.AUTH_SIGNING_KID ?? defaultAuthSigningKid,
    authSigningSecret: env.AUTH_SIGNING_SECRET ?? "",
    controlEpochEnforcement: parseBooleanFlag(env.CONTROL_EPOCH_ENFORCEMENT),
    databasePoolMax: parsePositiveInteger(env.DATABASE_POOL_MAX, defaultDatabasePoolMax),
    databaseUrl: env.DATABASE_URL ?? "",
    eventFanoutCatchUpPollMs: parseNonNegativeInteger(
      env.EVENT_FANOUT_CATCH_UP_MS,
      defaultEventFanoutCatchUpPollMs,
    ),
    port: Number.parseInt(env.PORT ?? String(defaultPort), 10),
    resourceLimits: {
      eventFanoutBatchLimit: parsePositiveResourceLimit(
        env.EVENT_FANOUT_BATCH_LIMIT,
        defaultResourceLimits.eventFanoutBatchLimit,
      ),
      eventListDefaultLimit: parsePositiveResourceLimit(
        env.EVENT_LIST_DEFAULT_LIMIT,
        defaultResourceLimits.eventListDefaultLimit,
      ),
      eventListMaxLimit: parsePositiveResourceLimit(
        env.EVENT_LIST_MAX_LIMIT,
        defaultResourceLimits.eventListMaxLimit,
      ),
      httpMaxBodyBytes: parsePositiveResourceLimit(
        env.HTTP_MAX_BODY_BYTES,
        defaultResourceLimits.httpMaxBodyBytes,
      ),
      wsBackpressureBufferedBytes: parsePositiveResourceLimit(
        env.WS_BACKPRESSURE_BUFFERED_BYTES,
        defaultResourceLimits.wsBackpressureBufferedBytes,
      ),
      wsMaxPayloadBytes: parsePositiveResourceLimit(
        env.WS_MAX_PAYLOAD_BYTES,
        defaultResourceLimits.wsMaxPayloadBytes,
      ),
      wsMessageRateLimit: parsePositiveResourceLimit(
        env.WS_MESSAGE_RATE_LIMIT,
        defaultResourceLimits.wsMessageRateLimit,
      ),
      wsMessageRateWindowMs: parsePositiveResourceLimit(
        env.WS_MESSAGE_RATE_WINDOW_MS,
        defaultResourceLimits.wsMessageRateWindowMs,
      ),
      wsReplayMaxEvents: parsePositiveResourceLimit(
        env.WS_REPLAY_MAX_EVENTS,
        defaultResourceLimits.wsReplayMaxEvents,
      ),
    },
    taskClaimSweepBatchSize: parsePositiveInteger(
      env.TASK_CLAIM_SWEEP_BATCH_SIZE,
      defaultTaskClaimSweepBatchSize,
    ),
    taskClaimSweepMs: parseNonNegativeInteger(env.TASK_CLAIM_SWEEP_MS, defaultTaskClaimSweepMs),
  });
}

interface RawServerConfig {
  readonly authAcceptedSigningSecrets: Readonly<Record<string, string>>;
  readonly authMode: AuthMode;
  readonly authSigningKid: string;
  readonly authSigningSecret: string;
  readonly controlEpochEnforcement: boolean;
  readonly databasePoolMax: number;
  readonly databaseUrl: string;
  readonly eventFanoutCatchUpPollMs: number;
  readonly port: number;
  readonly resourceLimits: ResourceLimits;
  readonly taskClaimSweepBatchSize: number;
  readonly taskClaimSweepMs: number;
}

/** Applies cross-field auth validation after primitive config parsing. */
function normalizeServerConfig(config: RawServerConfig): ServerConfig {
  const authSigningKid = config.authSigningKid.trim() || defaultAuthSigningKid;
  const authSigningSecret = config.authSigningSecret.trim();
  if (config.authMode === "required" && authSigningSecret.length === 0) {
    throw new Error("AUTH_SIGNING_SECRET is required when AUTH_MODE=required");
  }
  const databaseUrl = config.databaseUrl.trim();
  if (databaseUrl.length === 0) {
    throw new Error("DATABASE_URL is required");
  }
  return {
    ...config,
    authSigningKid,
    authSigningSecret: authSigningSecret.length > 0 ? authSigningSecret : null,
    databaseUrl,
    resourceLimits: normalizeResourceLimits(config.resourceLimits),
  };
}

/** Keeps related resource limits coherent after primitive parsing. */
function normalizeResourceLimits(limits: ResourceLimits): ResourceLimits {
  const eventListMaxLimit = Math.max(limits.eventListDefaultLimit, limits.eventListMaxLimit);
  return {
    ...limits,
    eventListMaxLimit,
  };
}

/** Parses the auth enforcement mode from an explicit environment string. */
function parseAuthMode(value: string | undefined): AuthMode {
  if (value === undefined || value === "") {
    return defaultAuthMode;
  }
  if (value === "required" || value === "disabled") {
    return value;
  }
  throw new Error("AUTH_MODE must be required or disabled");
}

/** Parses a JSON object containing accepted signing secrets keyed by kid. */
function parseAuthAcceptedSigningSecrets(value: string): Readonly<Record<string, string>> {
  let parsed: unknown;
  try {
    parsed = JSON.parse(value);
  } catch {
    throw new Error("AUTH_ACCEPTED_SIGNING_SECRETS must be a JSON object");
  }
  if (typeof parsed !== "object" || parsed === null || Array.isArray(parsed)) {
    throw new Error("AUTH_ACCEPTED_SIGNING_SECRETS must be a JSON object");
  }
  const entries = Object.entries(parsed);
  for (const [kid, secret] of entries) {
    if (kid.length === 0 || typeof secret !== "string" || secret.trim().length === 0) {
      throw new Error("AUTH_ACCEPTED_SIGNING_SECRETS must map non-empty kids to secrets");
    }
  }
  return Object.fromEntries(entries) as Readonly<Record<string, string>>;
}

/**
 * Builds an Effect integer config that accepts only non-negative values and
 * falls back on missing, parse, or validation failures.
 */
function nonNegativeIntegerConfig(name: string, fallback: number): Config.Config<number> {
  return Config.integer(name).pipe(
    Config.validate({
      message: `${name} must be a non-negative integer`,
      validation: (value) => value >= 0,
    }),
    Config.orElse(() => Config.succeed(fallback)),
  );
}

/**
 * Builds an Effect integer config that accepts only positive values and falls
 * back on missing, parse, or validation failures.
 */
function positiveIntegerConfig(name: string, fallback: number): Config.Config<number> {
  return Config.integer(name).pipe(
    Config.validate({
      message: `${name} must be a positive integer`,
      validation: (value) => value > 0,
    }),
    Config.orElse(() => Config.succeed(fallback)),
  );
}

/**
 * Converts Node's optional environment map to the flat map required by
 * ConfigProvider.fromMap.
 */
function toConfigMap(env: NodeJS.ProcessEnv): Map<string, string> {
  const entries = Object.entries(env).filter((entry): entry is [string, string] => {
    const [, value] = entry;
    return value !== undefined;
  });
  return new Map(entries);
}

/**
 * Parses the documented boolean tokens shared by direct and Effect config.
 * Absence selects enforced mode; an explicit invalid value fails startup.
 */
function parseBooleanFlag(value: string | undefined): boolean {
  if (value === undefined || value.trim() === "") {
    return true;
  }
  const normalized = value.trim().toLowerCase();
  if (normalized === "true" || normalized === "1" || normalized === "yes" || normalized === "on") {
    return true;
  }
  if (normalized === "false" || normalized === "0" || normalized === "no" || normalized === "off") {
    return false;
  }
  throw new Error("CONTROL_EPOCH_ENFORCEMENT must be a documented boolean");
}

/**
 * Parses a non-negative integer environment value or returns the provided
 * fallback for missing and invalid input.
 */
function parseNonNegativeInteger(value: string | undefined, fallback: number): number {
  if (value === undefined) {
    return fallback;
  }
  const parsed = Number.parseInt(value, 10);
  return Number.isFinite(parsed) && parsed >= 0 ? parsed : fallback;
}

/**
 * Parses a positive integer environment value or returns the provided fallback
 * for missing and invalid input.
 */
function parsePositiveInteger(value: string | undefined, fallback: number): number {
  if (value === undefined) {
    return fallback;
  }
  const parsed = Number.parseInt(value, 10);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : fallback;
}
