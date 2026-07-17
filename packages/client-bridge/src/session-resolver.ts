import { clientBridgeRoutes } from "./routes.js";
import {
  clientSessionBindingResponseSchema,
  clientSessionBindingsResponseSchema,
} from "./schemas.js";
import { type ClientBridgeTransport, createClientBridgeTransport } from "./transport.js";
import type {
  ClientBridgeResolveSessionResult,
  ClientBridgeSessionBinding,
  ClientBridgeSessionResolverClock,
  ClientBridgeSessionResolverConfig,
  ClientBridgeSessionResolverDebugInfo,
  ClientBridgeSessionResolverOptions,
  ClientBridgeSessionResolverOutcome,
} from "./types.js";

/** Default number of external-id → session-id entries retained by the cache. */
const DEFAULT_CACHE_CAPACITY = 1_024;
/** Default idle time-to-live before an untouched cache entry is evicted. */
const DEFAULT_IDLE_TTL_MS = 1_800_000;
/** Default cap on distinct concurrent in-flight resolutions. */
const DEFAULT_MAX_IN_FLIGHT = 64;

/** One bounded cache entry tracking recency for deterministic idle expiry. */
interface ResolverCacheEntry {
  /** Epoch-millisecond timestamp of the most recent read or write. */
  lastAccessedAt: number;
  /** Durable Tether session id bound to the external id. */
  readonly sessionId: string;
}

/**
 * Raised when a new distinct resolution would exceed the resolver's in-flight
 * cap. The offending request is never issued; callers should back off or retry.
 */
export class ClientBridgeSessionResolverResourceLimitError extends Error {
  /** Distinct in-flight cap that was reached when this error was raised. */
  readonly maxInFlight: number;

  /** Captures an in-flight-resolution overflow with its enforced cap. */
  constructor(input: { readonly maxInFlight: number }) {
    super(
      `Client bridge session resolver exceeded its in-flight resolution limit of ${input.maxInFlight}`,
    );
    this.maxInFlight = input.maxInFlight;
    this.name = "ClientBridgeSessionResolverResourceLimitError";
  }
}

/**
 * Validates a resolver bound and returns either the caller's value or the
 * default, rejecting anything that would disable or unbound the resolver.
 */
function resolvePositiveBound(input: {
  readonly defaultValue: number;
  readonly integer: boolean;
  readonly label: string;
  readonly value: number | undefined;
}): number {
  if (input.value === undefined) {
    return input.defaultValue;
  }
  if (
    !Number.isFinite(input.value) ||
    input.value <= 0 ||
    (input.integer && !Number.isInteger(input.value))
  ) {
    throw new RangeError(
      `${input.label} must be a positive finite ${input.integer ? "integer" : "number"}`,
    );
  }
  return input.value;
}

/**
 * Resolves provider-specific conversations to durable Tether sessions. Client
 * bridges use this instead of duplicating `/client-bindings` REST calls,
 * response validation, and in-process session-id caching.
 *
 * The resolver is deterministically bounded: it caches at most `cacheCapacity`
 * entries under LRU eviction, expires entries after `idleTtlMs` of idle time,
 * deduplicates same-key resolutions, and caps distinct in-flight resolutions at
 * `maxInFlight`. None of these bounds can be disabled.
 */
export class ClientBridgeSessionResolver {
  private readonly cache = new Map<string, ResolverCacheEntry>();
  private readonly cacheCapacity: number;
  private readonly clock: ClientBridgeSessionResolverClock;
  private currentSessionId: string | null;
  private evictionCount = 0;
  private expiryCount = 0;
  private readonly idleTtlMs: number;
  private readonly inFlight = new Map<string, Promise<ClientBridgeResolveSessionResult>>();
  private lastOutcome: ClientBridgeSessionResolverOutcome | null = null;
  private readonly maxInFlight: number;
  private readonly transport: ClientBridgeTransport;

  /** Creates a resolver for one external provider. */
  constructor(
    private readonly config: ClientBridgeSessionResolverConfig,
    options: ClientBridgeSessionResolverOptions = {},
  ) {
    this.cacheCapacity = resolvePositiveBound({
      defaultValue: DEFAULT_CACHE_CAPACITY,
      integer: true,
      label: "cacheCapacity",
      value: config.cacheCapacity,
    });
    this.idleTtlMs = resolvePositiveBound({
      defaultValue: DEFAULT_IDLE_TTL_MS,
      integer: false,
      label: "idleTtlMs",
      value: config.idleTtlMs,
    });
    this.maxInFlight = resolvePositiveBound({
      defaultValue: DEFAULT_MAX_IN_FLIGHT,
      integer: true,
      label: "maxInFlight",
      value: config.maxInFlight,
    });
    this.clock = options.clock ?? { now: () => Date.now() };
    this.currentSessionId = config.defaultSessionId ?? null;
    this.transport = createClientBridgeTransport({
      ...(config.authToken === undefined ? {} : { authToken: config.authToken }),
      ...(options.fetch === undefined ? {} : { fetch: options.fetch }),
      serviceUrl: config.serviceUrl,
    });
  }

  /** Returns inspectable resolver state for bridge debug surfaces. */
  debugInfo(): ClientBridgeSessionResolverDebugInfo {
    return {
      cacheCapacity: this.cacheCapacity,
      cacheSize: this.cache.size,
      currentSessionId: this.currentSessionId,
      evictionCount: this.evictionCount,
      expiryCount: this.expiryCount,
      inFlightCount: this.inFlight.size,
      lastOutcome: this.lastOutcome,
      maxInFlight: this.maxInFlight,
      requestCount: this.transport.debugInfo().requestCount,
    };
  }

  /** Removes exactly one external-id mapping and records the invalidation. */
  invalidate(externalId: string): void {
    this.cache.delete(externalId);
    this.lastOutcome = "invalidated";
  }

  /** Lists active bindings for this resolver's provider and seeds the local session cache. */
  async listBindings(): Promise<readonly ClientBridgeSessionBinding[]> {
    const body = await this.transport.requestJson({
      body: null,
      method: "GET",
      path: clientBridgeRoutes.clientBindings(this.config.provider),
      schema: clientSessionBindingsResponseSchema,
    });
    for (const binding of body.bindings) {
      this.admit(binding.externalId, binding.sessionId, "seeded");
    }
    return body.bindings;
  }

  /** Returns a cached session id or resolves/creates a binding for the external id. */
  async ensureSessionId(externalId: string): Promise<string> {
    const cachedSessionId = this.lookup(externalId);
    if (cachedSessionId !== null) {
      this.currentSessionId = cachedSessionId;
      return cachedSessionId;
    }
    const result = await this.resolveSession(externalId);
    return result.session.sessionId;
  }

  /** Resolves or creates a binding for one provider conversation. */
  resolveSession(externalId: string): Promise<ClientBridgeResolveSessionResult> {
    const existing = this.inFlight.get(externalId);
    if (existing !== undefined) {
      this.lastOutcome = "in-flight-joined";
      return existing;
    }
    if (this.inFlight.size >= this.maxInFlight) {
      this.lastOutcome = "in-flight-overflow";
      return Promise.reject(
        new ClientBridgeSessionResolverResourceLimitError({ maxInFlight: this.maxInFlight }),
      );
    }
    const pending = this.performResolve(externalId);
    this.inFlight.set(externalId, pending);
    return pending;
  }

  /** Issues one resolution request and always clears its in-flight entry. */
  private async performResolve(externalId: string): Promise<ClientBridgeResolveSessionResult> {
    try {
      const body = await this.transport.requestJson({
        body: {
          externalId,
          provider: this.config.provider,
          ...(this.config.defaultSessionId ? { sessionId: this.config.defaultSessionId } : {}),
        },
        method: "POST",
        path: clientBridgeRoutes.clientBindingSession(),
        schema: clientSessionBindingResponseSchema,
      });
      this.currentSessionId = body.session.sessionId;
      this.admit(externalId, body.session.sessionId, "resolved");
      return body;
    } finally {
      this.inFlight.delete(externalId);
    }
  }

  /**
   * Admits one external-id → session-id mapping through the single bounded path.
   * Marks the key most-recently-used and evicts the least-recently-used entry
   * when the write pushes the cache beyond capacity.
   */
  private admit(externalId: string, sessionId: string, outcome: "resolved" | "seeded"): void {
    this.cache.delete(externalId);
    this.cache.set(externalId, { lastAccessedAt: this.clock.now(), sessionId });
    if (this.cache.size > this.cacheCapacity) {
      const lruKey = this.cache.keys().next().value;
      if (lruKey !== undefined) {
        this.cache.delete(lruKey);
      }
      this.evictionCount += 1;
      this.lastOutcome = "evicted";
      return;
    }
    this.lastOutcome = outcome;
  }

  /**
   * Reads one cache entry, applying idle expiry and refreshing recency on a hit.
   * Returns the session id on a live hit, or null on a miss or expiry.
   */
  private lookup(externalId: string): string | null {
    const entry = this.cache.get(externalId);
    if (entry === undefined) {
      return null;
    }
    const now = this.clock.now();
    if (now - entry.lastAccessedAt > this.idleTtlMs) {
      this.cache.delete(externalId);
      this.expiryCount += 1;
      this.lastOutcome = "expired";
      return null;
    }
    this.cache.delete(externalId);
    entry.lastAccessedAt = now;
    this.cache.set(externalId, entry);
    this.lastOutcome = "cache-hit";
    return entry.sessionId;
  }
}
