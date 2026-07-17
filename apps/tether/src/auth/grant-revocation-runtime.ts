import type { EventEmitter } from "node:events";

import type { AuthGrantRevocationStore } from "./grant-stores.js";
import { defaultAuthSocketRegistryMaxSocketCount } from "./socket-registry.js";

/** Dedicated Postgres channel for committed parent-grant revocations. */
export const authGrantRevocationNotificationChannel = "tether_auth_grant_revocations";

/** Default repair interval keeps missed revocations below the five-second bound. */
export const defaultAuthGrantRevocationPollIntervalMs = 1_000;

/** Maximum local parent grants checked during one repair pass. */
export const defaultAuthGrantRevocationPollBatchLimit = 2_000;
const maximumAuthGrantRevocationPropagationMs = 5_000;
const defaultAuthGrantRevocationPollReadTimeoutMs = 1_000;
const defaultAuthGrantRevocationReconnectDelayMs = 100;
const defaultAuthGrantRevocationListenerSetupTimeoutMs = 1_000;

interface RevocationRegistry {
  readonly closeGrant: (
    grantJti: string,
    reason: "auth_grant_expired" | "auth_grant_revoked" | "auth_store_unavailable",
  ) => number;
  readonly grantJtis: () => readonly string[];
}

interface NotificationMessage {
  readonly channel: string;
  readonly payload?: string | undefined;
}

interface ListenerClient extends Pick<EventEmitter, "off" | "on"> {
  readonly query: (
    config: string | { readonly query_timeout: number; readonly text: string },
  ) => Promise<unknown>;
  readonly release: (destroy?: boolean) => void;
}

interface RevocationDatabase {
  readonly pool: {
    readonly connect: () => Promise<ListenerClient>;
  };
}

/** Content-free process diagnostics for notification and polling repair. */
export interface AuthGrantRevocationDebugInfo {
  readonly invalidNotificationCount: number;
  readonly listenerConnected: boolean;
  readonly listenerFailureCount: number;
  readonly notificationCloseCount: number;
  readonly notificationCount: number;
  readonly pollBatchLimit: number;
  readonly pollCheckedGrantCount: number;
  readonly pollCloseCount: number;
  readonly pollCount: number;
  readonly pollFailureCount: number;
  readonly pollIntervalMs: number;
  readonly scheduled: boolean;
}

/** Dependencies for cross-replica parent-grant lifecycle propagation. */
export interface AuthGrantRevocationRuntimeOptions {
  readonly database: RevocationDatabase;
  readonly listenEnabled?: boolean;
  readonly maximumRegisteredGrantCount?: number;
  readonly now?: () => Date;
  readonly pollBatchLimit?: number;
  readonly pollIntervalMs?: number;
  readonly pollReadTimeoutMs?: number;
  readonly registry: RevocationRegistry;
  readonly store: AuthGrantRevocationStore;
}

/**
 * Closes locally registered sockets from committed revocation notifications,
 * with bounded durable polling as the missed-notification repair path.
 */
export class AuthGrantRevocationRuntime {
  readonly #database: RevocationDatabase;
  readonly #listenEnabled: boolean;
  readonly #maximumRegisteredGrantCount: number;
  readonly #now: () => Date;
  readonly #pollBatchLimit: number;
  readonly #pollIntervalMs: number;
  readonly #pollReadTimeoutMs: number;
  readonly #registry: RevocationRegistry;
  readonly #store: AuthGrantRevocationStore;
  #invalidNotificationCount = 0;
  #listener: ListenerClient | null = null;
  #listenerConnecting: Promise<void> | null = null;
  #listenerFailureCount = 0;
  #notificationCloseCount = 0;
  #notificationCount = 0;
  #nextPollIndex = 0;
  #pollCheckedGrantCount = 0;
  #pollCloseCount = 0;
  #pollCount = 0;
  #pollFailureCount = 0;
  #pollTimer: ReturnType<typeof setTimeout> | null = null;
  #polling: Promise<void> | null = null;
  #reconnectTimer: ReturnType<typeof setTimeout> | null = null;
  #started = false;

  constructor(options: AuthGrantRevocationRuntimeOptions) {
    this.#database = options.database;
    this.#listenEnabled = options.listenEnabled ?? true;
    this.#maximumRegisteredGrantCount =
      options.maximumRegisteredGrantCount ?? defaultAuthSocketRegistryMaxSocketCount;
    this.#now = options.now ?? (() => new Date());
    this.#pollBatchLimit = options.pollBatchLimit ?? defaultAuthGrantRevocationPollBatchLimit;
    this.#pollIntervalMs = options.pollIntervalMs ?? defaultAuthGrantRevocationPollIntervalMs;
    this.#pollReadTimeoutMs =
      options.pollReadTimeoutMs ?? defaultAuthGrantRevocationPollReadTimeoutMs;
    this.#registry = options.registry;
    this.#store = options.store;
    if (
      !Number.isSafeInteger(this.#maximumRegisteredGrantCount) ||
      this.#maximumRegisteredGrantCount <= 0
    ) {
      throw new Error("auth_revocation_registry_capacity_invalid");
    }
    if (!Number.isSafeInteger(this.#pollBatchLimit) || this.#pollBatchLimit <= 0) {
      throw new Error("auth_revocation_poll_batch_invalid");
    }
    if (!Number.isSafeInteger(this.#pollIntervalMs) || this.#pollIntervalMs < 0) {
      throw new Error("auth_revocation_poll_interval_invalid");
    }
    if (!Number.isSafeInteger(this.#pollReadTimeoutMs) || this.#pollReadTimeoutMs <= 0) {
      throw new Error("auth_revocation_poll_timeout_invalid");
    }
    const rounds = Math.ceil(this.#maximumRegisteredGrantCount / this.#pollBatchLimit);
    const worstCaseRepairMs = rounds * (this.#pollIntervalMs + this.#pollReadTimeoutMs);
    if (this.#pollIntervalMs > 0 && worstCaseRepairMs > maximumAuthGrantRevocationPropagationMs) {
      throw new Error("auth_revocation_poll_coverage_invalid");
    }
  }

  /** Starts bounded polling before asynchronously establishing LISTEN. */
  async start(): Promise<void> {
    if (this.#started) {
      return;
    }
    this.#started = true;
    if (this.#pollIntervalMs > 0) {
      this.#schedulePoll();
    }
    if (this.#listenEnabled) {
      this.#startListenerConnection();
    }
  }

  /** Stops polling and releases the dedicated LISTEN client. */
  async stop(): Promise<void> {
    this.#started = false;
    if (this.#reconnectTimer !== null) {
      clearTimeout(this.#reconnectTimer);
      this.#reconnectTimer = null;
    }
    if (this.#pollTimer !== null) {
      clearTimeout(this.#pollTimer);
      this.#pollTimer = null;
    }
    await this.#polling;
    await this.#listenerConnecting;
    const listener = this.#listener;
    this.#listener = null;
    if (listener !== null) {
      listener.off("notification", this.#handleNotification);
      listener.off("error", this.#handleListenerError);
      try {
        await listener.query({
          query_timeout: defaultAuthGrantRevocationListenerSetupTimeoutMs,
          text: `UNLISTEN ${authGrantRevocationNotificationChannel}`,
        });
      } catch {
        listener.release(true);
        return;
      }
      listener.release();
    }
  }

  /** Runs one serialized, bounded durable repair pass. */
  async pollNow(): Promise<void> {
    if (this.#polling !== null) {
      return this.#polling;
    }
    this.#polling = this.#runPoll();
    try {
      await this.#polling;
    } finally {
      this.#polling = null;
    }
  }

  /** Returns bounded counters without parent grant or socket identities. */
  debugInfo(): AuthGrantRevocationDebugInfo {
    return {
      invalidNotificationCount: this.#invalidNotificationCount,
      listenerConnected: this.#listener !== null,
      listenerFailureCount: this.#listenerFailureCount,
      notificationCloseCount: this.#notificationCloseCount,
      notificationCount: this.#notificationCount,
      pollBatchLimit: this.#pollBatchLimit,
      pollCheckedGrantCount: this.#pollCheckedGrantCount,
      pollCloseCount: this.#pollCloseCount,
      pollCount: this.#pollCount,
      pollFailureCount: this.#pollFailureCount,
      pollIntervalMs: this.#pollIntervalMs,
      scheduled: this.#pollTimer !== null,
    };
  }

  readonly #handleNotification = (message: NotificationMessage): void => {
    if (message.channel !== authGrantRevocationNotificationChannel) {
      return;
    }
    this.#notificationCount += 1;
    const grantJti = parseAuthGrantRevocationNotification(message.payload);
    if (grantJti === null) {
      this.#invalidNotificationCount += 1;
      return;
    }
    this.#notificationCloseCount += this.#registry.closeGrant(grantJti, "auth_grant_revoked");
  };

  readonly #handleListenerError = (): void => {
    this.#listenerFailureCount += 1;
    const listener = this.#detachListener();
    listener?.release(true);
    this.#scheduleReconnect();
  };

  async #runPoll(): Promise<void> {
    this.#pollCount += 1;
    const grantJtis = this.#registry.grantJtis();
    if (grantJtis.length === 0) {
      this.#nextPollIndex = 0;
      return;
    }
    const count = Math.min(this.#pollBatchLimit, grantJtis.length);
    const selected = Array.from(
      { length: count },
      (_, offset) => grantJtis[(this.#nextPollIndex + offset) % grantJtis.length],
    ).filter((jti): jti is string => jti !== undefined);
    this.#nextPollIndex = (this.#nextPollIndex + selected.length) % grantJtis.length;
    this.#pollCheckedGrantCount += selected.length;
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), this.#pollReadTimeoutMs);
    timeout.unref?.();
    let grants: Awaited<ReturnType<AuthGrantRevocationStore["findManyByJti"]>>;
    try {
      grants = await this.#store.findManyByJti(selected, {
        signal: controller.signal,
        timeoutMs: this.#pollReadTimeoutMs,
      });
    } catch {
      this.#pollFailureCount += selected.length;
      for (const grantJti of selected) {
        this.#pollCloseCount += this.#registry.closeGrant(grantJti, "auth_store_unavailable");
      }
      return;
    } finally {
      clearTimeout(timeout);
    }
    const grantsByJti = new Map(grants.map((grant) => [grant.jti, grant]));
    for (const grantJti of selected) {
      const grant = grantsByJti.get(grantJti);
      if (grant === undefined || grant.revokedAt !== null) {
        this.#pollCloseCount += this.#registry.closeGrant(grantJti, "auth_grant_revoked");
      } else if (grant.expiresAt.getTime() <= this.#now().getTime()) {
        this.#pollCloseCount += this.#registry.closeGrant(grantJti, "auth_grant_expired");
      }
    }
  }

  async #connectListener(): Promise<void> {
    if (!this.#started || this.#listener !== null) {
      return;
    }
    let listener: ListenerClient | null = null;
    try {
      const connecting = this.#database.pool.connect();
      try {
        listener = await withTimeout(connecting, defaultAuthGrantRevocationListenerSetupTimeoutMs);
      } catch (error) {
        void connecting.then(
          (lateListener) => lateListener.release(true),
          () => undefined,
        );
        throw error;
      }
      listener.on("notification", this.#handleNotification);
      listener.on("error", this.#handleListenerError);
      await listener.query({
        query_timeout: defaultAuthGrantRevocationListenerSetupTimeoutMs,
        text: `LISTEN ${authGrantRevocationNotificationChannel}`,
      });
      if (!this.#started) {
        this.#detachListenerHandlers(listener);
        listener.release(true);
        return;
      }
      this.#listener = listener;
    } catch {
      this.#listenerFailureCount += 1;
      if (listener !== null) {
        this.#detachListenerHandlers(listener);
        listener.release(true);
      }
      this.#scheduleReconnect();
    }
  }

  #detachListener(): ListenerClient | null {
    const listener = this.#listener;
    this.#listener = null;
    if (listener !== null) {
      this.#detachListenerHandlers(listener);
    }
    return listener;
  }

  #detachListenerHandlers(listener: ListenerClient): void {
    listener.off("notification", this.#handleNotification);
    listener.off("error", this.#handleListenerError);
  }

  #scheduleReconnect(): void {
    if (!this.#started || !this.#listenEnabled || this.#reconnectTimer !== null) {
      return;
    }
    this.#reconnectTimer = setTimeout(() => {
      this.#reconnectTimer = null;
      this.#startListenerConnection();
    }, defaultAuthGrantRevocationReconnectDelayMs);
    this.#reconnectTimer.unref?.();
  }

  #schedulePoll(): void {
    if (!this.#started || this.#pollIntervalMs <= 0 || this.#pollTimer !== null) {
      return;
    }
    this.#pollTimer = setTimeout(() => {
      this.#pollTimer = null;
      void this.pollNow().finally(() => this.#schedulePoll());
    }, this.#pollIntervalMs);
    this.#pollTimer.unref?.();
  }

  #startListenerConnection(): void {
    if (!this.#started || !this.#listenEnabled || this.#listenerConnecting !== null) {
      return;
    }
    const connecting = this.#connectListener();
    this.#listenerConnecting = connecting;
    void connecting.finally(() => {
      if (this.#listenerConnecting === connecting) {
        this.#listenerConnecting = null;
      }
    });
  }
}

/** Serializes one bounded grant identity for transactional Postgres NOTIFY. */
export function serializeAuthGrantRevocationNotification(grantJti: string): string {
  if (grantJti.length === 0 || grantJti.length > 128) {
    throw new Error("auth_grant_jti_invalid");
  }
  return JSON.stringify({ grantJti });
}

/** Parses one exact notification payload without accepting extra fields. */
export function parseAuthGrantRevocationNotification(payload: string | undefined): string | null {
  if (payload === undefined) {
    return null;
  }
  try {
    const parsed: unknown = JSON.parse(payload);
    if (
      typeof parsed !== "object" ||
      parsed === null ||
      Array.isArray(parsed) ||
      Object.keys(parsed).length !== 1 ||
      !("grantJti" in parsed) ||
      typeof parsed.grantJti !== "string" ||
      parsed.grantJti.length === 0 ||
      parsed.grantJti.length > 128
    ) {
      return null;
    }
    return parsed.grantJti;
  } catch {
    return null;
  }
}

async function withTimeout<TValue>(operation: Promise<TValue>, timeoutMs: number): Promise<TValue> {
  let timer: ReturnType<typeof setTimeout> | null = null;
  try {
    return await Promise.race([
      operation,
      new Promise<never>((_resolve, reject) => {
        timer = setTimeout(
          () => reject(new Error("auth_revocation_listener_setup_timeout")),
          timeoutMs,
        );
        timer.unref?.();
      }),
    ]);
  } finally {
    if (timer !== null) {
      clearTimeout(timer);
    }
  }
}
