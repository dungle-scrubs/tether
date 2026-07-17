import type { AuthContext } from "./token.js";

/** Authenticated WebSocket paths sharing parent-grant lifecycle enforcement. */
export type AuthSocketStreamKind = "host" | "observer" | "participant" | "viewer";

/** Minimal close surface needed by the process-local registry. */
export interface AuthRegistrySocket {
  readonly close: (code: number, reason: string) => void;
}

/** Content-free process-local socket lifecycle diagnostics. */
export interface AuthSocketRegistryDebugInfo {
  readonly closeCount: number;
  readonly grantCount: number;
  readonly maxSocketCount: number;
  readonly socketCount: number;
  readonly socketsByStream: Readonly<Record<AuthSocketStreamKind, number>>;
  readonly timerCount: number;
}

interface AuthSocketEntry {
  readonly grantJti: string | null;
  readonly socket: AuthRegistrySocket;
  readonly streamKind: AuthSocketStreamKind;
  timer: ReturnType<typeof setTimeout> | null;
}

/** Dependencies for deterministic expiry testing. */
export interface AuthSocketRegistryOptions {
  readonly maxSocketCount?: number;
  readonly now?: () => number;
}

/** Default process bound used to make revocation polling convergence finite. */
export const defaultAuthSocketRegistryMaxSocketCount = 2_000;
const maximumTimerDelayMs = 2_147_483_647;

/**
 * Owns authenticated sockets, their exact parent-expiry timers, and the
 * process-local index used by revocation notification and polling.
 */
export class AuthSocketRegistry {
  readonly #byGrant = new Map<string, Set<AuthRegistrySocket>>();
  readonly #entries = new Map<AuthRegistrySocket, AuthSocketEntry>();
  readonly #maxSocketCount: number;
  readonly #now: () => number;
  #closeCount = 0;

  constructor(options: AuthSocketRegistryOptions = {}) {
    this.#maxSocketCount = options.maxSocketCount ?? defaultAuthSocketRegistryMaxSocketCount;
    this.#now = options.now ?? Date.now;
    if (!Number.isSafeInteger(this.#maxSocketCount) || this.#maxSocketCount <= 0) {
      throw new Error("auth_socket_capacity_invalid");
    }
  }

  /** Registers one authenticated stream and returns idempotent cleanup. */
  register(input: {
    readonly context: AuthContext;
    readonly socket: AuthRegistrySocket;
    readonly streamKind: AuthSocketStreamKind;
  }): () => void {
    this.unregister(input.socket);
    if (this.#entries.size >= this.#maxSocketCount) {
      input.socket.close(1013, "auth_socket_capacity");
      return () => undefined;
    }
    const expiresAt = Date.parse(input.context.expiresAt);
    if (!Number.isFinite(expiresAt)) {
      input.socket.close(1008, "auth_claim_invalid");
      return () => undefined;
    }
    const entry: AuthSocketEntry = {
      grantJti: input.context.grantJti,
      socket: input.socket,
      streamKind: input.streamKind,
      timer: null,
    };
    this.#entries.set(input.socket, entry);
    if (entry.grantJti !== null) {
      const sockets = this.#byGrant.get(entry.grantJti) ?? new Set<AuthRegistrySocket>();
      sockets.add(input.socket);
      this.#byGrant.set(entry.grantJti, sockets);
    }
    this.#scheduleExpiry(entry, expiresAt);
    return () => this.unregister(input.socket);
  }

  /** Removes one socket and clears its owned expiry timer. */
  unregister(socket: AuthRegistrySocket): void {
    const entry = this.#entries.get(socket);
    if (entry === undefined) {
      return;
    }
    this.#entries.delete(socket);
    if (entry.timer !== null) {
      clearTimeout(entry.timer);
      entry.timer = null;
    }
    if (entry.grantJti === null) {
      return;
    }
    const sockets = this.#byGrant.get(entry.grantJti);
    sockets?.delete(socket);
    if (sockets?.size === 0) {
      this.#byGrant.delete(entry.grantJti);
    }
  }

  /** Closes and unregisters every local socket bound to one parent grant. */
  closeGrant(
    grantJti: string,
    reason: "auth_grant_expired" | "auth_grant_revoked" | "auth_store_unavailable",
  ): number {
    const sockets = [...(this.#byGrant.get(grantJti) ?? [])];
    for (const socket of sockets) {
      this.#closeEntry(socket, reason);
    }
    return sockets.length;
  }

  /** Returns current durable parent identities for bounded internal polling. */
  grantJtis(): readonly string[] {
    return [...this.#byGrant.keys()].sort();
  }

  /** Returns bounded counts without grant, participant, session, or socket identities. */
  debugInfo(): AuthSocketRegistryDebugInfo {
    const socketsByStream: Record<AuthSocketStreamKind, number> = {
      host: 0,
      observer: 0,
      participant: 0,
      viewer: 0,
    };
    for (const entry of this.#entries.values()) {
      socketsByStream[entry.streamKind] += 1;
    }
    return {
      closeCount: this.#closeCount,
      grantCount: this.#byGrant.size,
      maxSocketCount: this.#maxSocketCount,
      socketCount: this.#entries.size,
      socketsByStream,
      timerCount: [...this.#entries.values()].filter((entry) => entry.timer !== null).length,
    };
  }

  #closeEntry(
    socket: AuthRegistrySocket,
    reason:
      | "auth_claim_invalid"
      | "auth_grant_expired"
      | "auth_grant_revoked"
      | "auth_store_unavailable",
  ): void {
    if (!this.#entries.has(socket)) {
      return;
    }
    this.unregister(socket);
    this.#closeCount += 1;
    try {
      socket.close(1008, reason);
    } catch {
      // Transport teardown races are terminal and already reflected by cleanup.
    }
  }

  #scheduleExpiry(entry: AuthSocketEntry, expiresAt: number): void {
    if (this.#entries.get(entry.socket) !== entry) {
      return;
    }
    const remainingMs = expiresAt - this.#now();
    if (remainingMs <= 0) {
      this.#closeEntry(entry.socket, "auth_grant_expired");
      return;
    }
    entry.timer = setTimeout(
      () => {
        entry.timer = null;
        this.#scheduleExpiry(entry, expiresAt);
      },
      Math.min(remainingMs, maximumTimerDelayMs),
    );
    entry.timer.unref?.();
  }
}
