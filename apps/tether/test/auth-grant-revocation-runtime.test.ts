import { EventEmitter } from "node:events";

import { describe, expect, it, vi } from "vitest";

import {
  AuthGrantRevocationRuntime,
  authGrantRevocationNotificationChannel,
  serializeAuthGrantRevocationNotification,
} from "../src/auth/grant-revocation-runtime.js";
import { createAuthPersistenceStores } from "../src/auth/db-grant-stores.js";
import type { AuthGrantRecord, AuthGrantRevocationStore } from "../src/auth/grant-stores.js";
import type { DatabasePool } from "../src/db.js";

const now = new Date("2026-07-17T01:00:00.000Z");

describe("authentication grant revocation runtime", () => {
  it("closes notified grants and exposes only bounded counters", async () => {
    const listener = new FakeListenerClient();
    const registry = createRegistry(["grant_notified"]);
    const runtime = new AuthGrantRevocationRuntime({
      database: createDatabase(listener),
      pollIntervalMs: 0,
      registry,
      store: createGrantStore(new Map()),
    });
    await runtime.start();
    await vi.waitFor(() => {
      expect(listener.queries).toContain(`LISTEN ${authGrantRevocationNotificationChannel}`);
    });

    listener.emit("notification", {
      channel: authGrantRevocationNotificationChannel,
      payload: serializeAuthGrantRevocationNotification("grant_notified"),
    });

    expect(registry.closeGrant).toHaveBeenCalledWith("grant_notified", "auth_grant_revoked");
    expect(runtime.debugInfo()).toMatchObject({
      invalidNotificationCount: 0,
      listenerConnected: true,
      notificationCloseCount: 1,
      notificationCount: 1,
    });
    expect(JSON.stringify(runtime.debugInfo())).not.toContain("grant_notified");
    await runtime.stop();
    expect(listener.queries).toContain(`UNLISTEN ${authGrantRevocationNotificationChannel}`);
  });

  it("uses bounded polling to repair revoked, expired, and missing grant state", async () => {
    const registry = createRegistry([
      "grant_active",
      "grant_expired",
      "grant_missing",
      "grant_revoked",
    ]);
    const records = new Map<string, AuthGrantRecord>([
      ["grant_active", createGrant("grant_active")],
      ["grant_expired", createGrant("grant_expired", { expiresAt: now })],
      ["grant_revoked", createGrant("grant_revoked", { revokedAt: now })],
    ]);
    const runtime = new AuthGrantRevocationRuntime({
      database: createDatabase(new FakeListenerClient()),
      listenEnabled: false,
      now: () => now,
      pollBatchLimit: 4,
      pollIntervalMs: 0,
      registry,
      store: createGrantStore(records),
    });

    await runtime.pollNow();

    expect(registry.closeGrant).toHaveBeenCalledWith("grant_expired", "auth_grant_expired");
    expect(registry.closeGrant).toHaveBeenCalledWith("grant_missing", "auth_grant_revoked");
    expect(registry.closeGrant).toHaveBeenCalledWith("grant_revoked", "auth_grant_revoked");
    expect(registry.closeGrant).not.toHaveBeenCalledWith("grant_active", expect.anything());
    expect(runtime.debugInfo()).toMatchObject({
      pollCheckedGrantCount: 4,
      pollCloseCount: 3,
      pollCount: 1,
      pollFailureCount: 0,
    });
  });

  it("uses one bounded batch read and closes affected grants when authority is unavailable", async () => {
    const registry = createRegistry(["grant_1", "grant_2", "grant_3"]);
    const store: AuthGrantRevocationStore = {
      findManyByJti: vi.fn(async () => {
        throw new Error("database unavailable");
      }),
    };
    const runtime = new AuthGrantRevocationRuntime({
      database: createDatabase(new FakeListenerClient()),
      listenEnabled: false,
      pollBatchLimit: 2,
      pollIntervalMs: 0,
      registry,
      store,
    });

    await runtime.pollNow();

    expect(store.findManyByJti).toHaveBeenCalledTimes(1);
    expect(store.findManyByJti).toHaveBeenCalledWith(
      ["grant_1", "grant_2"],
      expect.objectContaining({ signal: expect.any(AbortSignal), timeoutMs: 1_000 }),
    );
    expect(registry.closeGrant).toHaveBeenCalledWith("grant_1", "auth_store_unavailable");
    expect(registry.closeGrant).toHaveBeenCalledWith("grant_2", "auth_store_unavailable");
    expect(runtime.debugInfo()).toMatchObject({
      pollCheckedGrantCount: 2,
      pollFailureCount: 2,
    });
  });

  it("rejects polling configurations that cannot cover the bounded registry within five seconds", () => {
    expect(
      () =>
        new AuthGrantRevocationRuntime({
          database: createDatabase(new FakeListenerClient()),
          maximumRegisteredGrantCount: 2,
          pollBatchLimit: 1,
          pollIntervalMs: 1_000,
          pollReadTimeoutMs: 1_000,
          registry: createRegistry([]),
          store: createGrantStore(new Map()),
        }),
    ).not.toThrow();
    expect(
      () =>
        new AuthGrantRevocationRuntime({
          database: createDatabase(new FakeListenerClient()),
          maximumRegisteredGrantCount: 20,
          pollBatchLimit: 1,
          pollIntervalMs: 100,
          pollReadTimeoutMs: 500,
          registry: createRegistry([]),
          store: createGrantStore(new Map()),
        }),
    ).toThrow("auth_revocation_poll_coverage_invalid");
  });

  it("releases a listener acquired before LISTEN setup fails", async () => {
    const listener = new FakeListenerClient({ failListen: true });
    const runtime = new AuthGrantRevocationRuntime({
      database: createDatabase(listener),
      pollIntervalMs: 0,
      registry: createRegistry([]),
      store: createGrantStore(new Map()),
    });

    await expect(runtime.start()).resolves.toBeUndefined();
    await vi.waitFor(() => {
      expect(listener.release).toHaveBeenCalledWith(true);
      expect(runtime.debugInfo()).toMatchObject({
        listenerConnected: false,
        listenerFailureCount: 1,
      });
    });
  });

  it("releases a failed listener and reconnects while polling remains available", async () => {
    vi.useFakeTimers();
    const first = new FakeListenerClient();
    const second = new FakeListenerClient();
    const database = createDatabaseSequence([first, second]);
    const runtime = new AuthGrantRevocationRuntime({
      database,
      pollIntervalMs: 1_000,
      registry: createRegistry([]),
      store: createGrantStore(new Map()),
    });
    await runtime.start();
    await vi.waitFor(() => {
      expect(first.queries).toContain(`LISTEN ${authGrantRevocationNotificationChannel}`);
    });

    first.emit("error", new Error("listener disconnected"));
    await vi.advanceTimersByTimeAsync(100);

    expect(first.release).toHaveBeenCalledOnce();
    expect(database.pool.connect).toHaveBeenCalledTimes(2);
    expect(second.queries).toContain(`LISTEN ${authGrantRevocationNotificationChannel}`);
    expect(runtime.debugInfo()).toMatchObject({
      listenerConnected: true,
      listenerFailureCount: 1,
      scheduled: true,
    });
    await runtime.stop();
    vi.useRealTimers();
  });

  it("cancels a stalled batch and closes its grants before polling and shutdown settle", async () => {
    const registry = createRegistry(["grant_stalled"]);
    const store: AuthGrantRevocationStore = {
      findManyByJti: vi.fn(
        (_grantJtis, options) =>
          new Promise<readonly AuthGrantRecord[]>((_resolve, reject) => {
            options.signal.addEventListener(
              "abort",
              () => reject(new Error("database read cancelled")),
              { once: true },
            );
          }),
      ),
    };
    const runtime = new AuthGrantRevocationRuntime({
      database: createDatabase(new FakeListenerClient()),
      listenEnabled: false,
      pollIntervalMs: 0,
      pollReadTimeoutMs: 5,
      registry,
      store,
    });

    await runtime.pollNow();
    await expect(runtime.stop()).resolves.toBeUndefined();

    expect(runtime.debugInfo()).toMatchObject({ pollFailureCount: 1 });
    expect(registry.closeGrant).toHaveBeenCalledWith("grant_stalled", "auth_store_unavailable");
  });

  it("starts fallback polling without awaiting a stalled LISTEN acquisition", async () => {
    vi.useFakeTimers();
    let resolveListener = (_listener: FakeListenerClient): void => undefined;
    const listenerPromise = new Promise<FakeListenerClient>((resolve) => {
      resolveListener = resolve;
    });
    const store = createGrantStore(new Map([["grant_active", createGrant("grant_active")]]));
    const runtime = new AuthGrantRevocationRuntime({
      database: { pool: { connect: vi.fn(() => listenerPromise) } },
      maximumRegisteredGrantCount: 1,
      pollBatchLimit: 1,
      pollIntervalMs: 100,
      pollReadTimeoutMs: 100,
      registry: createRegistry(["grant_active"]),
      store,
    });

    await expect(runtime.start()).resolves.toBeUndefined();
    await vi.advanceTimersByTimeAsync(100);

    expect(store.findManyByJti).toHaveBeenCalledOnce();
    const listener = new FakeListenerClient();
    resolveListener(listener);
    await Promise.resolve();
    await Promise.resolve();
    await runtime.stop();
    vi.useRealTimers();
  });

  it("destroys a listener that resolves after the bounded setup deadline", async () => {
    vi.useFakeTimers();
    let resolveListener = (_listener: FakeListenerClient): void => undefined;
    const listenerPromise = new Promise<FakeListenerClient>((resolve) => {
      resolveListener = resolve;
    });
    const runtime = new AuthGrantRevocationRuntime({
      database: { pool: { connect: vi.fn(() => listenerPromise) } },
      pollIntervalMs: 0,
      registry: createRegistry([]),
      store: createGrantStore(new Map()),
    });

    await runtime.start();
    await vi.advanceTimersByTimeAsync(1_000);
    const listener = new FakeListenerClient();
    resolveListener(listener);
    await Promise.resolve();
    await Promise.resolve();
    expect(listener.release).toHaveBeenCalledWith(true);
    await runtime.stop();
    vi.useRealTimers();
  });

  it("destroys a client when LISTEN itself exceeds the setup deadline", async () => {
    vi.useFakeTimers();
    const listener = new FakeListenerClient({ stallListen: true });
    const runtime = new AuthGrantRevocationRuntime({
      database: createDatabase(listener),
      pollIntervalMs: 0,
      registry: createRegistry([]),
      store: createGrantStore(new Map()),
    });

    await runtime.start();
    await Promise.resolve();
    await Promise.resolve();
    expect(listener.queries).toContain(`LISTEN ${authGrantRevocationNotificationChannel}`);
    await vi.advanceTimersByTimeAsync(1_000);

    expect(listener.release).toHaveBeenCalledWith(true);
    await runtime.stop();
    vi.useRealTimers();
  });

  it("destroys a client when UNLISTEN cannot confirm cleanup", async () => {
    vi.useFakeTimers();
    const listener = new FakeListenerClient({ stallUnlisten: true });
    const runtime = new AuthGrantRevocationRuntime({
      database: createDatabase(listener),
      pollIntervalMs: 0,
      registry: createRegistry([]),
      store: createGrantStore(new Map()),
    });
    await runtime.start();
    await Promise.resolve();
    await Promise.resolve();
    expect(listener.queries).toContain(`LISTEN ${authGrantRevocationNotificationChannel}`);

    const stopping = runtime.stop();
    await vi.advanceTimersByTimeAsync(1_000);

    await expect(stopping).resolves.toBeUndefined();
    expect(listener.release).toHaveBeenCalledWith(true);
    vi.useRealTimers();
  });

  it("aborts pending poll-client acquisition and destroys the late client", async () => {
    let resolveClient = (_client: FakeListenerClient): void => undefined;
    const connecting = new Promise<FakeListenerClient>((resolve) => {
      resolveClient = resolve;
    });
    const connect = vi.fn(() => connecting);
    const database = createDatabasePool(connect);
    const store = createAuthPersistenceStores(database).grants;
    const controller = new AbortController();
    const reading = store.findManyByJti(["grant_pending"], {
      signal: controller.signal,
      timeoutMs: 1_000,
    });

    controller.abort();
    await expect(reading).rejects.toThrow("auth_grant_read_failed");
    await expect(
      store.findManyByJti(["grant_next"], {
        signal: new AbortController().signal,
        timeoutMs: 1_000,
      }),
    ).rejects.toThrow("auth_grant_read_failed");
    await expect(
      store.findManyByJti(["grant_after_next"], {
        signal: new AbortController().signal,
        timeoutMs: 1_000,
      }),
    ).rejects.toThrow("auth_grant_read_failed");
    expect(connect).toHaveBeenCalledOnce();
    const client = new FakeListenerClient();
    resolveClient(client);
    await vi.waitFor(() => expect(client.release).toHaveBeenCalledWith(true));
  });

  it("observes cancellation fired synchronously during pool acquisition", async () => {
    const controller = new AbortController();
    let resolveClient = (_client: FakeListenerClient): void => undefined;
    const connecting = new Promise<FakeListenerClient>((resolve) => {
      resolveClient = resolve;
    });
    const connect = vi.fn(() => {
      controller.abort();
      return connecting;
    });
    const store = createAuthPersistenceStores(createDatabasePool(connect)).grants;

    await expect(
      store.findManyByJti(["grant_raced_abort"], {
        signal: controller.signal,
        timeoutMs: 1_000,
      }),
    ).rejects.toThrow("auth_grant_read_failed");
    const client = new FakeListenerClient();
    resolveClient(client);
    await vi.waitFor(() => expect(client.release).toHaveBeenCalledWith(true));
  });
});

function createRegistry(grantJtis: readonly string[]) {
  return {
    closeGrant: vi.fn(() => 1),
    grantJtis: vi.fn(() => grantJtis),
  };
}

function createGrant(jti: string, overrides: Partial<AuthGrantRecord> = {}): AuthGrantRecord {
  return {
    audience: "tether-rest",
    expiresAt: new Date("2026-07-18T01:00:00.000Z"),
    issuedAt: new Date("2026-07-17T00:00:00.000Z"),
    issuer: "https://auth.revocation.test",
    jti,
    kid: "current",
    metadata: { requestId: null, source: "admin" },
    revokedAt: null,
    role: "participant",
    sessionScope: "*",
    subject: "part_revocation",
    ...overrides,
  };
}

function createGrantStore(records: ReadonlyMap<string, AuthGrantRecord>): AuthGrantRevocationStore {
  return {
    findManyByJti: vi.fn(async (grantJtis: readonly string[]) =>
      grantJtis.flatMap((grantJti) => {
        const grant = records.get(grantJti);
        return grant === undefined ? [] : [grant];
      }),
    ),
  };
}

function createDatabase(client: FakeListenerClient) {
  return {
    pool: {
      connect: vi.fn(async () => client),
    },
  };
}

function createDatabaseSequence(clients: readonly FakeListenerClient[]) {
  let index = 0;
  return {
    pool: {
      connect: vi.fn(async () => {
        const client = clients[index];
        index += 1;
        if (client === undefined) {
          throw new Error("No listener client available");
        }
        return client;
      }),
    },
  };
}

class FakeListenerClient extends EventEmitter {
  readonly queries: string[] = [];
  readonly release = vi.fn();
  readonly #failListen: boolean;
  readonly #stallListen: boolean;
  readonly #stallUnlisten: boolean;

  constructor(
    options: {
      readonly failListen?: boolean;
      readonly stallListen?: boolean;
      readonly stallUnlisten?: boolean;
    } = {},
  ) {
    super();
    this.#failListen = options.failListen ?? false;
    this.#stallListen = options.stallListen ?? false;
    this.#stallUnlisten = options.stallUnlisten ?? false;
  }

  async query(
    config: string | { readonly query_timeout: number; readonly text: string },
  ): Promise<{ readonly rows: readonly [] }> {
    const sql = typeof config === "string" ? config : config.text;
    this.queries.push(sql);
    if (this.#failListen && sql.startsWith("LISTEN")) {
      throw new Error("listen unavailable");
    }
    if (
      (this.#stallListen && sql.startsWith("LISTEN")) ||
      (this.#stallUnlisten && sql.startsWith("UNLISTEN"))
    ) {
      const timeoutMs = typeof config === "string" ? 1_000 : config.query_timeout;
      return new Promise((_resolve, reject) => {
        setTimeout(() => reject(new Error("listen query timeout")), timeoutMs);
      });
    }
    return { rows: [] };
  }
}

function createDatabasePool(connect: () => Promise<FakeListenerClient>): DatabasePool {
  return {
    db: {} as DatabasePool["db"],
    end: vi.fn(async () => undefined),
    pool: { connect: vi.fn(connect) } as unknown as DatabasePool["pool"],
  };
}
