import { randomUUID } from "node:crypto";

import {
  RestParticipantControlClient,
  type RestParticipantControlContext,
  RestParticipantControlError,
  type RestParticipantControlFetch,
} from "@dungle-scrubs/tether-client";
import {
  eventListResponseSchema,
  restControlRenewalResponseSchema,
  sessionEventSchema,
  systemProducerId,
} from "@dungle-scrubs/tether-protocol";
import pg from "pg";
import { afterAll, afterEach, beforeAll, describe, expect, it } from "vitest";
import { z } from "zod";

import { createAuthPersistenceStores } from "../src/auth/db-grant-stores.js";
import { createAuthGrantLifecycle } from "../src/auth/grant-lifecycle.js";
import {
  mintTestAuthToken,
  testAuthSigningKid,
  testAuthSigningSecret,
} from "../src/auth/test-tokens.js";
import { createPool, type DatabasePool, migrate } from "../src/db.js";
import { type AppServer, createAppServer } from "../src/http.js";

/**
 * RFC-12 first-cohort strict two-instance REST control proof.
 *
 * Scope: Tether transport and fencing evidence only. Every acquisition,
 * release, append, heartbeat, and event read in this suite traverses public
 * HTTP against a strict-mode app (auth required, legacy tokens rejected,
 * control-epoch enforcement on) backed by synthetic durable grants on one
 * disposable database. Durable grant rows are seeded once in `beforeAll`
 * through the internal grant lifecycle purely as fixture setup; no public
 * provisioning behavior is claimed. No Graybox admission, cutover, barrier
 * schema, or unknown-outcome reconciliation is proven here.
 *
 * The suite activates only under `E2E=true` with an explicitly supplied
 * loopback `E2E_ADMIN_DATABASE_URL` (never printed), matching the provisioning
 * runner in `scripts/test-e2e.ts`.
 */

const strictRestControlE2e = process.env.E2E === "true" ? describe : describe.skip;

/** Synthetic in-memory signing issuer shared by the app and fixture grants. */
const fixtureAuthIssuer = "https://auth.e2e.tether.local";

/** Inert test-only custom event type; it asserts no Graybox barrier schema. */
const syntheticEventType = "e2e.rest-control.synthetic.v1";

/** One parsed event from the public event-list response. */
type ListedEvent = z.infer<typeof eventListResponseSchema>["events"][number];

/** Raw HTTP result: status plus JSON kept as unknown until parsed. */
interface RawHttpResponse {
  readonly body: unknown;
  readonly status: number;
}

/** Local response shapes without a protocol-owned schema. */
const createSessionResponseSchema = z.object({
  session: z.object({ sessionId: z.string().min(1) }),
});

const appendEventResponseSchema = z.object({
  event: sessionEventSchema,
  status: z.enum(["created", "replayed"]),
});

/** Shared shape of the fenced control rejections this suite inspects. */
const controlErrorResponseSchema = z.object({
  code: z.string().min(1).optional(),
  currentEpoch: z.number().int().positive().nullable().optional(),
  instanceId: z.string().min(1).optional(),
  reason: z.string().min(1).optional(),
});

/**
 * Reads the admin database URL required for enabled runs. Accepts only an
 * explicit loopback target for this fixture and never embeds the value (which
 * carries credentials) in any error message.
 */
function readLoopbackE2eAdminDatabaseUrl(): string {
  const value = process.env.E2E_ADMIN_DATABASE_URL;
  if (process.env.E2E !== "true") {
    return "postgres://e2e-disabled@127.0.0.1:54329/postgres";
  }
  if (value === undefined || value.length === 0) {
    throw new Error("E2E_ADMIN_DATABASE_URL is required when E2E=true");
  }
  let url: URL;
  try {
    url = new URL(value);
  } catch {
    throw new Error("E2E_ADMIN_DATABASE_URL must be a valid loopback database URL");
  }
  if (
    (url.protocol !== "postgres:" && url.protocol !== "postgresql:") ||
    url.hostname !== "127.0.0.1"
  ) {
    throw new Error("E2E_ADMIN_DATABASE_URL must be a PostgreSQL URL on 127.0.0.1");
  }
  return value;
}

const adminDatabaseUrl = readLoopbackE2eAdminDatabaseUrl();

/** Sends one request to the app under test and returns unparsed JSON. */
async function fetchJson(
  baseUrl: string,
  path: string,
  init: {
    readonly authToken?: string;
    readonly body?: unknown;
    readonly method?: string;
    readonly timeoutMs?: number;
  } = {},
): Promise<RawHttpResponse> {
  const timeoutMs = init.timeoutMs ?? 5_000;
  const response = await fetch(`${baseUrl}${path}`, {
    ...(init.body !== undefined ? { body: JSON.stringify(init.body) } : {}),
    headers: {
      "content-type": "application/json",
      ...(init.authToken ? { authorization: `Bearer ${init.authToken}` } : {}),
    },
    method: init.method ?? (init.body !== undefined ? "POST" : "GET"),
    signal: AbortSignal.timeout(timeoutMs),
  });
  const text = await response.text();
  let body: unknown = null;
  if (text.length > 0) {
    try {
      body = JSON.parse(text) as unknown;
    } catch {
      body = null;
    }
  }
  return { body, status: response.status };
}

/** Keeps real control HTTP requests bounded while honoring the client's abort signal. */
const controlFetch: RestParticipantControlFetch = (input, init) => {
  const timeout = AbortSignal.timeout(5_000);
  return fetch(input, {
    ...init,
    signal: init.signal ? AbortSignal.any([init.signal, timeout]) : timeout,
  });
};

/** Parses one response against a schema, treating the body as unknown until here. */
function parseRestJson<TValue>(schema: z.ZodType<TValue>, body: unknown, label: string): TValue {
  const parsed = schema.safeParse(body);
  if (!parsed.success) {
    throw new Error(`${label} response did not match its schema`);
  }
  return parsed.data;
}

/** Quotes a Postgres identifier for database create/drop commands. */
function quoteIdentifier(value: string): string {
  return `"${value.replaceAll('"', '""')}"`;
}

/** Builds the disposable database URL from the loopback admin URL. */
function buildDatabaseUrl(databaseName: string): string {
  const url = new URL(adminDatabaseUrl);
  url.pathname = `/${databaseName}`;
  return url.toString();
}

/** Creates the isolated disposable database for this run. */
async function createDatabase(databaseName: string): Promise<void> {
  const adminPool = new pg.Pool({
    connectionString: adminDatabaseUrl,
    connectionTimeoutMillis: 5_000,
    query_timeout: 5_000,
    statement_timeout: 5_000,
  });
  try {
    await adminPool.query(`CREATE DATABASE ${quoteIdentifier(databaseName)}`);
  } finally {
    await adminPool.end();
  }
}

/**
 * Terminates only this run's randomly generated database connections and drops
 * only that database.
 */
async function dropDatabase(databaseName: string): Promise<void> {
  const adminPool = new pg.Pool({
    connectionString: adminDatabaseUrl,
    connectionTimeoutMillis: 5_000,
    query_timeout: 5_000,
    statement_timeout: 5_000,
  });
  try {
    await adminPool.query(
      "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = $1",
      [databaseName],
    );
    await adminPool.query(`DROP DATABASE IF EXISTS ${quoteIdentifier(databaseName)}`);
  } finally {
    await adminPool.end();
  }
}

/** Sanitized lock-wait evidence for one known fixture backend. */
interface RaceWaiterRow {
  readonly blockers: readonly number[];
  readonly expectedStatement: boolean;
  readonly pid: number;
  readonly waitEvent: string;
  readonly waitEventType: string;
}

/** PostgreSQL's advisory identity, compared directly without signed hash conversion. */
interface RaceAdvisoryLockRow {
  readonly database: number;
  readonly classid: number;
  readonly objid: number;
  readonly objsubid: number;
  readonly pid: number;
  readonly granted: boolean;
  readonly mode: string;
}

/** Collapses SQL whitespace into one space for fingerprint comparison. */
function normalizeSql(value: string): string {
  return value.replace(/\s+/gu, " ").trim();
}

/** Static fingerprint of the transaction-scoped advisory lock helper. */
const raceAdvisoryHelperFingerprint = normalizeSql(
  "SELECT pg_advisory_xact_lock(hashtext($1::text), hashtext($2::text))",
);

/** Static fingerprint of the current-lease SELECT FOR UPDATE in db.ts. */
const raceCurrentLeaseForUpdateFingerprint = normalizeSql(`
  SELECT
    acquisition_id AS "acquisitionId",
    claimed_at AS "claimedAt",
    control_channel AS "controlChannel",
    epoch,
    instance_id AS "instanceId",
    last_seen_at AS "lastSeenAt",
    lease_expires_at AS "leaseExpiresAt",
    participant_id AS "participantId",
    released_at AS "releasedAt",
    session_id AS "sessionId",
    superseded_at AS "supersededAt"
  FROM participant_control_leases
  WHERE session_id = $1
    AND participant_id = $2
    AND released_at IS NULL
    AND superseded_at IS NULL
  ORDER BY lease_expires_at DESC, claimed_at DESC, instance_id
  FOR UPDATE
`);

/** Static fingerprint of the participant presence SELECT FOR UPDATE in db.ts. */
const racePresenceForUpdateFingerprint = normalizeSql(`
  SELECT
    capabilities,
    display_name AS "displayName",
    joined_at AS "joinedAt",
    last_seen_at AS "lastSeenAt",
    participant_id AS "participantId",
    runtime_kind AS "runtimeKind",
    session_id AS "sessionId"
  FROM participants
  WHERE session_id = $1
    AND participant_id = $2
  FOR UPDATE
`);

/** Connects a bounded fixture client and closes it if setup fails. */
async function connectRaceDirectClient(input: {
  readonly applicationName: string;
  readonly connectionString: string;
}): Promise<pg.Client> {
  const client = new pg.Client({
    application_name: input.applicationName,
    connectionString: input.connectionString,
    connectionTimeoutMillis: 5_000,
    statement_timeout: 12_000,
  });
  // Idle-transaction expiry can close the blocker after a failed test. The
  // query/cleanup checks surface that failure without an unhandled event.
  client.on("error", () => {});
  try {
    await client.connect();
    await client.query("SET default_transaction_isolation = 'read committed'");
    await client.query("SET lock_timeout = 10000");
    await client.query("SET idle_in_transaction_session_timeout = 12000");
    return client;
  } catch {
    await client.end().catch(() => {});
    throw new Error("race direct connection setup failed");
  }
}

/** Polls an exact active wait condition; elapsed time alone never passes. */
async function observeRaceWaiter(input: {
  readonly applicationName: string;
  readonly deadlineMs: number;
  readonly expectedFingerprint: string;
  readonly expectedPid?: number;
  readonly expectedWaitEvent: string;
  readonly observer: pg.Client;
  readonly requiredBlocker: number;
}): Promise<RaceWaiterRow> {
  const deadline = performance.now() + input.deadlineMs;
  while (performance.now() < deadline) {
    const result = await input.observer.query<{
      blockers: number[];
      pid: number;
      query: string | null;
      state: string;
      wait_event: string | null;
      wait_event_type: string | null;
    }>(
      `SELECT pid, state, wait_event, wait_event_type,
              pg_blocking_pids(pid) AS blockers, query
       FROM pg_stat_activity
       WHERE datname = current_database() AND application_name = $1`,
      [input.applicationName],
    );
    const matches = result.rows.filter(
      (row) =>
        (input.expectedPid === undefined || row.pid === input.expectedPid) &&
        row.state === "active" &&
        row.wait_event_type === "Lock" &&
        row.wait_event === input.expectedWaitEvent &&
        row.query !== null &&
        normalizeSql(row.query) === input.expectedFingerprint &&
        row.blockers.includes(input.requiredBlocker),
    );
    if (matches.length > 1) throw new Error("race observer found ambiguous waiters");
    const match = matches[0];
    if (match !== undefined) {
      if (match.blockers.length !== 1) throw new Error("race observer found unexpected blockers");
      return {
        blockers: match.blockers,
        expectedStatement: true,
        pid: match.pid,
        waitEvent: input.expectedWaitEvent,
        waitEventType: "Lock",
      };
    }
    await new Promise((resolve) => setTimeout(resolve, 20));
  }
  throw new Error("race observer wait condition timed out");
}

/** Matches the known holder's granted advisory identity to the known waiter. */
async function observeRaceAdvisoryPair(input: {
  readonly deadlineMs: number;
  readonly holderPid: number;
  readonly waiterPid: number;
  readonly observer: pg.Client;
}): Promise<void> {
  const deadline = performance.now() + input.deadlineMs;
  while (performance.now() < deadline) {
    const result = await input.observer.query<RaceAdvisoryLockRow>(
      `SELECT database, pid, classid, objid, objsubid, mode, granted
       FROM pg_locks
       WHERE pid = ANY($1::int[]) AND locktype = 'advisory'
         AND database = (SELECT oid FROM pg_database WHERE datname = current_database())
         AND objsubid = 2 AND mode = 'ExclusiveLock'`,
      [[input.holderPid, input.waiterPid]],
    );
    const holders = result.rows.filter((row) => row.pid === input.holderPid && row.granted);
    const waiters = result.rows.filter((row) => row.pid === input.waiterPid && !row.granted);
    const pairs = holders.flatMap((holder) =>
      waiters.filter(
        (waiter) =>
          holder.database === waiter.database &&
          holder.classid === waiter.classid &&
          holder.objid === waiter.objid &&
          holder.objsubid === waiter.objsubid,
      ),
    );
    if (pairs.length > 1) throw new Error("race advisory identity is ambiguous");
    if (pairs.length === 1) return;
    await new Promise((resolve) => setTimeout(resolve, 20));
  }
  throw new Error("race advisory identity did not match");
}

/** Waits for database work on the two fixture pools to settle after HTTP ends. */
async function awaitRaceBackendsIdle(
  observer: pg.Client,
  applicationNames: readonly string[],
): Promise<void> {
  const deadline = performance.now() + 5_000;
  while (performance.now() < deadline) {
    const result = await observer.query<{ count: number }>(
      `SELECT count(*)::int AS count FROM pg_stat_activity
       WHERE datname = current_database() AND application_name = ANY($1::text[])
         AND state <> 'idle'`,
      [applicationNames],
    );
    if (result.rows[0]?.count === 0) return;
    await new Promise((resolve) => setTimeout(resolve, 20));
  }
  throw new Error("race database work did not settle");
}

/** Bounds each teardown step so later cleanup still runs after a stuck operation. */
async function boundRaceCleanup(action: Promise<unknown>, timeoutMs = 5_000): Promise<void> {
  let timer: ReturnType<typeof setTimeout> | undefined;
  try {
    await Promise.race([
      action,
      new Promise<never>((_, reject) => {
        timer = setTimeout(() => reject(new Error("race cleanup step timed out")), timeoutMs);
      }),
    ]);
  } finally {
    if (timer !== undefined) clearTimeout(timer);
  }
}

strictRestControlE2e("tether e2e strict two-instance REST control", () => {
  const runId = randomUUID();
  const databaseName = `tether_e2e_rest_2p_${runId.replaceAll("-", "_")}`;
  const databaseUrl = buildDatabaseUrl(databaseName);

  const participantIdP = "part_e2e_rest_2p_p";
  const participantIdQ = "part_e2e_rest_2p_q";
  const instanceIdA = `inst_e2e_rest_2p_a_${runId}`;
  const instanceIdB = `inst_e2e_rest_2p_b_${runId}`;
  const instanceIdQ = `inst_e2e_rest_2p_q_${runId}`;
  const fixedEventId = `evt_e2e_rest_2p_fixed_${runId}`;
  const fixedPayload = { cohort: "rest-two-instance", marker: runId, role: "fixed" };
  const alteredPayload = { cohort: "rest-two-instance", marker: runId, role: "altered" };

  let app: AppServer | null = null;
  let baseUrl = "";
  let databaseCreationAttempted = false;
  let committedSequence: number | null = null;
  let pool: DatabasePool | null = null;
  let clientA: RestParticipantControlClient | null = null;
  let clientB: RestParticipantControlClient | null = null;
  let clientQ: RestParticipantControlClient | null = null;
  let sessionId = "";
  let pBearer = "";
  let qBearer = "";

  /** Captured acquisition-time context for instance A, kept for its stale retry. */
  let contextA: RestParticipantControlContext | null = null;

  beforeAll(async () => {
    // CREATE may commit before its connection reports an error. Cleanup still
    // attempts this unique database if setup has an unknown outcome.
    databaseCreationAttempted = true;
    await createDatabase(databaseName);
    pool = createPool(databaseUrl);
    await migrate(pool);

    // Fixture setup only: seed synthetic audited durable grants through the
    // internal lifecycle so the strict app can authenticate the setup
    // authority and the two scoped participants. This is not a public
    // provisioning proof, and the bearers never leave this process.
    const grantLifecycle = createAuthGrantLifecycle({
      activeKid: testAuthSigningKid,
      issuanceEnabled: true,
      issuer: fixtureAuthIssuer,
      secrets: { [testAuthSigningKid]: testAuthSigningSecret },
      stores: createAuthPersistenceStores(pool),
    });

    app = createAppServer(pool, {
      auth: {
        activeKid: testAuthSigningKid,
        allowLegacyTokens: false,
        issuer: fixtureAuthIssuer,
        mode: "required",
        secrets: { [testAuthSigningKid]: testAuthSigningSecret },
      },
      sessionService: { controlEpochEnforcement: true },
      taskClaimSweeper: { intervalMs: 0 },
    });
    const port = await app.listen(0, "127.0.0.1");
    baseUrl = `http://127.0.0.1:${port}`;

    // Service-scoped setup authority: the only identity allowed to create the
    // session over the public route.
    const setupGrant = await grantLifecycle.create({
      actorSubject: "svc_e2e_rest_2p_setup",
      reasonCode: "bootstrap",
      role: "admin",
      sessionScope: "*",
      source: "bootstrap",
      subject: "svc_e2e_rest_2p_setup",
      ttlSeconds: 3_600,
    });

    const sessionResponse = await fetchJson(baseUrl, "/sessions", {
      authToken: setupGrant.bearer,
      body: { sessionId: `sess_e2e_rest_2p_${runId}` },
    });
    if (sessionResponse.status !== 201) {
      throw new Error(`public session creation failed with status ${sessionResponse.status}`);
    }
    sessionId = parseRestJson(createSessionResponseSchema, sessionResponse.body, "session create")
      .session.sessionId;

    // Session-scoped participant grants: the grant subject becomes the
    // authenticated participant id, so P and Q mutations are identity-bound.
    const pGrant = await grantLifecycle.create({
      actorSubject: "svc_e2e_rest_2p_setup",
      reasonCode: "bootstrap",
      role: "participant",
      sessionScope: sessionId,
      source: "bootstrap",
      subject: participantIdP,
      ttlSeconds: 3_600,
    });
    pBearer = pGrant.bearer;
    const qGrant = await grantLifecycle.create({
      actorSubject: "svc_e2e_rest_2p_setup",
      reasonCode: "bootstrap",
      role: "participant",
      sessionScope: sessionId,
      source: "bootstrap",
      subject: participantIdQ,
      ttlSeconds: 3_600,
    });
    qBearer = qGrant.bearer;

    // Two actual exported control clients for the same participant P with
    // distinct instance ids, plus one independent participant client for Q.
    clientA = new RestParticipantControlClient(
      {
        authToken: pBearer,
        instanceId: instanceIdA,
        participantId: participantIdP,
        runtimeKind: "generic_agent",
        serviceUrl: baseUrl,
      },
      { fetch: controlFetch },
    );
    clientB = new RestParticipantControlClient(
      {
        authToken: pBearer,
        instanceId: instanceIdB,
        participantId: participantIdP,
        runtimeKind: "generic_agent",
        serviceUrl: baseUrl,
      },
      { fetch: controlFetch },
    );
    clientQ = new RestParticipantControlClient(
      {
        authToken: qBearer,
        instanceId: instanceIdQ,
        participantId: participantIdQ,
        runtimeKind: "generic_agent",
        serviceUrl: baseUrl,
      },
      { fetch: controlFetch },
    );
  }, 30_000);

  afterAll(async () => {
    // Every step runs even when an earlier one fails; failures are collected
    // and reported together instead of silently succeeding.
    const cleanupFailures: string[] = [];
    const attemptCleanup = async (step: string, run: () => Promise<void>): Promise<void> => {
      try {
        await run();
      } catch {
        // Connection/transport errors can contain credentials or request data.
        cleanupFailures.push(step);
      }
    };

    // Release leases over public HTTP while the app is still listening.
    if (clientA !== null) {
      await attemptCleanup("stop client A", () => clientA?.stop() ?? Promise.resolve());
    }
    if (clientB !== null) {
      await attemptCleanup("stop client B", () => clientB?.stop() ?? Promise.resolve());
    }
    if (clientQ !== null) {
      await attemptCleanup("stop client Q", () => clientQ?.stop() ?? Promise.resolve());
    }
    if (app !== null) {
      await attemptCleanup("close app server", () => app?.close() ?? Promise.resolve());
    }
    if (pool !== null) {
      await attemptCleanup("end database pool", () => pool?.end() ?? Promise.resolve());
    }
    if (databaseCreationAttempted) {
      await attemptCleanup("drop disposable database", () => dropDatabase(databaseName));
    }
    if (cleanupFailures.length > 0) {
      throw new Error(`strict two-instance cleanup failures: ${cleanupFailures.join("; ")}`);
    }
  }, 30_000);

  const appendEvent = (input: {
    readonly authToken: string;
    readonly controlEpoch: number;
    readonly eventId: string;
    readonly instanceId: string;
    readonly payload: Record<string, unknown>;
    readonly producerId: string;
  }): Promise<RawHttpResponse> =>
    fetchJson(baseUrl, `/sessions/${sessionId}/events`, {
      authToken: input.authToken,
      body: {
        controlEpoch: input.controlEpoch,
        eventId: input.eventId,
        instanceId: input.instanceId,
        payload: input.payload,
        producerId: input.producerId,
        type: syntheticEventType,
      },
      method: "POST",
    });

  const requireClients = (): {
    readonly a: RestParticipantControlClient;
    readonly b: RestParticipantControlClient;
    readonly q: RestParticipantControlClient;
  } => {
    if (clientA === null || clientB === null || clientQ === null) {
      throw new Error("control clients were not initialized");
    }
    return { a: clientA, b: clientB, q: clientQ };
  };

  it("qualifies the strict public REST cohort across two instances and two participants", async () => {
    // These requests prove the fixture's strict settings at the public boundary.
    const unauthenticated = await fetchJson(baseUrl, `/sessions/${sessionId}/events?after=0`);
    expect(unauthenticated.status).toBe(401);
    expect(unauthenticated.body).toEqual({ error: "Unauthorized", reason: "missing" });
    const legacy = await fetchJson(baseUrl, `/sessions/${sessionId}/events?after=0`, {
      authToken: mintTestAuthToken({
        participantId: participantIdP,
        role: "participant",
        sessionId,
      }),
    });
    // This token is signed by the same synthetic key accepted by the app.
    expect(legacy.status).toBe(401);
    expect(legacy.body).toEqual({ error: "Unauthorized", reason: "auth_legacy_token_rejected" });

    // instance A acquires the public REST control lease.
    {
      const { a } = requireClients();
      const context = await a.context(sessionId);
      expect(context.controlEpoch).toBeGreaterThan(0);
      expect(context.instanceId).toBe(instanceIdA);
      expect(context.participantId).toBe(participantIdP);
      expect(context.sessionId).toBe(sessionId);
      contextA = context;
    }
    // Supplied stale epochs are fenced even in compatibility mode. A missing
    // epoch must also fail to establish that enforcement is enabled.
    const missingEpoch = await fetchJson(baseUrl, `/sessions/${sessionId}/events`, {
      authToken: pBearer,
      body: {
        eventId: `evt_e2e_missing_epoch_${runId}`,
        instanceId: instanceIdA,
        payload: {},
        producerId: participantIdP,
        type: syntheticEventType,
      },
    });
    expect(missingEpoch.status).toBe(428);
    expect(parseRestJson(controlErrorResponseSchema, missingEpoch.body, "missing epoch").code).toBe(
      "CONTROL_EPOCH_REQUIRED",
    );

    // rejects instance B with an active-instance conflict while A holds the lease.
    {
      const { b } = requireClients();
      const failure = await b.context(sessionId).then(
        () => null,
        (error: unknown) => error,
      );
      expect(failure).toBeInstanceOf(RestParticipantControlError);
      expect(failure instanceof RestParticipantControlError ? failure.code : null).toBe(
        "CONTROL_CONFLICT",
      );

      // The same public registration route reports the active instance id.
      const probe = await fetchJson(baseUrl, `/sessions/${sessionId}/participants`, {
        authToken: pBearer,
        body: {
          acquisitionId: `acq_e2e_probe_${runId}`,
          capabilities: {},
          controlChannel: "rest",
          instanceId: instanceIdB,
          participantId: participantIdP,
          runtimeKind: "generic_agent",
        },
        method: "POST",
      });
      expect(probe.status).toBe(409);
      const conflict = parseRestJson(controlErrorResponseSchema, probe.body, "conflict probe");
      expect(conflict.code).toBe("CONTROL_CONFLICT");
      expect(conflict.instanceId).toBe(instanceIdA);
    }

    // instance A commits a fixed-id synthetic custom event.
    {
      if (contextA === null) {
        throw new Error("instance A context was not captured");
      }
      const response = await appendEvent({
        authToken: pBearer,
        controlEpoch: contextA.controlEpoch,
        eventId: fixedEventId,
        instanceId: contextA.instanceId,
        payload: fixedPayload,
        producerId: participantIdP,
      });
      expect(response.status).toBe(201);
      const created = parseRestJson(appendEventResponseSchema, response.body, "fixed append");
      expect(created.status).toBe("created");
      expect(created.event.eventId).toBe(fixedEventId);
      expect(created.event.producerId).toBe(participantIdP);
      expect(created.event.seq).toBeGreaterThan(0);
      committedSequence = created.event.seq;
    }

    // instance B acquires a strictly higher epoch after A releases.
    {
      const { a, b } = requireClients();
      if (contextA === null) {
        throw new Error("instance A context was not captured");
      }
      await expect(a.release(sessionId)).resolves.toBe(true);
      const context = await b.context(sessionId);
      expect(context.controlEpoch).toBeGreaterThan(contextA.controlEpoch);
      expect(context.instanceId).toBe(instanceIdB);
    }

    // renews B over the public heartbeat route and keeps its epoch.
    {
      const { b } = requireClients();
      const context = await b.context(sessionId);
      const response = await fetchJson(
        baseUrl,
        `/sessions/${sessionId}/participants/${encodeURIComponent(participantIdP)}/heartbeat`,
        {
          authToken: pBearer,
          body: { controlEpoch: context.controlEpoch, instanceId: context.instanceId },
          method: "POST",
        },
      );
      expect(response.status).toBe(200);
      const renewed = parseRestJson(
        restControlRenewalResponseSchema,
        response.body,
        "heartbeat renewal",
      );
      expect(renewed.controlEpoch).toBe(context.controlEpoch);
    }

    // fences A's distinct-instance retry of the committed event at the
    // instance fence, ahead of both the epoch check and idempotency.
    {
      const { b } = requireClients();
      if (contextA === null) {
        throw new Error("instance A context was not captured");
      }
      const contextB = await b.context(sessionId);
      const response = await appendEvent({
        authToken: pBearer,
        controlEpoch: contextA.controlEpoch,
        eventId: fixedEventId,
        instanceId: contextA.instanceId,
        payload: fixedPayload,
        producerId: participantIdP,
      });
      expect(response.status).toBe(409);
      const fenced = parseRestJson(
        controlErrorResponseSchema,
        response.body,
        "cross-instance retry",
      );
      // renewControlLease compares the instance before the epoch, so a retry
      // from a non-holder instance gets CONTROL_CONFLICT naming the active
      // holder B no matter which epoch it supplies. The committed event is
      // never consulted: a 200 "replayed" answer here would mean idempotency
      // lookup ran ahead of the instance fence.
      expect(fenced.code).toBe("CONTROL_CONFLICT");
      expect(fenced.instanceId).toBe(contextB.instanceId);
      expect(fenced.instanceId).toBe(instanceIdB);
    }

    // rotates B's epoch in place through the exported client: capture B's
    // current context, release over public HTTP, then reacquire on the same
    // client and instance id for a strictly higher epoch.
    {
      const { b } = requireClients();
      const priorB = await b.context(sessionId);
      await expect(b.release(sessionId)).resolves.toBe(true);
      const rotatedB = await b.context(sessionId);
      expect(rotatedB.controlEpoch).toBeGreaterThan(priorB.controlEpoch);
      expect(rotatedB.instanceId).toBe(priorB.instanceId);

      // Same-instance stale retry of the committed fixed event under B's
      // captured pre-rotation context: the instance now passes the instance
      // fence, so the rotated epoch is what rejects it - still before the
      // idempotency lookup, which would otherwise answer 200 "replayed".
      const response = await appendEvent({
        authToken: pBearer,
        controlEpoch: priorB.controlEpoch,
        eventId: fixedEventId,
        instanceId: priorB.instanceId,
        payload: fixedPayload,
        producerId: participantIdP,
      });
      expect(response.status).toBe(409);
      const stale = parseRestJson(controlErrorResponseSchema, response.body, "stale retry");
      expect(stale.code).toBe("CONTROL_EPOCH_STALE");
      expect(stale.currentEpoch).toBe(rotatedB.controlEpoch);
    }

    // replays the identical retry under B's current (rotated) context without
    // a second stored event.
    {
      const { b } = requireClients();
      const contextB = await b.context(sessionId);
      const response = await appendEvent({
        authToken: pBearer,
        controlEpoch: contextB.controlEpoch,
        eventId: fixedEventId,
        instanceId: contextB.instanceId,
        payload: fixedPayload,
        producerId: participantIdP,
      });
      expect(response.status).toBe(200);
      const replayed = parseRestJson(appendEventResponseSchema, response.body, "identical retry");
      expect(replayed.status).toBe("replayed");
      expect(replayed.event.eventId).toBe(fixedEventId);
      expect(replayed.event.seq).toBe(committedSequence);
      expect(replayed.event.payload).toEqual(fixedPayload);
    }

    // rejects an altered payload for the same event id as a conflict.
    {
      const { b } = requireClients();
      const contextB = await b.context(sessionId);
      const response = await appendEvent({
        authToken: pBearer,
        controlEpoch: contextB.controlEpoch,
        eventId: fixedEventId,
        instanceId: contextB.instanceId,
        payload: alteredPayload,
        producerId: participantIdP,
      });
      expect(response.status).toBe(409);
      const conflict = parseRestJson(controlErrorResponseSchema, response.body, "altered retry");
      expect(conflict.reason).toBe("event_id_conflict");
    }

    // lets participant Q append under its independent lease while B stays valid.
    {
      const { b, q } = requireClients();
      const contextB = await b.context(sessionId);
      const contextQ = await q.context(sessionId);
      expect(contextQ.controlEpoch).toBeGreaterThan(0);
      expect(contextQ.participantId).toBe(participantIdQ);

      const qAppend = await appendEvent({
        authToken: qBearer,
        controlEpoch: contextQ.controlEpoch,
        eventId: `evt_e2e_rest_2p_q_${runId}`,
        instanceId: contextQ.instanceId,
        payload: { cohort: "rest-two-instance", producer: "q", role: "independent" },
        producerId: participantIdQ,
      });
      expect(qAppend.status).toBe(201);
      const qCreated = parseRestJson(appendEventResponseSchema, qAppend.body, "Q append");
      expect(qCreated.event.producerId).toBe(participantIdQ);

      // B's lease is unaffected by Q's independent acquisition: B still appends
      // under the same captured epoch. Q is a second authorized participant
      // producer here, not a validated Graybox state producer; that producer
      // check belongs to Graybox replay and admission, not this transport proof.
      const bAppend = await appendEvent({
        authToken: pBearer,
        controlEpoch: contextB.controlEpoch,
        eventId: `evt_e2e_rest_2p_b2_${runId}`,
        instanceId: contextB.instanceId,
        payload: { cohort: "rest-two-instance", producer: "b", role: "post-q" },
        producerId: participantIdP,
      });
      expect(bAppend.status).toBe(201);
      const bCreated = parseRestJson(appendEventResponseSchema, bAppend.body, "B post-Q append");
      expect(bCreated.event.producerId).toBe(participantIdP);
    }

    // pages the full session log from zero and verifies positions, producers, and order.
    {
      const scanned: ListedEvent[] = [];
      let afterSeq = 0;
      let pages = 0;
      let reachedTail = false;
      while (!reachedTail) {
        const response = await fetchJson(
          baseUrl,
          `/sessions/${sessionId}/events?after=${afterSeq}&limit=1`,
          { authToken: pBearer },
        );
        expect(response.status).toBe(200);
        const page = parseRestJson(eventListResponseSchema, response.body, "event list page");
        expect(page.pagination.afterSeq).toBe(afterSeq);
        expect(page.pagination.limit).toBe(1);
        expect(page.pagination.returned).toBe(page.events.length);
        expect(page.events.length).toBeLessThanOrEqual(1);
        scanned.push(...page.events);
        if (page.events.length > 0) {
          expect(page.pagination.nextAfterSeq).toBe(page.events[0]?.seq);
        }
        if (page.pagination.hasMore) {
          expect(page.events.length).toBe(1);
          afterSeq = page.pagination.nextAfterSeq;
        } else {
          reachedTail = true;
        }
        pages += 1;
        if (pages > 1_000) {
          throw new Error("event pagination did not reach the tail");
        }
      }

      // Exact contiguous positions from one: no gaps in the accepted sequence.
      expect(scanned.map((event) => event.seq)).toEqual(
        Array.from({ length: scanned.length }, (_, index) => index + 1),
      );

      // The fixed-id event exists exactly once with its committed payload.
      const fixedEvents = scanned.filter((event) => event.eventId === fixedEventId);
      expect(fixedEvents).toHaveLength(1);
      expect(fixedEvents[0]?.seq).toBe(committedSequence);
      expect(fixedEvents[0]?.producerId).toBe(participantIdP);
      expect(fixedEvents[0]?.payload).toEqual(fixedPayload);

      // Both participant producers and native control positions are present,
      // with Tether the only other producer.
      const producers = new Set(scanned.map((event) => event.producerId));
      expect(producers.has(participantIdP)).toBe(true);
      expect(producers.has(participantIdQ)).toBe(true);
      expect(
        [...producers].every((producer) =>
          [participantIdP, participantIdQ, systemProducerId].includes(producer),
        ),
      ).toBe(true);
      expect(
        scanned.some(
          (event) =>
            event.producerId === systemProducerId &&
            (event.type === "participant.joined" || event.type === "participant.updated"),
        ),
      ).toBe(true);
      expect(
        scanned.some(
          (event) =>
            event.producerId === systemProducerId && event.type === "participant.heartbeat",
        ),
      ).toBe(true);

      // Global sequence order across producers: the fixed event precedes Q's
      // append, which precedes B's post-Q append.
      const fixedSeq = fixedEvents[0]?.seq ?? 0;
      const qSeq = scanned.find((event) => event.producerId === participantIdQ)?.seq ?? 0;
      const bPostQSeq = scanned
        .filter((event) => event.producerId === participantIdP)
        .reduce((latest, event) => Math.max(latest, event.seq), 0);
      expect(fixedSeq).toBeGreaterThan(0);
      expect(qSeq).toBeGreaterThan(fixedSeq);
      expect(bPostQSeq).toBeGreaterThan(qSeq);
    }
  }, 30_000);
});

/**
 * RFC-12 forced-race deterministic append/acquisition choreography.
 *
 * Scope: Tether transport and fencing evidence under contention. Each case
 * spins up two app pools against one unique disposable database, each with a
 * distinct synthetic `application_name`, and uses separate direct `pg.Client`
 * connections for the blocker (C) and the observer (O). Public REST requests
 * run through raw HTTP with no automatic heartbeat timers so the only waits
 * observed come from the choreography itself.
 *
 * The forced race uses one Participant with the same logical instance id
 * across both app pools because fresh-acquisition rotation on the same
 * instance is the qualifying invariant, not a successful competing-instance
 * takeover. The graph evidence must show the static wait identity from the
 * pinned source lines (db.ts:617 advisory helper; db.ts:1219, 1352
 * current-lease SELECT FOR UPDATE; db.ts:2029 event-id advisory helper;
 * db.ts:2356/2373 participant presence SELECT FOR UPDATE) backed by
 * pg_blocking_pids and granted/ungranted pg_locks comparisons.
 *
 * The inverse case rejects old append in `renewControlLease` preflight, not
 * in the atomic append guard; that distinction is preserved by the comment in
 * the inverse case body. No Graybox barrier schema is exercised or claimed.
 */

const raceRestControlE2e = process.env.E2E === "true" ? describe : describe.skip;

/** Builds the disposable test database URL with race-specific session settings. */
function buildRaceDatabaseUrl(databaseName: string): string {
  const url = new URL(adminDatabaseUrl);
  url.pathname = `/${databaseName}`;
  // Fixture-only deadlines wired through libpq `options`. The race window
  // runs against the source's 60-second REST lease TTL; these settings bound
  // a stuck wait at the SQL layer without leaking into production.
  url.searchParams.set(
    "options",
    [
      "-c lock_timeout=10000",
      "-c statement_timeout=12000",
      "-c idle_in_transaction_session_timeout=12000",
      "-c default_transaction_isolation=read\\ committed",
      "-c timezone=UTC",
    ].join(" "),
  );
  return url.toString();
}

/** Computes a synthetic application name for one race-pool role. */
function raceApplicationName(role: string, runId: string): string {
  const safeRunId = runId.replaceAll("-", "_");
  return `tether_e2e_race_${role}_${safeRunId}`;
}

/** Augments a database URL with a per-pool `application_name` value. */
function withApplicationName(databaseUrl: string, applicationName: string): string {
  const url = new URL(databaseUrl);
  url.searchParams.set("application_name", applicationName);
  return url.toString();
}

/** Inert test-only custom event type for marker rows in the race fixture. */
const raceMarkerEventType = "e2e.rest-control.race.marker.v1";

/** Local response shape for one race-acquisition public REST response. */
const raceAcquisitionResponseSchema = z.object({
  acquisitionId: z.string().min(1),
  acquisitionStatus: z.enum(["claimed", "replayed", "superseded"]),
  controlEpoch: z.number().int().positive(),
  leaseExpiresAt: z.string().datetime({ offset: true }),
  participant: z.record(z.string(), z.unknown()),
  registrationStatus: z.enum(["joined", "refreshed", "updated"]),
  renewAfterMs: z.number().int().positive(),
});

/** Parsed fields from one race-acquisition public REST response. */
type RaceAcquisitionResponse = z.infer<typeof raceAcquisitionResponseSchema>;

/**
 * Race fixture shared between the forward and inverse cases. Each case uses a
 * fresh synthetic Session id, fresh acquisition ids, fresh Participant, fresh
 * inert marker payload, and the same logical instance id across both pools.
 */

/**
 * Issues one race public REST participant registration and returns the raw
 * response, leaving parsing to the caller. Use this when the call must be
 * kept in flight so observers can inspect the wait state; track it through
 * `trackRaceRequest` so cleanup never leaves an unhandled rejection.
 */
function raceAcquireRaw(input: {
  readonly acquisitionId: string;
  readonly authToken: string;
  readonly baseUrl: string;
  readonly instanceId: string;
  readonly sessionId: string;
  readonly participantId: string;
}): Promise<RawHttpResponse> {
  const timeout = AbortSignal.timeout(20_000);
  return fetch(`${input.baseUrl}/sessions/${input.sessionId}/participants`, {
    body: JSON.stringify({
      acquisitionId: input.acquisitionId,
      capabilities: {},
      controlChannel: "rest",
      instanceId: input.instanceId,
      runtimeKind: "generic_agent",
      participantId: input.participantId,
    }),
    headers: {
      "content-type": "application/json",
      authorization: `Bearer ${input.authToken}`,
    },
    method: "POST",
    signal: timeout,
  })
    .then(async (response) => {
      const text = await response.text();
      let body: unknown = null;
      if (text.length > 0) {
        try {
          body = JSON.parse(text) as unknown;
        } catch {
          body = null;
        }
      }
      return { body, status: response.status };
    })
    .catch((error: unknown): RawHttpResponse => {
      return {
        body: { error: error instanceof Error ? error.name : "unknown" },
        status: 0,
      };
    });
}

/**
 * Issues one race public REST participant registration and awaits it
 * synchronously, parsing the captured control context. Use this only when
 * the caller is ready to wait for the full response, never for in-flight
 * race observation.
 */
async function raceAcquire(input: {
  readonly acquisitionId: string;
  readonly authToken: string;
  readonly baseUrl: string;
  readonly instanceId: string;
  readonly sessionId: string;
  readonly participantId: string;
}): Promise<RaceAcquisitionResponse> {
  const response = await raceAcquireRaw(input);
  if (response.status !== 201 && response.status !== 200) {
    throw new Error(`race acquire failed with status ${response.status}`);
  }
  return parseRestJson(raceAcquisitionResponseSchema, response.body, "race acquire");
}

/**
 * Issues one race public REST event append with the supplied control context.
 * Returns the raw response so callers can drive the choreography without
 * immediately awaiting the underlying transport.
 */
function raceStartAppend(input: {
  readonly authToken: string;
  readonly baseUrl: string;
  readonly body: {
    readonly controlEpoch: number;
    readonly eventId: string;
    readonly instanceId: string;
    readonly payload: Record<string, unknown>;
    readonly producerId: string;
    readonly sessionId: string;
    readonly type: string;
  };
}): Promise<RawHttpResponse> {
  // The forced window uses raw HTTP fetch with a 20-second request timeout
  // so an aborted or stuck request resolves cleanly during cleanup rather
  // than leaving an unhandled rejection behind.
  const timeout = AbortSignal.timeout(20_000);
  return fetch(`${input.baseUrl}/sessions/${input.body.sessionId}/events`, {
    body: JSON.stringify(input.body),
    headers: {
      "content-type": "application/json",
      authorization: `Bearer ${input.authToken}`,
    },
    method: "POST",
    signal: timeout,
  })
    .then(async (response) => {
      const text = await response.text();
      let body: unknown = null;
      if (text.length > 0) {
        try {
          body = JSON.parse(text) as unknown;
        } catch {
          body = null;
        }
      }
      return { body, status: response.status };
    })
    .catch((error: unknown): RawHttpResponse => {
      return {
        body: { error: error instanceof Error ? error.name : "unknown" },
        status: 0,
      };
    });
}

raceRestControlE2e("tether e2e forced append/acquisition race", () => {
  const runId = randomUUID();
  const runIdSlug = runId.replaceAll("-", "_");
  const databaseName = `tether_e2e_race_${runIdSlug}`;
  const databaseUrl = buildRaceDatabaseUrl(databaseName);

  const oldAppName = raceApplicationName("old", runId);
  const newAppName = raceApplicationName("new", runId);

  const instanceId = `inst_e2e_race_${runId}`;
  const oldParticipantId = `part_e2e_race_old_${runIdSlug}`;
  const newParticipantId = `part_e2e_race_new_${runIdSlug}`;

  const oldSessionId = `sess_e2e_race_old_${runIdSlug}`;
  const newSessionId = `sess_e2e_race_new_${runIdSlug}`;

  const oldAcquisitionId = `acq_e2e_race_old_${runId}`;
  const oldAcquisitionIdRotated = `acq_e2e_race_old_rot_${runId}`;
  const newAcquisitionId = `acq_e2e_race_new_${runId}`;
  const newAcquisitionIdRotated = `acq_e2e_race_new_rot_${runId}`;
  const oldEventId = `evt_e2e_race_old_${runId}`;
  const markerEventId = `evt_e2e_race_marker_${runId}`;
  const inverseOldEventId = `evt_e2e_race_inverse_old_${runId}`;
  const inverseMarkerEventId = `evt_e2e_race_inverse_marker_${runId}`;

  let databaseCreationAttempted = false;
  let setupGrantBearer = "";
  let oldParticipantBearer = "";
  let newParticipantBearer = "";
  let oldApp: AppServer | null = null;
  let newApp: AppServer | null = null;
  let oldBaseUrl = "";
  let newBaseUrl = "";
  let oldPool: DatabasePool | null = null;
  let newPool: DatabasePool | null = null;
  let observer: pg.Client | null = null;
  let blocker: pg.Client | null = null;
  let blockerPid = 0;
  let poolErrorObserved = false;
  const pendingRaceRequests = new Set<Promise<RawHttpResponse>>();
  const knownRaceLeases = new Map<
    string,
    {
      authToken: string;
      participantId: string;
      controlEpoch: number;
    }
  >();

  beforeAll(async () => {
    // CREATE may commit before its connection reports an error. Cleanup still
    // attempts this unique database if setup has an unknown outcome.
    databaseCreationAttempted = true;
    await createDatabase(databaseName);
    const oldPoolUrl = withApplicationName(databaseUrl, oldAppName);
    const newPoolUrl = withApplicationName(databaseUrl, newAppName);
    oldPool = createPool(oldPoolUrl, { max: 4 });
    newPool = createPool(newPoolUrl, { max: 4 });
    // A backend can become idle between cleanup's state query and its
    // termination. Preserve a fixed failure flag instead of an unhandled
    // pool error or raw connection diagnostics.
    oldPool.pool.on("error", () => {
      poolErrorObserved = true;
    });
    newPool.pool.on("error", () => {
      poolErrorObserved = true;
    });
    await migrate(oldPool);

    const grantLifecycle = createAuthGrantLifecycle({
      activeKid: testAuthSigningKid,
      issuanceEnabled: true,
      issuer: fixtureAuthIssuer,
      secrets: { [testAuthSigningKid]: testAuthSigningSecret },
      stores: createAuthPersistenceStores(oldPool),
    });

    const appOptions = {
      auth: {
        activeKid: testAuthSigningKid,
        allowLegacyTokens: false,
        issuer: fixtureAuthIssuer,
        mode: "required" as const,
        secrets: { [testAuthSigningKid]: testAuthSigningSecret },
      },
      sessionService: { controlEpochEnforcement: true },
      taskClaimSweeper: { intervalMs: 0 },
    };
    oldApp = createAppServer(oldPool, appOptions);
    newApp = createAppServer(newPool, appOptions);
    const oldPort = await oldApp.listen(0, "127.0.0.1");
    const newPort = await newApp.listen(0, "127.0.0.1");
    oldBaseUrl = `http://127.0.0.1:${oldPort}`;
    newBaseUrl = `http://127.0.0.1:${newPort}`;

    // Confirm both pools report the expected application_name on a checked
    // out connection so waiter rows in pg_stat_activity can be filtered to
    // exactly this run's app pool.
    {
      const oldCheck = await oldPool.pool.connect();
      try {
        const result = await oldCheck.query<{
          readonly name: string;
          readonly isolation: string;
          readonly lockTimeout: string;
          readonly statementTimeout: string;
          readonly idleTimeout: string;
        }>(
          "SELECT current_setting('application_name') AS name, current_setting('default_transaction_isolation') AS isolation, current_setting('lock_timeout') AS \"lockTimeout\", current_setting('statement_timeout') AS \"statementTimeout\", current_setting('idle_in_transaction_session_timeout') AS \"idleTimeout\"",
        );
        if (
          result.rows[0]?.name !== oldAppName ||
          result.rows[0]?.isolation !== "read committed" ||
          result.rows[0]?.lockTimeout !== "10s" ||
          result.rows[0]?.statementTimeout !== "12s" ||
          result.rows[0]?.idleTimeout !== "12s"
        ) {
          throw new Error("old pool application_name mismatch");
        }
      } finally {
        oldCheck.release();
      }
    }
    {
      const newCheck = await newPool.pool.connect();
      try {
        const result = await newCheck.query<{
          readonly name: string;
          readonly isolation: string;
          readonly lockTimeout: string;
          readonly statementTimeout: string;
          readonly idleTimeout: string;
        }>(
          "SELECT current_setting('application_name') AS name, current_setting('default_transaction_isolation') AS isolation, current_setting('lock_timeout') AS \"lockTimeout\", current_setting('statement_timeout') AS \"statementTimeout\", current_setting('idle_in_transaction_session_timeout') AS \"idleTimeout\"",
        );
        if (
          result.rows[0]?.name !== newAppName ||
          result.rows[0]?.isolation !== "read committed" ||
          result.rows[0]?.lockTimeout !== "10s" ||
          result.rows[0]?.statementTimeout !== "12s" ||
          result.rows[0]?.idleTimeout !== "12s"
        ) {
          throw new Error("new pool application_name mismatch");
        }
      } finally {
        newCheck.release();
      }
    }

    // Service-scoped setup authority: the only identity allowed to create
    // both race Sessions over the public route.
    const setupGrant = await grantLifecycle.create({
      actorSubject: "svc_e2e_race_setup",
      reasonCode: "bootstrap",
      role: "admin",
      sessionScope: "*",
      source: "bootstrap",
      subject: "svc_e2e_race_setup",
      ttlSeconds: 3_600,
    });
    setupGrantBearer = setupGrant.bearer;

    for (const sessionId of [oldSessionId, newSessionId]) {
      const response = await fetchJson(oldBaseUrl, "/sessions", {
        authToken: setupGrantBearer,
        body: { sessionId },
      });
      if (response.status !== 201) {
        throw new Error(`race session creation failed with status ${response.status}`);
      }
    }

    // Session-scoped participant grants: the grant subject becomes the
    // authenticated participant id, so P mutations are identity-bound.
    const oldPartGrant = await grantLifecycle.create({
      actorSubject: "svc_e2e_race_setup",
      reasonCode: "bootstrap",
      role: "participant",
      sessionScope: oldSessionId,
      source: "bootstrap",
      subject: oldParticipantId,
      ttlSeconds: 3_600,
    });
    oldParticipantBearer = oldPartGrant.bearer;
    const newPartGrant = await grantLifecycle.create({
      actorSubject: "svc_e2e_race_setup",
      reasonCode: "bootstrap",
      role: "participant",
      sessionScope: newSessionId,
      source: "bootstrap",
      subject: newParticipantId,
      ttlSeconds: 3_600,
    });
    newParticipantBearer = newPartGrant.bearer;

    // Observer: autocommit only, never inside a long transaction. Uses the
    // same cluster role as the apps so pg_stat_activity is fully visible.
    observer = await connectRaceDirectClient({
      applicationName: raceApplicationName("observer", runId),
      connectionString: buildRaceDatabaseUrl(databaseName),
    });
    await observer.query("SET statement_timeout = 1000");
    const activityCheck = await observer.query<{ readonly track_activities: string }>(
      "SHOW track_activities",
    );
    if (activityCheck.rows[0]?.track_activities !== "on") {
      throw new Error("track_activities is not on for race observer");
    }

    // Blocker: separate direct connection holding either the event-id advisory
    // lock (forward) or the participant presence FOR UPDATE (inverse).
    blocker = await connectRaceDirectClient({
      applicationName: raceApplicationName("blocker", runId),
      connectionString: buildRaceDatabaseUrl(databaseName),
    });
    const blockerPidResult = await blocker.query<{ readonly pid: number }>(
      "SELECT pg_backend_pid() AS pid",
    );
    const pid = blockerPidResult.rows[0]?.pid;
    if (typeof pid !== "number") {
      throw new Error("blocker did not report a backend PID");
    }
    blockerPid = pid;
  }, 30_000);

  afterEach(async () => {
    const failures: string[] = [];
    const attempt = async (label: string, run: () => Promise<unknown>, timeoutMs = 5_000) => {
      try {
        await boundRaceCleanup(run(), timeoutMs);
      } catch {
        failures.push(label);
      }
    };
    // Release the pause even when an assertion failed, then settle the HTTP
    // work before starting another case. HTTP abort alone is not SQL rollback.
    if (blocker !== null) {
      await attempt("rollback case blocker", () => blocker?.query("ROLLBACK") ?? Promise.resolve());
    }
    await attempt("settle case requests", () => Promise.all([...pendingRaceRequests]), 22_000);
    if (observer !== null) {
      try {
        await awaitRaceBackendsIdle(observer, [oldAppName, newAppName]);
      } catch {
        failures.push("database work did not settle");
        await attempt(
          "terminate active fixture backends",
          () =>
            observer?.query(
              `SELECT pg_terminate_backend(pid) FROM pg_stat_activity
           WHERE datname = current_database() AND application_name = ANY($1::text[])
             AND state <> 'idle'`,
              [[oldAppName, newAppName]],
            ) ?? Promise.resolve(),
        );
        await attempt(
          "confirm fixture backends settled",
          () =>
            observer === null
              ? Promise.resolve()
              : awaitRaceBackendsIdle(observer, [oldAppName, newAppName]),
          6_000,
        );
      }
    }
    for (const [sessionId, lease] of knownRaceLeases) {
      await attempt("release known race lease", async () => {
        const response = await fetchJson(
          newBaseUrl,
          `/sessions/${sessionId}/participants/${lease.participantId}/control/release`,
          {
            authToken: lease.authToken,
            body: { instanceId, controlEpoch: lease.controlEpoch },
          },
        );
        if (response.status !== 200 && response.status !== 409) {
          throw new Error("race lease release failed");
        }
      });
    }
    knownRaceLeases.clear();
    if (failures.length > 0) throw new Error(`race case cleanup failures: ${failures.join("; ")}`);
  }, 60_000);

  afterAll(async () => {
    // Every step runs even when an earlier one fails; failures are collected
    // and reported together instead of silently succeeding. Step labels are
    // fixed so reviewers can map them back to the plan's cleanup order.
    const cleanupFailures: string[] = [];
    const attemptCleanup = async (step: string, run: () => Promise<void>): Promise<void> => {
      try {
        await boundRaceCleanup(run());
      } catch {
        cleanupFailures.push(step);
      }
    };

    // 1. Roll back the blocker first so it stops holding the race-relevant
    // lock and stops pinning the row/identity the apps may still inspect.
    if (blocker !== null) {
      await attemptCleanup("rollback race blocker", async () => {
        await blocker?.query("ROLLBACK");
      });
      await attemptCleanup(
        "end race blocker connection",
        () => blocker?.end() ?? Promise.resolve(),
      );
    }

    // 2. Close the observer after the blocker is released.
    if (observer !== null) {
      await attemptCleanup(
        "end race observer connection",
        () => observer?.end() ?? Promise.resolve(),
      );
    }

    // 3. Close both app servers, draining in-flight requests.
    if (oldApp !== null) {
      await attemptCleanup("close race old app server", () => oldApp?.close() ?? Promise.resolve());
    }
    if (newApp !== null) {
      await attemptCleanup("close race new app server", () => newApp?.close() ?? Promise.resolve());
    }

    // 4. End both app pools.
    if (oldPool !== null) {
      await attemptCleanup("end race old pool", () => oldPool?.end() ?? Promise.resolve());
    }
    if (newPool !== null) {
      await attemptCleanup("end race new pool", () => newPool?.end() ?? Promise.resolve());
    }

    // 5. Drop only this run's disposable database and confirm it is gone.
    if (databaseCreationAttempted) {
      await attemptCleanup("drop race disposable database", () => dropDatabase(databaseName));
      await attemptCleanup("confirm race database absence", async () => {
        const adminPool = new pg.Pool({
          connectionString: adminDatabaseUrl,
          connectionTimeoutMillis: 5_000,
          statement_timeout: 5_000,
        });
        try {
          const result = await adminPool.query<{ readonly exists: boolean }>(
            "SELECT EXISTS (SELECT 1 FROM pg_database WHERE datname = $1) AS exists",
            [databaseName],
          );
          if (result.rows[0]?.exists === true) {
            throw new Error("race database still present after drop");
          }
        } finally {
          await adminPool.end();
        }
      });
    }

    if (poolErrorObserved) cleanupFailures.push("app pool connection error");
    if (cleanupFailures.length > 0) {
      throw new Error(`race cleanup failures: ${cleanupFailures.join("; ")}`);
    }
  }, 60_000);

  /** Tracks handled requests until their bounded HTTP operation settles. */
  const trackRaceRequest = (pending: Promise<RawHttpResponse>): Promise<RawHttpResponse> => {
    pendingRaceRequests.add(pending);
    void pending.then(
      () => pendingRaceRequests.delete(pending),
      () => pendingRaceRequests.delete(pending),
    );
    return pending;
  };

  it("forward race: old append owns the lease row", async () => {
    // Synthetic proof parameters for this run:
    //   S = oldSessionId, P = oldParticipantId, I = instanceId,
    //   E = oldEventId, H = markerEventId.
    // The old pool holds the lease through a fresh REST acquisition before
    // the forced window opens, so its connection is the one observed under
    // application_name = oldAppName.
    const forwardAcquire = await raceAcquire({
      acquisitionId: oldAcquisitionId,
      authToken: oldParticipantBearer,
      baseUrl: oldBaseUrl,
      instanceId,
      sessionId: oldSessionId,
      participantId: oldParticipantId,
    });
    expect(forwardAcquire.acquisitionStatus).toBe("claimed");
    expect(forwardAcquire.controlEpoch).toBeGreaterThan(0);
    const epochOld = forwardAcquire.controlEpoch;
    knownRaceLeases.set(oldSessionId, {
      authToken: oldParticipantBearer,
      participantId: oldParticipantId,
      controlEpoch: epochOld,
    });

    // C: BEGIN, then transaction-scoped advisory lock keyed by
    // ('session_event_id', E). The advisory lock helper at db.ts:617 takes
    // transaction-scoped locks and releases them on rollback.
    if (blocker === null) {
      throw new Error("race blocker is not connected");
    }
    await blocker.query("BEGIN");
    await blocker.query("SELECT pg_advisory_xact_lock(hashtext($1::text), hashtext($2::text))", [
      "session_event_id",
      oldEventId,
    ]);
    if (blockerPid === 0) {
      throw new Error("race blocker PID was not captured");
    }

    // Start old append E under epoch e through the old pool, then observe.
    const oldAppendRequest = trackRaceRequest(
      raceStartAppend({
        authToken: oldParticipantBearer,
        baseUrl: oldBaseUrl,
        body: {
          controlEpoch: epochOld,
          eventId: oldEventId,
          instanceId,
          payload: { case: "old" },
          producerId: oldParticipantId,
          sessionId: oldSessionId,
          type: raceMarkerEventType,
        },
      }),
    );

    // Wait for the old pool's waiter to land in the Lock/advisory wait on
    // the event-id advisory helper, blocked by the blocker PID.
    if (observer === null) {
      throw new Error("race observer is not connected");
    }
    const oldWaiter = await observeRaceWaiter({
      applicationName: oldAppName,
      deadlineMs: 3_000,
      expectedFingerprint: raceAdvisoryHelperFingerprint,
      expectedWaitEvent: "advisory",
      observer,
      requiredBlocker: blockerPid,
    });
    expect(oldWaiter.expectedStatement).toBe(true);
    expect(oldWaiter.waitEventType).toBe("Lock");
    expect(oldWaiter.waitEvent).toBe("advisory");
    expect(oldWaiter.blockers).toContain(blockerPid);
    const oldWaiterPid = oldWaiter.pid;
    await observeRaceAdvisoryPair({
      deadlineMs: 3_000,
      holderPid: blockerPid,
      waiterPid: oldWaiterPid,
      observer,
    });

    // Start the replacement registration through the new pool with the same
    // logical instance I and a fresh acquisitionId. It must wait on the
    // old pool's lease-row lock (transactionid) after holding the Participant
    // advisory.
    const replacementRequest = trackRaceRequest(
      raceAcquireRaw({
        acquisitionId: oldAcquisitionIdRotated,
        authToken: oldParticipantBearer,
        baseUrl: newBaseUrl,
        instanceId,
        sessionId: oldSessionId,
        participantId: oldParticipantId,
      }),
    );

    const newWaiter = await observeRaceWaiter({
      applicationName: newAppName,
      deadlineMs: 3_000,
      expectedFingerprint: raceCurrentLeaseForUpdateFingerprint,
      expectedWaitEvent: "transactionid",
      observer,
      requiredBlocker: oldWaiterPid,
    });
    expect(newWaiter.expectedStatement).toBe(true);
    expect(newWaiter.waitEventType).toBe("Lock");
    expect(newWaiter.waitEvent).toBe("transactionid");
    expect(newWaiter.blockers).toContain(oldWaiterPid);

    await observeRaceWaiter({
      applicationName: oldAppName,
      deadlineMs: 3_000,
      expectedFingerprint: raceAdvisoryHelperFingerprint,
      expectedPid: oldWaiterPid,
      expectedWaitEvent: "advisory",
      observer,
      requiredBlocker: blockerPid,
    });

    // Release C: the old append's advisory wait resolves, then the
    // replacement's lease-row wait resolves after the old append commits.
    await blocker.query("ROLLBACK");
    const [oldAppendResponse, replacementResponse] = await Promise.all([
      oldAppendRequest,
      replacementRequest,
    ]);

    expect(oldAppendResponse.status).toBe(201);
    const oldAppended = parseRestJson(
      appendEventResponseSchema,
      oldAppendResponse.body,
      "forward old append",
    );
    expect(oldAppended.status).toBe("created");
    expect(oldAppended.event.eventId).toBe(oldEventId);
    expect(oldAppended.event.payload).toEqual({ case: "old" });
    const oldAppendSeq = oldAppended.event.seq;

    expect(replacementResponse.status).toBe(201);
    const replaced = parseRestJson(
      raceAcquisitionResponseSchema,
      replacementResponse.body,
      "forward replacement acquire",
    );
    expect(replaced.acquisitionId).toBe(oldAcquisitionIdRotated);
    expect(replaced.acquisitionStatus).toBe("superseded");
    expect(replaced.controlEpoch).toBeGreaterThan(epochOld);
    knownRaceLeases.set(oldSessionId, {
      authToken: oldParticipantBearer,
      participantId: oldParticipantId,
      controlEpoch: replaced.controlEpoch,
    });

    // Marker append under the replacement epoch. Recorded as H.
    const markerResponse = await fetchJson(newBaseUrl, `/sessions/${oldSessionId}/events`, {
      authToken: oldParticipantBearer,
      body: {
        controlEpoch: replaced.controlEpoch,
        eventId: markerEventId,
        instanceId,
        payload: { case: "marker" },
        producerId: oldParticipantId,
        type: raceMarkerEventType,
      },
      method: "POST",
      timeoutMs: 20_000,
    });
    expect(markerResponse.status).toBe(201);
    const markerAppended = parseRestJson(
      appendEventResponseSchema,
      markerResponse.body,
      "forward marker append",
    );
    expect(markerAppended.status).toBe("created");
    expect(markerAppended.event.eventId).toBe(markerEventId);
    const markerSeq = markerAppended.event.seq;
    expect(markerSeq).toBeGreaterThan(oldAppendSeq);

    // Paginate the full public log from zero and verify E exists exactly
    // once below H and the marker exists exactly once at H, with contiguous
    // native/custom sequence positions.
    const scanned: ListedEvent[] = [];
    let afterSeq = 0;
    let reachedTail = false;
    let pages = 0;
    while (!reachedTail) {
      const pageResponse = await fetchJson(
        newBaseUrl,
        `/sessions/${oldSessionId}/events?after=${afterSeq}&limit=1`,
        { authToken: oldParticipantBearer, timeoutMs: 20_000 },
      );
      expect(pageResponse.status).toBe(200);
      const page = parseRestJson(eventListResponseSchema, pageResponse.body, "race event page");
      expect(page.pagination.afterSeq).toBe(afterSeq);
      scanned.push(...page.events);
      if (page.pagination.hasMore) {
        afterSeq = page.pagination.nextAfterSeq;
        expect(page.events.length).toBe(1);
      } else {
        reachedTail = true;
      }
      pages += 1;
      if (pages > 1_000) {
        throw new Error("forward race pagination did not reach the tail");
      }
    }

    // Contiguous sequences from one with no gaps.
    expect(scanned.map((event) => event.seq)).toEqual(
      Array.from({ length: scanned.length }, (_, index) => index + 1),
    );

    const oldEvents = scanned.filter((event) => event.eventId === oldEventId);
    expect(oldEvents).toHaveLength(1);
    expect(oldEvents[0]?.payload).toEqual({ case: "old" });
    expect(oldEvents[0]?.seq).toBeLessThan(markerSeq);

    const markerEvents = scanned.filter((event) => event.eventId === markerEventId);
    expect(markerEvents).toHaveLength(1);
    expect(markerEvents[0]?.seq).toBe(markerSeq);
    expect(markerEvents[0]?.payload).toEqual({ case: "marker" });
  }, 30_000);

  it("inverse race: replacement owns the lease row", async () => {
    // Synthetic proof parameters for this run on a fresh Session:
    //   S = newSessionId, P = newParticipantId, I = instanceId,
    //   E = inverseOldEventId, H = inverseMarkerEventId.
    // Inverse pre-acquire uses the new pool so the replacement that
    // follows it can be observed under application_name = newAppName.
    const inverseAcquire = await raceAcquire({
      acquisitionId: newAcquisitionId,
      authToken: newParticipantBearer,
      baseUrl: newBaseUrl,
      instanceId,
      sessionId: newSessionId,
      participantId: newParticipantId,
    });
    expect(inverseAcquire.acquisitionStatus).toBe("claimed");
    const epochOld = inverseAcquire.controlEpoch;
    knownRaceLeases.set(newSessionId, {
      authToken: newParticipantBearer,
      participantId: newParticipantId,
      controlEpoch: epochOld,
    });

    // C: BEGIN, then SELECT ... FOR UPDATE on the existing presence row.
    // The participant advisory is NOT held by C; only the tuple lock is.
    if (blocker === null) {
      throw new Error("race blocker is not connected");
    }
    if (observer === null) {
      throw new Error("race observer is not connected");
    }
    await blocker.query("BEGIN");
    const presenceResult = await blocker.query<{ readonly participant_id: string }>(
      `SELECT participant_id FROM participants
       WHERE session_id = $1 AND participant_id = $2
       FOR UPDATE`,
      [newSessionId, newParticipantId],
    );
    expect(presenceResult.rowCount).toBe(1);
    if (blockerPid === 0) {
      throw new Error("race blocker PID was not captured");
    }

    // Start the same-instance fresh-acquisition replacement through the
    // new pool. By source order it holds the participant advisory, has
    // superseded and inserted the new lease, and is now blocked inside
    // upsertParticipantWithClient at the presence FOR UPDATE.
    const replacementRequest = trackRaceRequest(
      raceAcquireRaw({
        acquisitionId: newAcquisitionIdRotated,
        authToken: newParticipantBearer,
        baseUrl: newBaseUrl,
        instanceId,
        sessionId: newSessionId,
        participantId: newParticipantId,
      }),
    );

    const newWaiter = await observeRaceWaiter({
      applicationName: newAppName,
      deadlineMs: 3_000,
      expectedFingerprint: racePresenceForUpdateFingerprint,
      expectedWaitEvent: "transactionid",
      observer,
      requiredBlocker: blockerPid,
    });
    expect(newWaiter.expectedStatement).toBe(true);
    expect(newWaiter.waitEventType).toBe("Lock");
    expect(newWaiter.waitEvent).toBe("transactionid");
    expect(newWaiter.blockers).toContain(blockerPid);
    const newWaiterPid = newWaiter.pid;

    // Start the old public append under epoch e through the old pool. The
    // renewal preflight acquires the participant advisory first and parks
    // there on the waiter that already owns it.
    const oldAppendRequest = trackRaceRequest(
      raceStartAppend({
        authToken: newParticipantBearer,
        baseUrl: oldBaseUrl,
        body: {
          controlEpoch: epochOld,
          eventId: inverseOldEventId,
          instanceId,
          payload: { case: "old" },
          producerId: newParticipantId,
          sessionId: newSessionId,
          type: raceMarkerEventType,
        },
      }),
    );

    const oldWaiter = await observeRaceWaiter({
      applicationName: oldAppName,
      deadlineMs: 3_000,
      expectedFingerprint: raceAdvisoryHelperFingerprint,
      expectedWaitEvent: "advisory",
      observer,
      requiredBlocker: newWaiterPid,
    });
    expect(oldWaiter.expectedStatement).toBe(true);
    expect(oldWaiter.waitEventType).toBe("Lock");
    expect(oldWaiter.waitEvent).toBe("advisory");
    expect(oldWaiter.blockers).toContain(newWaiterPid);
    const oldWaiterPid = oldWaiter.pid;

    // The waiter that holds the participant advisory (the new waiter)
    // must still be parked on the blocker's tuple lock. This proves the
    // Participant advisory and the presence tuple lock were acquired in
    // source order and no other identity is in between.
    const newStillWaiting = await observeRaceWaiter({
      applicationName: newAppName,
      deadlineMs: 3_000,
      expectedFingerprint: racePresenceForUpdateFingerprint,
      expectedPid: newWaiterPid,
      expectedWaitEvent: "transactionid",
      observer,
      requiredBlocker: blockerPid,
    });
    expect(newStillWaiting.expectedStatement).toBe(true);
    expect(newStillWaiting.blockers).toContain(blockerPid);

    // Confirm the participant advisory identity is held by the new waiter
    // and waited on by the old waiter; the inner ungranted row must use
    // the same classid/objid as the granted advisory on the new waiter.
    await observeRaceAdvisoryPair({
      deadlineMs: 3_000,
      holderPid: newWaiterPid,
      waiterPid: oldWaiterPid,
      observer,
    });

    // Release C: Q proceeds and supersedes; P then proceeds and the renewal
    // preflight rejects the supplied epoch against the new current epoch.
    // This rejection occurs inside renewControlLease (the public append
    // preflight) and does NOT exercise the atomic append guard's own
    // rejection branch - that distinction is preserved by the test comment.
    await blocker.query("ROLLBACK");
    const [replacementResponse, oldAppendResponse] = await Promise.all([
      replacementRequest,
      oldAppendRequest,
    ]);

    expect(replacementResponse.status).toBe(201);
    const replaced = parseRestJson(
      raceAcquisitionResponseSchema,
      replacementResponse.body,
      "inverse replacement acquire",
    );
    expect(replaced.acquisitionId).toBe(newAcquisitionIdRotated);
    expect(replaced.acquisitionStatus).toBe("superseded");
    expect(replaced.controlEpoch).toBeGreaterThan(epochOld);
    knownRaceLeases.set(newSessionId, {
      authToken: newParticipantBearer,
      participantId: newParticipantId,
      controlEpoch: replaced.controlEpoch,
    });

    expect(oldAppendResponse.status).toBe(409);
    const stale = parseRestJson(
      controlErrorResponseSchema,
      oldAppendResponse.body,
      "inverse old append",
    );
    expect(stale.code).toBe("CONTROL_EPOCH_STALE");
    expect(stale.currentEpoch).toBe(replaced.controlEpoch);

    // Marker append under the replacement epoch captures H.
    const markerResponse = await fetchJson(newBaseUrl, `/sessions/${newSessionId}/events`, {
      authToken: newParticipantBearer,
      body: {
        controlEpoch: replaced.controlEpoch,
        eventId: inverseMarkerEventId,
        instanceId,
        payload: { case: "marker" },
        producerId: newParticipantId,
        type: raceMarkerEventType,
      },
      method: "POST",
      timeoutMs: 20_000,
    });
    expect(markerResponse.status).toBe(201);
    const markerAppended = parseRestJson(
      appendEventResponseSchema,
      markerResponse.body,
      "inverse marker append",
    );
    expect(markerAppended.status).toBe("created");
    expect(markerAppended.event.eventId).toBe(inverseMarkerEventId);
    const markerSeq = markerAppended.event.seq;

    // Paginate from zero and require E absent and marker once at H.
    const scanned: ListedEvent[] = [];
    let afterSeq = 0;
    let reachedTail = false;
    let pages = 0;
    while (!reachedTail) {
      const pageResponse = await fetchJson(
        newBaseUrl,
        `/sessions/${newSessionId}/events?after=${afterSeq}&limit=1`,
        { authToken: newParticipantBearer, timeoutMs: 20_000 },
      );
      expect(pageResponse.status).toBe(200);
      const page = parseRestJson(eventListResponseSchema, pageResponse.body, "inverse event page");
      expect(page.pagination.afterSeq).toBe(afterSeq);
      scanned.push(...page.events);
      if (page.pagination.hasMore) {
        afterSeq = page.pagination.nextAfterSeq;
        expect(page.events.length).toBe(1);
      } else {
        reachedTail = true;
      }
      pages += 1;
      if (pages > 1_000) {
        throw new Error("inverse race pagination did not reach the tail");
      }
    }

    // Contiguous sequences from one with no gaps.
    expect(scanned.map((event) => event.seq)).toEqual(
      Array.from({ length: scanned.length }, (_, index) => index + 1),
    );

    const oldEvents = scanned.filter((event) => event.eventId === inverseOldEventId);
    expect(oldEvents).toHaveLength(0);

    const markerEvents = scanned.filter((event) => event.eventId === inverseMarkerEventId);
    expect(markerEvents).toHaveLength(1);
    expect(markerEvents[0]?.seq).toBe(markerSeq);
    expect(markerEvents[0]?.payload).toEqual({ case: "marker" });
  }, 30_000);
});
