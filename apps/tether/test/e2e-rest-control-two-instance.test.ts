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
import { afterAll, beforeAll, describe, expect, it } from "vitest";
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
  init: { readonly authToken?: string; readonly body?: unknown; readonly method?: string } = {},
): Promise<RawHttpResponse> {
  const response = await fetch(`${baseUrl}${path}`, {
    ...(init.body !== undefined ? { body: JSON.stringify(init.body) } : {}),
    headers: {
      "content-type": "application/json",
      ...(init.authToken ? { authorization: `Bearer ${init.authToken}` } : {}),
    },
    method: init.method ?? (init.body !== undefined ? "POST" : "GET"),
    signal: AbortSignal.timeout(5_000),
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
