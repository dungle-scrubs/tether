import type { IncomingMessage, ServerResponse } from "node:http";
import { Readable } from "node:stream";
import { URL } from "node:url";

import { Effect } from "effect";
import { describe, expect, it } from "vitest";

import type { PermanentSessionDeleteResult } from "../src/db.js";
import { HostPresenceRuntime } from "../src/host-presence.js";
import { handleSessionHttpRoute } from "../src/http-session-route-handlers.js";
import type { SubscriptionHub } from "../src/hub.js";
import type { ResourceLimits } from "../src/resource-limits.js";
import type { SessionServiceEffect } from "../src/session-service.js";
import type { SessionListItem } from "../src/types.js";

class CapturingResponse {
  statusCode: number | null = null;
  body: Record<string, unknown> | null = null;
  writeHead(statusCode: number): this {
    this.statusCode = statusCode;
    return this;
  }
  end(payload?: string): void {
    if (payload !== undefined) {
      this.body = JSON.parse(payload) as Record<string, unknown>;
    }
  }
}

function postRequest(): IncomingMessage {
  const request = Readable.from([]) as unknown as IncomingMessage;
  (request as { method?: string }).method = "POST";
  return request;
}

function sessionItem(overrides: Partial<SessionListItem> = {}): SessionListItem {
  return {
    activeTaskCount: 0,
    activity: "settled",
    archived: true,
    bindings: [],
    createdAt: "2026-07-12T00:00:00.000Z",
    eventCount: 3,
    host: "none",
    lastEventAt: "2026-07-12T00:01:00.000Z",
    participantCount: 0,
    sessionId: "sess_del",
    taskCount: 0,
    ...overrides,
  };
}

interface DeleteCall {
  readonly hasLiveHost?: (() => boolean) | undefined;
  readonly sessionId: string;
}

function fakeService(input: {
  readonly deleteCalls: DeleteCall[];
  readonly deleteResult: PermanentSessionDeleteResult;
  readonly sessions: readonly SessionListItem[];
}): SessionServiceEffect {
  return {
    deleteSession: (request: DeleteCall) => {
      input.deleteCalls.push(request);
      return Effect.succeed(input.deleteResult);
    },
    listSessions: () => Effect.succeed([...input.sessions]),
  } as unknown as SessionServiceEffect;
}

function runDelete(input: {
  readonly hostPresence: HostPresenceRuntime;
  readonly response: CapturingResponse;
  readonly service: SessionServiceEffect;
}): Promise<boolean> {
  return Effect.runPromise(
    handleSessionHttpRoute({
      authContext: null,
      hostPresence: input.hostPresence,
      hub: { broadcast: () => {} } as unknown as SubscriptionHub,
      replicaId: "replica_test",
      request: postRequest(),
      resourceLimits: { httpMaxBodyBytes: 1_000_000 } as ResourceLimits,
      response: input.response as unknown as ServerResponse,
      runtimeTopology: "single",
      service: input.service,
      url: new URL("http://localhost/sessions/sess_del/delete"),
    }),
  );
}

describe("permanent session delete route fencing", () => {
  it("passes a live Host Presence probe into the fenced service delete", async () => {
    const deleteCalls: DeleteCall[] = [];
    const hostPresence = new HostPresenceRuntime();
    const response = new CapturingResponse();
    const service = fakeService({
      deleteCalls,
      deleteResult: { status: "deleted" },
      sessions: [sessionItem()],
    });

    const handled = await runDelete({ hostPresence, response, service });

    expect(handled).toBe(true);
    expect(response.statusCode).toBe(200);
    expect(response.body).toEqual({ ok: true, sessionId: "sess_del" });
    expect(deleteCalls).toHaveLength(1);
    const probe = deleteCalls[0]?.hasLiveHost;
    expect(probe).toBeTypeOf("function");
    // The probe is live, not a snapshot: a host connecting after the route's
    // advisory pre-check is observed by the in-transaction re-check.
    expect(probe?.()).toBe(false);
    hostPresence.upsertHost("sess_del", {
      displayName: "late host",
      instanceId: "inst_late",
      participantId: "part_host",
    });
    expect(probe?.()).toBe(true);
  });

  it("maps an in-transaction refusal to the typed 409 response", async () => {
    const deleteCalls: DeleteCall[] = [];
    const response = new CapturingResponse();
    const service = fakeService({
      deleteCalls,
      deleteResult: {
        detail: "a turn is active on this session",
        reason: "protected",
        status: "refused",
      },
      sessions: [sessionItem()],
    });

    await runDelete({ hostPresence: new HostPresenceRuntime(), response, service });

    expect(response.statusCode).toBe(409);
    expect(response.body).toEqual({
      detail: "a turn is active on this session",
      ok: false,
      reason: "protected",
    });
  });

  it("maps an in-transaction not_found to 404", async () => {
    const response = new CapturingResponse();
    const service = fakeService({
      deleteCalls: [],
      deleteResult: { status: "not_found" },
      sessions: [sessionItem()],
    });

    await runDelete({ hostPresence: new HostPresenceRuntime(), response, service });

    expect(response.statusCode).toBe(404);
    expect(response.body).toEqual({
      detail: "session not found",
      ok: false,
      reason: "not-found",
    });
  });

  it("still refuses at the advisory pre-check without calling the service delete", async () => {
    const deleteCalls: DeleteCall[] = [];
    const response = new CapturingResponse();
    const service = fakeService({
      deleteCalls,
      deleteResult: { status: "deleted" },
      sessions: [sessionItem({ archived: false })],
    });

    await runDelete({ hostPresence: new HostPresenceRuntime(), response, service });

    expect(response.statusCode).toBe(409);
    expect(response.body).toMatchObject({ ok: false, reason: "not-archived" });
    expect(deleteCalls).toHaveLength(0);
  });
});
