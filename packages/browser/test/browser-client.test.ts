import type {
  BrowserSessionSnapshot,
  OperatorGrantScope,
  SessionEvent,
  TaskRecord,
} from "@dungle-scrubs/tether-protocol";
import { browserCsrfHeaderName } from "@dungle-scrubs/tether-protocol";
import { afterEach, describe, expect, it, vi } from "vitest";

import { BrowserOperatorClient, BrowserOperatorHttpError } from "../src/index.js";

const serviceUrl = "https://hub.example.test";
const csrfToken = "c".repeat(43);
const scope: OperatorGrantScope = {
  actions: ["archive"],
  commands: ["scan"],
  permissions: [
    "approval.submit",
    "browser-session.read",
    "browser-session.revoke",
    "scan.request",
    "session.read",
    "websocket.connect",
  ],
  scopeKeys: ["scope_primary"],
  sessionIds: ["sess_email"],
  targetKinds: ["handling"],
};

afterEach(() => {
  FakeWebSocket.instances.length = 0;
  vi.unstubAllGlobals();
});

describe("BrowserOperatorClient HTTP boundary", () => {
  it("bootstraps the cookie session and loads a validated snapshot with browser fetch", async () => {
    const calls: Array<{ readonly init: RequestInit | undefined; readonly url: string }> = [];
    vi.stubGlobal(
      "fetch",
      vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
        const url = String(input);
        calls.push({ init, url });
        if (url.endsWith("/operator/browser-session")) {
          return jsonResponse({
            expiresAt: "2026-08-02T00:00:00.000Z",
            grantJti: "grant_browser",
            scope,
            sessionIds: ["sess_email"],
            status: "active",
            subject: "operator@example.test",
          });
        }
        return jsonResponse(snapshotFixture());
      }),
    );
    const client = new BrowserOperatorClient({ csrfToken, serviceUrl });

    const session = await client.bootstrap();
    const snapshot = await client.loadSnapshot("sess_email");

    expect(session.scope).toEqual(scope);
    expect(snapshot).toEqual(snapshotFixture());
    expect(calls.map((call) => call.url)).toEqual([
      `${serviceUrl}/operator/browser-session`,
      `${serviceUrl}/operator/sessions/sess_email/snapshot`,
    ]);
    expect(calls.every((call) => call.init?.credentials === "include")).toBe(true);
    expect(client.debugInfo()).toMatchObject({ bootstrapCount: 1, snapshotCount: 1 });
  });

  it("submits scoped commands and canonical approvals with CSRF protection", async () => {
    const requests: Array<{
      readonly body: unknown;
      readonly headers: Headers;
      readonly url: string;
    }> = [];
    vi.stubGlobal(
      "fetch",
      vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
        const headers = new Headers(init?.headers);
        requests.push({
          body: init?.body === undefined ? undefined : JSON.parse(String(init.body)),
          headers,
          url: String(input),
        });
        if (String(input).endsWith("/commands")) {
          return jsonResponse({ status: "created", task: taskFixture() }, 201);
        }
        return jsonResponse({
          rejectionReason: "target_not_in_manifest",
          status: "rejected",
          task: null,
        });
      }),
    );
    const client = new BrowserOperatorClient({ csrfToken, serviceUrl });

    const command = await client.requestCommand("sess_email", {
      command: "scan",
      scopeKey: "scope_primary",
    });
    const approval = await client.submitApproval("sess_email", "task_review", {
      decision: "approved",
      reason: { source: "workbench" },
      target: {
        action: "archive",
        digest: "digest_1",
        scopeKey: "scope_primary",
        targetId: "message_1",
        targetKind: "handling",
        targetRevision: "revision_1",
      },
    });

    expect(command.status).toBe("created");
    expect(approval).toEqual({
      rejectionReason: "target_not_in_manifest",
      status: "rejected",
      task: null,
    });
    expect(requests).toHaveLength(2);
    expect(
      requests.every((request) => request.headers.get(browserCsrfHeaderName) === csrfToken),
    ).toBe(true);
    expect(requests.map((request) => request.body)).toEqual([
      { command: "scan", scopeKey: "scope_primary" },
      {
        decision: "approved",
        reason: { source: "workbench" },
        target: {
          action: "archive",
          digest: "digest_1",
          scopeKey: "scope_primary",
          targetId: "message_1",
          targetKind: "handling",
          targetRevision: "revision_1",
        },
      },
    ]);
  });

  it("returns the canonical ignored approval for duplicate decisions", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn(async () =>
        jsonResponse({
          approval: approvalFixture(),
          decision: "rejected",
          existingDecision: "approved",
          ignoredReason: "already_approved",
          status: "ignored",
          task: taskFixture(),
        }),
      ),
    );
    const client = new BrowserOperatorClient({ csrfToken, serviceUrl });

    const result = await client.submitApproval("sess_email", "task_review", {
      decision: "rejected",
      reason: { source: "workbench" },
      target: {
        action: "archive",
        digest: "digest_1",
        scopeKey: "scope_primary",
        targetId: "message_1",
        targetKind: "handling",
        targetRevision: "revision_1",
      },
    });

    expect(result).toMatchObject({
      decision: "rejected",
      existingDecision: "approved",
      ignoredReason: "already_approved",
      status: "ignored",
    });
  });

  it("fails closed on non-success responses and invalid protocol bodies", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn(async () => jsonResponse({ reason: "operator_scope_denied" }, 403)),
    );
    const denied = new BrowserOperatorClient({ csrfToken, serviceUrl });
    await expect(denied.bootstrap()).rejects.toMatchObject({
      reason: "operator_scope_denied",
      status: 403,
    });

    vi.stubGlobal(
      "fetch",
      vi.fn(async () => jsonResponse({ status: "active" })),
    );
    const malformed = new BrowserOperatorClient({ csrfToken, serviceUrl });
    await expect(malformed.bootstrap()).rejects.toBeInstanceOf(BrowserOperatorHttpError);
  });
});

describe("BrowserOperatorClient event stream", () => {
  it("mints a one-time ticket and awaits replay handlers before advancing the cursor", async () => {
    const handlerBarrier = deferred<void>();
    const handled: number[] = [];
    const ticketRequests: string[] = [];
    installTicketFetch(ticketRequests, ["t".repeat(43)]);
    vi.stubGlobal("WebSocket", FakeWebSocket);
    const client = new BrowserOperatorClient({ csrfToken, serviceUrl });

    const stream = await client.connectSession({
      afterSeq: 7,
      onEvent: async (event) => {
        await handlerBarrier.promise;
        handled.push(event.seq);
      },
      sessionId: "sess_email",
    });
    const socket = requiredSocket(0);
    socket.emitEnvelope(eventEnvelope(8));
    socket.emitEnvelope({ op: "replay.complete" });
    await flushPromises();

    expect(handled).toEqual([]);
    expect(stream.debugInfo()).toMatchObject({ lastHandledSeq: 7, replayComplete: false });

    handlerBarrier.resolve();
    await stream.waitForReplayComplete();

    expect(handled).toEqual([8]);
    expect(stream.debugInfo()).toMatchObject({ lastHandledSeq: 8, replayComplete: true });
    expect(ticketRequests).toEqual([`${serviceUrl}/operator/websocket-ticket`]);
    expect(socket.url).toContain("/sessions/sess_email/stream");
    expect(socket.url).toContain("after=7");
    expect(socket.url).toContain(`ticket=${"t".repeat(43)}`);
    expect(socket.url).toContain("runtimeKind=observer");
  });

  it("reconnects with a fresh ticket and resumes after the last handled event", async () => {
    installTicketFetch([], ["a".repeat(43), "b".repeat(43)]);
    vi.stubGlobal("WebSocket", FakeWebSocket);
    const client = new BrowserOperatorClient({
      csrfToken,
      reconnect: { baseDelayMs: 0, maxAttempts: 2, maxDelayMs: 0 },
      serviceUrl,
    });
    const events: number[] = [];
    const stream = await client.connectSession({
      afterSeq: 3,
      onEvent: (event) => {
        events.push(event.seq);
      },
      sessionId: "sess_email",
    });
    const first = requiredSocket(0);
    first.emitEnvelope(eventEnvelope(4));
    first.emitEnvelope({ op: "replay.complete" });
    await stream.waitForReplayComplete();

    first.disconnect();
    await waitFor(() => FakeWebSocket.instances.length === 2);
    const second = requiredSocket(1);
    expect(second.url).toContain("after=4");
    expect(second.url).toContain(`ticket=${"b".repeat(43)}`);

    second.emitEnvelope(eventEnvelope(5));
    second.emitEnvelope({ op: "replay.complete" });
    await waitFor(() => stream.debugInfo().lastHandledSeq === 5);

    expect(events).toEqual([4, 5]);
    expect(stream.debugInfo()).toMatchObject({ reconnectCount: 1, ticketCount: 2 });
    stream.close();
  });

  it("settles active delivery before reconnect and fences the prior replay marker", async () => {
    const handlerBarrier = deferred<void>();
    installTicketFetch([], ["a".repeat(43), "b".repeat(43)]);
    vi.stubGlobal("WebSocket", FakeWebSocket);
    const client = new BrowserOperatorClient({
      csrfToken,
      reconnect: { baseDelayMs: 0, maxAttempts: 2, maxDelayMs: 0 },
      serviceUrl,
    });
    const stream = await client.connectSession({
      afterSeq: 3,
      onEvent: async () => handlerBarrier.promise,
      sessionId: "sess_email",
    });
    const first = requiredSocket(0);
    first.emitEnvelope(eventEnvelope(4));
    first.emitEnvelope({ op: "replay.complete" });
    await flushPromises();

    first.disconnect();
    await new Promise((resolve) => window.setTimeout(resolve, 0));
    await flushPromises();
    expect(FakeWebSocket.instances).toHaveLength(1);

    handlerBarrier.resolve();
    await waitFor(() => FakeWebSocket.instances.length === 2);
    const second = requiredSocket(1);
    expect(second.url).toContain("after=4");
    expect(stream.debugInfo()).toMatchObject({ replayComplete: false, state: "replaying" });

    second.emitEnvelope({ op: "replay.complete" });
    await waitFor(() => stream.debugInfo().replayComplete);
    stream.close();
  });

  it("pauses at the configured delivery bound even when diagnostics throw", async () => {
    const handlerBarrier = deferred<void>();
    installTicketFetch([], ["a".repeat(43)]);
    vi.stubGlobal("WebSocket", FakeWebSocket);
    const client = new BrowserOperatorClient({
      csrfToken,
      delivery: { handlerTimeoutMs: 10_000, maxQueueBytes: 1_000_000, maxQueueSize: 1 },
      serviceUrl,
    });
    const stream = await client.connectSession({
      afterSeq: 3,
      onError: () => {
        throw new Error("diagnostic callback failure");
      },
      onEvent: async () => handlerBarrier.promise,
      onStateChange: () => {
        throw new Error("diagnostic callback failure");
      },
      sessionId: "sess_email",
    });
    const socket = requiredSocket(0);

    socket.emitEnvelope(eventEnvelope(4));
    socket.emitEnvelope(eventEnvelope(5));
    await waitFor(() => stream.debugInfo().state === "paused");

    expect(stream.debugInfo()).toMatchObject({
      lastErrorReason: "delivery_queue_overflow",
      lastHandledSeq: 3,
      state: "paused",
    });
    expect(socket.readyState).toBe(FakeWebSocket.CLOSED);
    handlerBarrier.resolve();
  });

  it("passes handler timeout cancellation through the browser projection seam", async () => {
    installTicketFetch([], ["a".repeat(43)]);
    vi.stubGlobal("WebSocket", FakeWebSocket);
    const client = new BrowserOperatorClient({
      csrfToken,
      delivery: { handlerTimeoutMs: 1, maxQueueBytes: 1_000_000, maxQueueSize: 10 },
      serviceUrl,
    });
    let observedSignal: AbortSignal | null = null;
    const stream = await client.connectSession({
      afterSeq: 3,
      onEvent: async (_event, signal) => {
        observedSignal = signal;
        await new Promise<void>((resolve) => signal.addEventListener("abort", () => resolve()));
      },
      sessionId: "sess_email",
    });

    requiredSocket(0).emitEnvelope(eventEnvelope(4));
    await waitFor(() => stream.debugInfo().state === "paused");

    expect((observedSignal as AbortSignal | null)?.aborted).toBe(true);
    expect(stream.debugInfo()).toMatchObject({
      lastErrorReason: "event_handler_timeout",
      lastHandledSeq: 3,
    });
  });

  it("exhausts reconnect attempts across sockets that close before replay completes", async () => {
    const ticketRequests: string[] = [];
    installTicketFetch(ticketRequests, ["a".repeat(43), "b".repeat(43), "c".repeat(43)]);
    vi.stubGlobal("WebSocket", FakeWebSocket);
    const client = new BrowserOperatorClient({
      csrfToken,
      reconnect: { baseDelayMs: 0, maxAttempts: 1, maxDelayMs: 0 },
      serviceUrl,
    });
    const stream = await client.connectSession({
      afterSeq: 3,
      onEvent: () => undefined,
      sessionId: "sess_email",
    });

    requiredSocket(0).disconnect();
    await waitFor(() => FakeWebSocket.instances.length === 2);
    requiredSocket(1).disconnect();
    await waitFor(() => stream.debugInfo().state === "paused");

    expect(stream.debugInfo()).toMatchObject({
      lastErrorReason: "reconnect_exhausted",
      reconnectCount: 1,
      ticketCount: 2,
    });
    expect(ticketRequests).toHaveLength(2);
  });
});

/** Minimal validated browser snapshot. */
function snapshotFixture(): BrowserSessionSnapshot {
  return {
    cursor: 0,
    events: [],
    participants: [],
    sessionId: "sess_email",
    tasks: [],
    truncated: { events: false, participants: false, tasks: false },
  };
}

/** Minimal task record accepted by the public protocol. */
function taskFixture(): TaskRecord {
  return {
    cancelledAt: null,
    claimExpiredAt: null,
    claimExpiredBy: null,
    claimExpiresAt: null,
    claimId: null,
    claimedAt: null,
    claimedBy: null,
    completedAt: null,
    createdAt: "2026-08-01T00:00:00.000Z",
    failedAt: null,
    failure: null,
    input: { command: "scan", scopeKey: "scope_primary" },
    kind: "operator.scan",
    objective: "Run one provider-neutral scan",
    releasedAt: null,
    releasedBy: null,
    result: null,
    sessionId: "sess_email",
    taskId: "task_scan",
  };
}

/** Minimal canonical approval record accepted by duplicate-decision responses. */
function approvalFixture() {
  return {
    approvalEventId: "evt_approval",
    decidedAt: "2026-08-01T00:00:00.000Z",
    decidedByParticipantId: "operator@example.test",
    decision: "approved" as const,
    reason: { source: "workbench" },
    sessionId: "sess_email",
    targetKey: "handling:scope_primary:message_1:revision_1:digest_1:archive",
    taskId: "task_review",
  };
}

/** Builds one replay event envelope. */
function eventEnvelope(seq: number): { readonly event: SessionEvent; readonly op: "event" } {
  return {
    event: {
      createdAt: "2026-08-01T00:00:00.000Z",
      eventId: `evt_${seq}`,
      payload: {},
      producerId: "tether",
      seq,
      sessionId: "sess_email",
      type: "agent.output",
    },
    op: "event",
  };
}

/** Installs a deterministic ticket endpoint mock. */
function installTicketFetch(requests: string[], tickets: string[]): void {
  vi.stubGlobal(
    "fetch",
    vi.fn(async (input: RequestInfo | URL) => {
      requests.push(String(input));
      const ticket = tickets.shift();
      if (ticket === undefined) {
        return jsonResponse({ reason: "ticket_exhausted" }, 503);
      }
      return jsonResponse(
        {
          expiresAt: "2026-08-01T00:01:00.000Z",
          ticket,
        },
        201,
      );
    }),
  );
}

/** Creates a JSON response for browser fetch mocks. */
function jsonResponse(body: unknown, status = 200): Response {
  return new Response(JSON.stringify(body), {
    headers: { "content-type": "application/json" },
    status,
  });
}

/** Returns one created fake socket or fails the test with a clear reason. */
function requiredSocket(index: number): FakeWebSocket {
  const socket = FakeWebSocket.instances[index];
  if (socket === undefined) {
    throw new Error(`Missing fake WebSocket at index ${index}`);
  }
  return socket;
}

/** Minimal native-WebSocket test double running inside real Chromium. */
class FakeWebSocket extends EventTarget {
  static readonly CLOSED = 3;
  static readonly CLOSING = 2;
  static readonly CONNECTING = 0;
  static readonly OPEN = 1;
  static readonly instances: FakeWebSocket[] = [];

  readonly url: string;
  readyState = FakeWebSocket.CONNECTING;

  constructor(url: string | URL) {
    super();
    this.url = String(url);
    FakeWebSocket.instances.push(this);
    queueMicrotask(() => {
      this.readyState = FakeWebSocket.OPEN;
      this.dispatchEvent(new Event("open"));
    });
  }

  close(): void {
    if (this.readyState === FakeWebSocket.CLOSED) {
      return;
    }
    this.readyState = FakeWebSocket.CLOSED;
    this.dispatchEvent(new Event("close"));
  }

  disconnect(): void {
    this.readyState = FakeWebSocket.CLOSED;
    this.dispatchEvent(new Event("close"));
  }

  emitEnvelope(value: unknown): void {
    this.dispatchEvent(new MessageEvent("message", { data: JSON.stringify(value) }));
  }

  send(): void {}
}

/** Small deferred promise for delivery-order tests. */
function deferred<T>(): {
  readonly promise: Promise<T>;
  readonly resolve: (value: T | PromiseLike<T>) => void;
} {
  let resolvePromise: ((value: T | PromiseLike<T>) => void) | undefined;
  const promise = new Promise<T>((resolve) => {
    resolvePromise = resolve;
  });
  if (resolvePromise === undefined) {
    throw new Error("Deferred promise initialization failed");
  }
  return { promise, resolve: resolvePromise };
}

/** Lets queued browser microtasks run. */
async function flushPromises(): Promise<void> {
  await Promise.resolve();
  await Promise.resolve();
  await Promise.resolve();
}

/** Waits for one deterministic browser-side condition. */
async function waitFor(condition: () => boolean): Promise<void> {
  for (let attempt = 0; attempt < 100; attempt += 1) {
    if (condition()) {
      return;
    }
    await new Promise((resolve) => window.setTimeout(resolve, 0));
  }
  throw new Error("Condition did not become true");
}
