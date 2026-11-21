import { describe, expect, it, vi } from "vitest";

import type { StructuredLogEntry } from "../src/observability.js";
import {
  RestParticipantControlClient,
  type RestParticipantControlClientOptions,
  RestParticipantControlError,
  type RestParticipantControlFetch,
  type RestParticipantControlTimerHandle,
  type RestParticipantControlTimerScheduler,
} from "../src/rest-participant-control-client.js";

describe("RestParticipantControlClient", () => {
  it("exposes rejected contracts from acquisition and clears them after a valid renewal", async () => {
    const rejectedContracts = [
      {
        index: 0,
        taskKind: "synthetic.task",
        issues: [{ path: ["participantRuntimeKind"], code: "invalid_type" }],
      },
    ];
    const timers = new FakeTimerScheduler();
    const client = new RestParticipantControlClient(
      {
        instanceId: "inst_1",
        participantId: "part_1",
        runtimeKind: "generic_agent",
        serviceUrl: "http://tether.test",
      },
      {
        acquisitionIdFactory: () => "acq_1",
        now: () => Date.parse("2026-07-16T12:00:00.000Z"),
        timers,
        fetch: async (url) => {
          if (url.pathname.endsWith("heartbeat")) {
            return new Response(
              JSON.stringify({
                controlEpoch: 1,
                leaseExpiresAt: "2026-07-16T12:02:00.000Z",
                participant: {},
                renewAfterMs: 30_000,
                rejectedContracts: [],
                rejectedContractsTruncated: 0,
              }),
            );
          }
          const body = await acquisitionResponse("acq_1", 1).json();
          return new Response(
            JSON.stringify({ ...body, rejectedContracts, rejectedContractsTruncated: 7 }),
          );
        },
      },
    );
    expect((await client.context("sess_1")).rejectedContracts).toEqual(rejectedContracts);
    expect((await client.context("sess_1")).rejectedContractsTruncated).toBe(7);
    timers.fireNext();
    await vi.waitFor(async () => {
      expect((await client.context("sess_1")).rejectedContracts).toEqual([]);
      expect((await client.context("sess_1")).rejectedContractsTruncated).toBe(0);
    });
    await client.stop();
  });

  it("preserves unreported diagnostics from older acquisition and renewal responses", async () => {
    const requests: CapturedRequest[] = [];
    const timers = new FakeTimerScheduler();
    const client = new RestParticipantControlClient(
      {
        instanceId: "inst_1",
        participantId: "part_1",
        runtimeKind: "generic_agent",
        serviceUrl: "http://tether.test",
      },
      {
        acquisitionIdFactory: () => "acq_1",
        now: () => Date.parse("2026-07-16T12:00:00.000Z"),
        timers,
        fetch: createControlFetch(requests),
      },
    );
    try {
      expect((await client.context("sess_legacy")).rejectedContracts).toBeUndefined();
      timers.fireNext();
      await vi.waitFor(async () => {
        expect(requests).toHaveLength(2);
        expect((await client.context("sess_legacy")).leaseExpiresAt).toBe(
          "2026-07-16T12:02:00.000Z",
        );
      });
      expect((await client.context("sess_legacy")).rejectedContracts).toBeUndefined();
    } finally {
      await client.stop();
    }
  });

  it("joins concurrent context calls into one acquisition", async () => {
    const requests: CapturedRequest[] = [];
    const fetch = createControlFetch(requests);
    const client = createClient(fetch);

    const [left, right] = await Promise.all([client.context("sess_1"), client.context("sess_1")]);

    expect(left).toEqual(right);
    expect(requests).toHaveLength(1);
    expect(requests[0]?.body).toMatchObject({
      acquisitionId: "acq_1",
      instanceId: "inst_1",
      participantId: "part_1",
    });
    expect(client.debugInfo()).toMatchObject({
      activeContextCount: 1,
      acquiringSessionCount: 0,
      renewalTimerCount: 1,
    });
  });

  it("reuses one Acquisition ID across a transport retry", async () => {
    const requests: CapturedRequest[] = [];
    let attempt = 0;
    const fetch: RestParticipantControlFetch = async (url, init) => {
      requests.push(capture(url, init));
      attempt += 1;
      if (attempt === 1) {
        throw new Error("connection reset");
      }
      return acquisitionResponse("acq_1", 1);
    };
    const client = createClient(fetch);

    await expect(client.context("sess_1")).resolves.toMatchObject({
      acquisitionId: "acq_1",
      controlEpoch: 1,
    });
    expect(requests.map((request) => request.body)).toEqual([
      expect.objectContaining({ acquisitionId: "acq_1" }),
      expect.objectContaining({ acquisitionId: "acq_1" }),
    ]);
  });

  it("retains one Acquisition ID across separate calls after an uncertain failure", async () => {
    const requests: CapturedRequest[] = [];
    let nextId = 0;
    let attempt = 0;
    const client = new RestParticipantControlClient(
      {
        instanceId: "inst_1",
        participantId: "part_1",
        runtimeKind: "generic_agent",
        serviceUrl: "http://tether.test",
      },
      {
        acquisitionIdFactory: () => `acq_${++nextId}`,
        acquisitionTransportRetries: 0,
        fetch: async (url, init) => {
          requests.push(capture(url, init));
          attempt += 1;
          if (attempt === 1) {
            throw new Error("response lost");
          }
          return acquisitionResponse("acq_1", 1);
        },
      },
    );

    await expect(client.context("sess_1")).rejects.toMatchObject({
      code: "TRANSPORT",
    });
    await expect(client.context("sess_1")).resolves.toMatchObject({
      acquisitionId: "acq_1",
    });
    expect(requests.map((request) => request.body.acquisitionId)).toEqual(["acq_1", "acq_1"]);
  });

  it("retries acquisition body transport loss with the same Acquisition ID", async () => {
    const requests: CapturedRequest[] = [];
    let attempt = 0;
    const client = createClient(async (url, init) => {
      requests.push(capture(url, init));
      attempt += 1;
      if (attempt === 1) {
        return new Response(
          new ReadableStream({
            start(controller) {
              controller.error(new Error("body reset"));
            },
          }),
          { status: 201 },
        );
      }
      return acquisitionResponse("acq_1", 1);
    });

    await expect(client.context("sess_1")).resolves.toMatchObject({
      acquisitionId: "acq_1",
    });
    expect(requests.map((request) => request.body.acquisitionId)).toEqual(["acq_1", "acq_1"]);
  });

  it("rejects a valid acquisition response that echoes another Acquisition ID", async () => {
    const client = createClient(async () => acquisitionResponse("acq_other", 1));

    await expect(client.context("sess_1")).rejects.toMatchObject({
      code: "INVALID_RESPONSE",
    });
    expect(client.debugInfo().activeContextCount).toBe(0);
  });

  it("isolates contexts for different sessions", async () => {
    const requests: CapturedRequest[] = [];
    let nextId = 0;
    const client = new RestParticipantControlClient(
      {
        instanceId: "inst_1",
        participantId: "part_1",
        runtimeKind: "generic_agent",
        serviceUrl: "http://tether.test",
      },
      {
        acquisitionIdFactory: () => {
          nextId += 1;
          return `acq_${nextId}`;
        },
        fetch: createControlFetch(requests),
      },
    );

    const [left, right] = await Promise.all([client.context("sess_1"), client.context("sess_2")]);

    expect(left.acquisitionId).toBe("acq_1");
    expect(right.acquisitionId).toBe("acq_2");
    expect(requests.map((request) => request.path)).toEqual([
      "/sessions/sess_1/participants",
      "/sessions/sess_2/participants",
    ]);
  });

  it("releases exact active contexts once during repeated shutdown", async () => {
    const requests: CapturedRequest[] = [];
    const client = createClient(createControlFetch(requests));
    await client.context("sess_1");

    await Promise.all([client.stop(), client.stop()]);

    expect(requests).toHaveLength(2);
    expect(requests[1]).toEqual({
      body: {
        controlEpoch: 1,
        instanceId: "inst_1",
      },
      path: "/sessions/sess_1/participants/part_1/control/release",
    });
    expect(client.debugInfo()).toMatchObject({
      activeContextCount: 0,
      renewalTimerCount: 0,
      stopped: true,
    });
  });

  it("waits for an in-flight acquisition and releases it during shutdown", async () => {
    const requests: CapturedRequest[] = [];
    let resolveAcquisition: ((response: Response) => void) | null = null;
    const acquisition = new Promise<Response>((resolve) => {
      resolveAcquisition = resolve;
    });
    const client = createClient(async (url, init) => {
      const request = capture(url, init);
      requests.push(request);
      if (request.path.endsWith("/control/release")) {
        return new Response(JSON.stringify({ released: true }), {
          status: 200,
        });
      }
      return acquisition;
    });

    const context = client.context("sess_1");
    const stopped = client.stop();
    resolveAcquisition?.(acquisitionResponse("acq_1", 1));
    await context;
    await stopped;

    expect(requests.map((request) => request.path)).toEqual([
      "/sessions/sess_1/participants",
      "/sessions/sess_1/participants/part_1/control/release",
    ]);
  });

  it("bounds shutdown when an acquisition transport never settles", async () => {
    const client = new RestParticipantControlClient(
      {
        instanceId: "inst_1",
        participantId: "part_1",
        runtimeKind: "generic_agent",
        serviceUrl: "http://tether.test",
      },
      {
        acquisitionIdFactory: () => "acq_1",
        fetch: async () => new Promise<Response>(() => undefined),
        shutdownPendingWaitMs: 1,
      },
    );

    void client.context("sess_1").catch(() => undefined);
    await expect(client.stop()).resolves.toBeUndefined();

    expect(client.debugInfo()).toMatchObject({
      acquiringSessionCount: 0,
      stopped: true,
    });
  });

  it("uses a new Acquisition ID after a successful release", async () => {
    const requests: CapturedRequest[] = [];
    let nextId = 0;
    const client = new RestParticipantControlClient(
      {
        instanceId: "inst_1",
        participantId: "part_1",
        runtimeKind: "generic_agent",
        serviceUrl: "http://tether.test",
      },
      {
        acquisitionIdFactory: () => `acq_${++nextId}`,
        fetch: createControlFetch(requests),
      },
    );

    await client.context("sess_1");
    await expect(client.release("sess_1")).resolves.toBe(true);
    await expect(client.context("sess_1")).resolves.toMatchObject({
      acquisitionId: "acq_2",
    });

    expect(
      requests
        .filter((request) => request.path.endsWith("/participants"))
        .map((request) => request.body.acquisitionId),
    ).toEqual(["acq_1", "acq_2"]);
  });

  it("renews from server guidance without changing the installed generation", async () => {
    const requests: CapturedRequest[] = [];
    const timers = new FakeTimerScheduler();
    const client = new RestParticipantControlClient(
      {
        instanceId: "inst_1",
        participantId: "part_1",
        runtimeKind: "generic_agent",
        serviceUrl: "http://tether.test",
      },
      {
        acquisitionIdFactory: () => "acq_1",
        fetch: createControlFetch(requests),
        now: () => Date.parse("2026-07-16T12:00:00.000Z"),
        timers,
      },
    );
    const acquired = await client.context("sess_1");

    timers.fireNext();
    await vi.waitFor(() => {
      expect(requests).toHaveLength(2);
      // Acquisition deadline, renewal timer, renewal deadline, next renewal timer.
      expect(timers.unrefCount).toBe(4);
    });

    expect(requests[1]).toEqual({
      body: { controlEpoch: 1, instanceId: "inst_1" },
      path: "/sessions/sess_1/participants/part_1/heartbeat",
    });
    const current = await client.context("sess_1");
    expect(current).toMatchObject({
      acquisitionId: acquired.acquisitionId,
      controlEpoch: acquired.controlEpoch,
      leaseExpiresAt: "2026-07-16T12:02:00.000Z",
    });
  });

  it("rejects a renewal response that echoes another Control Epoch", async () => {
    const requests: CapturedRequest[] = [];
    const timers = new FakeTimerScheduler();
    const client = new RestParticipantControlClient(
      {
        instanceId: "inst_1",
        participantId: "part_1",
        runtimeKind: "generic_agent",
        serviceUrl: "http://tether.test",
      },
      {
        acquisitionIdFactory: () => "acq_1",
        fetch: async (url, init) => {
          const request = capture(url, init);
          requests.push(request);
          if (request.path.endsWith("/heartbeat")) {
            return new Response(
              JSON.stringify({
                controlEpoch: 2,
                leaseExpiresAt: "2026-07-16T12:02:00.000Z",
                participant: { participantId: "part_1" },
                renewAfterMs: 30_000,
              }),
              {
                headers: { "content-type": "application/json" },
                status: 200,
              },
            );
          }
          return acquisitionResponse("acq_1", 1);
        },
        now: () => Date.parse("2026-07-16T12:00:00.000Z"),
        timers,
      },
    );
    await client.context("sess_1");

    timers.fireNext();
    await vi.waitFor(() => expect(client.debugInfo().lastFailureCode).toBe("INVALID_RESPONSE"));

    expect(requests).toHaveLength(2);
  });

  it("clears a failed renewal timer and stops returning an expired context", async () => {
    const requests: CapturedRequest[] = [];
    const timers = new FakeTimerScheduler();
    let now = Date.parse("2026-07-16T12:00:00.000Z");
    const client = new RestParticipantControlClient(
      {
        instanceId: "inst_1",
        participantId: "part_1",
        runtimeKind: "generic_agent",
        serviceUrl: "http://tether.test",
      },
      {
        acquisitionIdFactory: () => `acq_${requests.length + 1}`,
        fetch: async (url, init) => {
          const request = capture(url, init);
          requests.push(request);
          if (request.path.endsWith("/heartbeat")) {
            throw new Error("renewal transport failed");
          }
          return acquisitionResponse(String(request.body.acquisitionId), requests.length);
        },
        now: () => now,
        timers,
      },
    );
    const original = await client.context("sess_1");

    timers.fireNext();
    await vi.waitFor(() => expect(client.debugInfo().renewalTimerCount).toBe(1));
    now = Date.parse(original.leaseExpiresAt) + 1;
    const replacement = await client.context("sess_1");

    expect(replacement.acquisitionId).not.toBe(original.acquisitionId);
  });

  it("invalidates only a matching generation and traces that public boundary", async () => {
    const requests: CapturedRequest[] = [];
    const client = createClient(createControlFetch(requests));
    const oldContext = await client.context("sess_1");
    expect(client.invalidate(oldContext)).toBe(true);
    const replacement = await client.context("sess_1");

    expect(replacement.controlEpoch).toBeGreaterThan(oldContext.controlEpoch);
    expect(client.invalidate(oldContext)).toBe(false);
    expect(client.debugInfo()).toMatchObject({
      activeContextCount: 1,
      boundary: {
        boundaryCalls: 4,
        lastOperation: "invalidate",
      },
    });
  });

  it("invalidates the current context with a snapshot taken before a renewal", async () => {
    const requests: CapturedRequest[] = [];
    const timers = new FakeTimerScheduler();
    const client = createTimedClient(createControlFetch(requests), timers);
    const beforeRenewal = await client.context("sess_1");

    timers.fireNext();
    await vi.waitFor(async () => {
      expect((await client.context("sess_1")).leaseExpiresAt).toBe("2026-07-16T12:02:00.000Z");
    });
    expect(await client.context("sess_1")).not.toBe(beforeRenewal);

    expect(client.invalidate(beforeRenewal)).toBe(true);
    expect(client.debugInfo()).toMatchObject({ activeContextCount: 0, renewalTimerCount: 0 });
    const replacement = await client.context("sess_1");

    expect(replacement.controlEpoch).toBeGreaterThan(beforeRenewal.controlEpoch);
    expect(requests.map((request) => request.path)).toEqual([
      "/sessions/sess_1/participants",
      "/sessions/sess_1/participants/part_1/heartbeat",
      "/sessions/sess_1/participants",
    ]);
  });

  it("leaves the current context in place when the snapshot names an older generation", async () => {
    const requests: CapturedRequest[] = [];
    let nextId = 0;
    const client = createTimedClient(createControlFetch(requests), new FakeTimerScheduler(), {
      acquisitionIdFactory: () => `acq_${++nextId}`,
    });
    const first = await client.context("sess_1");
    expect(client.invalidate(first)).toBe(true);
    const second = await client.context("sess_1");

    expect(client.invalidate(first)).toBe(false);
    expect(client.invalidate({ ...second, controlEpoch: first.controlEpoch })).toBe(false);
    expect(client.invalidate({ ...second, acquisitionId: first.acquisitionId })).toBe(false);
    await expect(client.context("sess_1")).resolves.toBe(second);
    expect(requests).toHaveLength(2);
  });

  it("bounds each control request with a default ten-second deadline", async () => {
    const timers = new FakeTimerScheduler();
    const client = createTimedClient(createControlFetch([]), timers);
    let resolved = false;
    const acquiring = client.context("sess_1").then((context) => {
      resolved = true;
      return context;
    });

    expect(timers.delays).toEqual([10_000]);
    await acquiring;
    expect(resolved).toBe(true);
    expect(timers.delays).not.toContain(10_000);
  });

  it.each([
    [0, 100],
    [250, 250],
    [10_000_000, 60_000],
  ] as const)("clamps requestTimeoutMs %s to %s", async (requested, applied) => {
    const timers = new FakeTimerScheduler();
    const client = createTimedClient(async () => new Promise<Response>(() => undefined), timers, {
      requestTimeoutMs: requested,
    });

    void client.context("sess_1").catch(() => undefined);

    expect(timers.delays).toEqual([applied]);
  });

  it("rejects an acquisition whose fetch never settles at the deadline", async () => {
    const timers = new FakeTimerScheduler();
    const signals: AbortSignal[] = [];
    const client = createTimedClient(
      async (_url, init) => {
        if (init.signal) {
          signals.push(init.signal);
        }
        return new Promise<Response>(() => undefined);
      },
      timers,
      { requestTimeoutMs: 2_000 },
    );
    const acquiring = client.context("sess_1").catch((error: unknown) => error);

    await vi.waitFor(() => expect(signals).toHaveLength(1));
    expect(signals[0]?.aborted).toBe(false);
    timers.fireDelay(2_000);

    const failure = await acquiring;
    expect(failure).toBeInstanceOf(RestParticipantControlError);
    expect(failure).toMatchObject({
      code: "TRANSPORT",
      details: { operation: "acquire", status: null },
    });
    expect(signals[0]?.aborted).toBe(true);
    expect(client.debugInfo()).toMatchObject({ acquiringSessionCount: 0, activeContextCount: 0 });
  });

  it("rejects an acquisition whose response body never ends at the deadline", async () => {
    const timers = new FakeTimerScheduler();
    let fetched = 0;
    const client = createTimedClient(
      async () => {
        fetched += 1;
        return new Response(new ReadableStream({ start() {} }), { status: 201 });
      },
      timers,
      { requestTimeoutMs: 2_000 },
    );
    const acquiring = client.context("sess_1").catch((error: unknown) => error);

    await vi.waitFor(() => expect(fetched).toBe(1));
    timers.fireDelay(2_000);

    await expect(acquiring).resolves.toMatchObject({
      code: "TRANSPORT",
      details: { operation: "acquire", status: 201 },
    });
  });

  it("aborts a stalled acquisition at shutdown before its deadline", async () => {
    const timers = new FakeTimerScheduler();
    const client = createTimedClient(async () => new Promise<Response>(() => undefined), timers, {
      shutdownPendingWaitMs: 1,
    });
    const acquiring = client.context("sess_1").catch((error: unknown) => error);

    await client.stop();

    await expect(acquiring).resolves.toMatchObject({ code: "TRANSPORT" });
    expect(timers.delays).toEqual([]);
  });

  it.each([
    ["a fetch that never settles", () => new Promise<Response>(() => undefined)],
    [
      "a response body that never ends",
      async () => new Response(new ReadableStream({ start() {} }), { status: 200 }),
    ],
  ] as const)("keeps the context and retries a renewal that times out on %s", async (_label, stall) => {
    const timers = new FakeTimerScheduler();
    const failures: unknown[] = [];
    const heartbeats: string[] = [];
    const control = createControlFetch([]);
    const client = createTimedClient(
      async (url, init) => {
        if (url.pathname.endsWith("/heartbeat")) {
          heartbeats.push(url.pathname);
          return stall();
        }
        return control(url, init);
      },
      timers,
      {
        onRenewalFailure: (failure) => failures.push(failure),
        requestTimeoutMs: 2_000,
      },
    );
    const acquired = await client.context("sess_1");

    timers.fireDelay(30_000);
    await vi.waitFor(() => expect(heartbeats).toHaveLength(1));
    timers.fireDelay(2_000);

    await vi.waitFor(() => expect(failures).toHaveLength(1));
    expect(failures[0]).toEqual({ code: "TRANSPORT", controlRetained: true, sessionId: "sess_1" });
    expect(client.debugInfo()).toMatchObject({
      activeContextCount: 1,
      lastFailureCode: "TRANSPORT",
      renewalTimerCount: 1,
    });
    expect(timers.delays).toEqual([1_000]);
    await expect(client.context("sess_1")).resolves.toBe(acquired);
  });

  it.each([
    ["a fetch that never settles", () => new Promise<Response>(() => undefined)],
    [
      "a response body that never ends",
      async () => new Response(new ReadableStream({ start() {} }), { status: 200 }),
    ],
  ] as const)("reports an uncertain release that times out on %s", async (_label, stall) => {
    const timers = new FakeTimerScheduler();
    const lost: unknown[] = [];
    const control = createControlFetch([]);
    let releases = 0;
    const client = createTimedClient(
      async (url, init) => {
        if (url.pathname.endsWith("/control/release")) {
          releases += 1;
          return stall();
        }
        return control(url, init);
      },
      timers,
      { onControlLost: (loss) => lost.push(loss), requestTimeoutMs: 2_000 },
    );
    await client.context("sess_1");

    const releasing = client.release("sess_1").catch((error: unknown) => error);
    await vi.waitFor(() => expect(releases).toBe(1));
    timers.fireDelay(2_000);

    await expect(releasing).resolves.toMatchObject({
      code: "TRANSPORT",
      details: { operation: "release" },
    });
    expect(client.debugInfo()).toMatchObject({
      activeContextCount: 0,
      uncertainReleaseCount: 1,
    });
    expect(lost).toEqual([]);
  });

  it("reports a renewal transport failure that keeps the context", async () => {
    const timers = new FakeTimerScheduler();
    const failures: unknown[] = [];
    const lost: unknown[] = [];
    const control = createControlFetch([]);
    const client = createTimedClient(
      async (url, init) => {
        if (url.pathname.endsWith("/heartbeat")) {
          throw new Error("connection reset");
        }
        return control(url, init);
      },
      timers,
      {
        onControlLost: (loss) => lost.push(loss),
        onRenewalFailure: (failure) => failures.push(failure),
      },
    );
    await client.context("sess_1");

    timers.fireNext();
    await vi.waitFor(() => expect(failures).toHaveLength(1));
    timers.fireNext();
    await vi.waitFor(() => expect(failures).toHaveLength(2));

    expect(failures).toEqual([
      { code: "TRANSPORT", controlRetained: true, sessionId: "sess_1" },
      { code: "TRANSPORT", controlRetained: true, sessionId: "sess_1" },
    ]);
    expect(lost).toEqual([]);
    expect(client.debugInfo().activeContextCount).toBe(1);
  });

  it.each([
    [409, { code: "CONTROL_EPOCH_STALE" }, "CONTROL_EPOCH_STALE"],
    [409, {}, "CONTROL_CONFLICT"],
    [428, { code: "CONTROL_EPOCH_REQUIRED" }, "CONTROL_EPOCH_REQUIRED"],
  ] as const)("reports a renewal rejected with HTTP %s as lost control (%s)", async (status, body, code) => {
    const timers = new FakeTimerScheduler();
    const failures: unknown[] = [];
    const lost: unknown[] = [];
    const control = createControlFetch([]);
    const client = createTimedClient(
      async (url, init) => {
        if (url.pathname.endsWith("/heartbeat")) {
          return new Response(JSON.stringify(body), { status });
        }
        return control(url, init);
      },
      timers,
      {
        onControlLost: (loss) => lost.push(loss),
        onRenewalFailure: (failure) => failures.push(failure),
      },
    );
    await client.context("sess_1");

    timers.fireNext();
    await vi.waitFor(() => expect(failures).toHaveLength(1));

    expect(failures).toEqual([{ code, controlRetained: false, sessionId: "sess_1" }]);
    expect(lost).toEqual([{ code, sessionId: "sess_1" }]);
    expect(client.debugInfo()).toMatchObject({ activeContextCount: 0, renewalTimerCount: 0 });
  });

  it("reports a renewal failure after the lease ran out as an expiry", async () => {
    const timers = new FakeTimerScheduler();
    const failures: unknown[] = [];
    const lost: unknown[] = [];
    let now = Date.parse("2026-07-16T12:00:00.000Z");
    const control = createControlFetch([]);
    const client = createTimedClient(
      async (url, init) => {
        if (url.pathname.endsWith("/heartbeat")) {
          now = Date.parse("2026-07-16T12:01:00.000Z");
          throw new Error("connection reset");
        }
        return control(url, init);
      },
      timers,
      {
        now: () => now,
        onControlLost: (loss) => lost.push(loss),
        onRenewalFailure: (failure) => failures.push(failure),
      },
    );
    await client.context("sess_1");

    timers.fireNext();
    await vi.waitFor(() => expect(failures).toHaveLength(1));

    expect(failures).toEqual([{ code: "TRANSPORT", controlRetained: false, sessionId: "sess_1" }]);
    expect(lost).toEqual([{ code: "LEASE_EXPIRED", sessionId: "sess_1" }]);
    expect(client.debugInfo().activeContextCount).toBe(0);
  });

  it("reports an expired context found by context() and an explicit invalidation", async () => {
    const lost: unknown[] = [];
    let now = Date.parse("2026-07-16T12:00:00.000Z");
    let nextId = 0;
    const client = createTimedClient(createControlFetch([]), new FakeTimerScheduler(), {
      acquisitionIdFactory: () => `acq_${++nextId}`,
      now: () => now,
      onControlLost: (loss) => lost.push(loss),
    });
    await client.context("sess_1");
    now = Date.parse("2026-07-16T12:01:00.000Z");
    const replacement = await client.context("sess_1");
    expect(lost).toEqual([{ code: "LEASE_EXPIRED", sessionId: "sess_1" }]);

    expect(client.invalidate(replacement)).toBe(true);
    expect(client.invalidate(replacement)).toBe(false);
    expect(lost).toEqual([
      { code: "LEASE_EXPIRED", sessionId: "sess_1" },
      { code: "INVALIDATED", sessionId: "sess_1" },
    ]);
  });

  it("does not report lost control for release or stop", async () => {
    const lost: unknown[] = [];
    const failures: unknown[] = [];
    const client = createTimedClient(createControlFetch([]), new FakeTimerScheduler(), {
      onControlLost: (loss) => lost.push(loss),
      onRenewalFailure: (failure) => failures.push(failure),
    });
    await client.context("sess_1");
    await client.release("sess_1");
    await client.context("sess_2");
    await client.stop();

    expect(lost).toEqual([]);
    expect(failures).toEqual([]);
  });

  it("survives throwing callbacks and traces their failures", async () => {
    const timers = new FakeTimerScheduler();
    const control = createControlFetch([]);
    const logs: StructuredLogEntry[] = [];
    let renewals = 0;
    const client = createTimedClient(
      async (url, init) => {
        if (url.pathname.endsWith("/heartbeat")) {
          renewals += 1;
          if (renewals === 1) {
            throw new Error("connection reset");
          }
          return new Response(JSON.stringify({ code: "CONTROL_EPOCH_STALE" }), { status: 409 });
        }
        return control(url, init);
      },
      timers,
      {
        observability: {
          boundaryLogsEnabled: true,
          logger: { log: (entry) => logs.push(entry) },
          moduleName: "RestParticipantControlClient",
        },
        onControlLost: () => {
          throw new Error("lost callback failed");
        },
        onRenewalFailure: () => {
          throw new Error("renewal callback failed");
        },
      },
    );
    const callbackErrors = () =>
      logs
        .filter((entry) => entry.message === "boundary.error" && entry.operation.startsWith("on"))
        .map((entry) => [entry.operation, (entry.data.error as { message: string }).message]);
    await client.context("sess_1");

    timers.fireNext();
    await vi.waitFor(() => expect(callbackErrors()).toHaveLength(1));
    expect(callbackErrors()).toEqual([["onRenewalFailure", "renewal callback failed"]]);
    expect(client.debugInfo()).toMatchObject({ activeContextCount: 1, renewalTimerCount: 1 });

    timers.fireNext();
    await vi.waitFor(() => expect(callbackErrors()).toHaveLength(3));
    expect(callbackErrors()).toEqual([
      ["onRenewalFailure", "renewal callback failed"],
      ["onControlLost", "lost callback failed"],
      ["onRenewalFailure", "renewal callback failed"],
    ]);
    expect(client.debugInfo()).toMatchObject({ activeContextCount: 0, renewalTimerCount: 0 });

    const again = await client.context("sess_1");
    expect(client.invalidate(again)).toBe(true);
    expect(callbackErrors()).toHaveLength(4);
    await expect(client.context("sess_1")).resolves.toMatchObject({ sessionId: "sess_1" });
  });

  it("passes callbacks no token, header or response body", async () => {
    const timers = new FakeTimerScheduler();
    const received: unknown[] = [];
    const control = createControlFetch([]);
    const client = new RestParticipantControlClient(
      {
        authToken: "secret-token",
        instanceId: "inst_1",
        participantId: "part_1",
        runtimeKind: "generic_agent",
        serviceUrl: "http://tether.test",
      },
      {
        acquisitionIdFactory: () => "acq_1",
        fetch: async (url, init) => {
          if (url.pathname.endsWith("/heartbeat")) {
            return new Response(
              JSON.stringify({ code: "CONTROL_EPOCH_STALE", detail: "response-body-detail" }),
              { status: 409 },
            );
          }
          return control(url, init);
        },
        now: () => Date.parse("2026-07-16T12:00:00.000Z"),
        onControlLost: (loss) => received.push(loss),
        onRenewalFailure: (failure) => received.push(failure),
        timers,
      },
    );
    await client.context("sess_1");

    timers.fireNext();
    await vi.waitFor(() => expect(received).toHaveLength(2));

    const serialized = JSON.stringify(received);
    expect(serialized).not.toContain("secret-token");
    expect(serialized).not.toContain("Bearer");
    expect(serialized).not.toContain("response-body-detail");
    for (const value of received) {
      expect(Object.keys(value as object).sort()).toEqual(
        Object.keys(value as object).includes("controlRetained")
          ? ["code", "controlRetained", "sessionId"]
          : ["code", "sessionId"],
      );
    }
  });

  it("keeps an uncertain release bounded and never replays it", async () => {
    const requests: CapturedRequest[] = [];
    const fetch: RestParticipantControlFetch = async (url, init) => {
      const request = capture(url, init);
      requests.push(request);
      if (request.path.endsWith("/control/release")) {
        throw new Error("socket closed with sensitive implementation detail");
      }
      return acquisitionResponse("acq_1", 1);
    };
    const client = createClient(fetch);
    await client.context("sess_1");

    await expect(client.release("sess_1")).rejects.toMatchObject({
      code: "TRANSPORT",
      details: { operation: "release", status: null },
    });
    expect(requests.filter((request) => request.path.endsWith("/control/release"))).toHaveLength(1);
    expect(client.debugInfo()).toMatchObject({
      activeContextCount: 0,
      lastFailureCode: "TRANSPORT",
      uncertainReleaseCount: 1,
    });
  });

  it.each([
    [401, {}, "AUTHENTICATION"],
    [428, { code: "CONTROL_ACQUISITION_ID_REQUIRED" }, "CONTROL_ACQUISITION_ID_REQUIRED"],
    [409, { code: "CONTROL_ACQUISITION_STALE" }, "CONTROL_ACQUISITION_STALE"],
    [409, {}, "CONTROL_CONFLICT"],
    [428, { code: "CONTROL_EPOCH_REQUIRED" }, "CONTROL_EPOCH_REQUIRED"],
    [409, { code: "CONTROL_EPOCH_STALE" }, "CONTROL_EPOCH_STALE"],
    [500, {}, "PERSISTENCE"],
  ] as const)("maps HTTP %s acquisition failures to %s", async (status, body, code) => {
    const client = createClient(async () => {
      return new Response(JSON.stringify(body), {
        headers: { "content-type": "application/json" },
        status,
      });
    });

    const failure = await client.context("sess_1").catch((error: unknown) => error);
    expect(failure).toBeInstanceOf(RestParticipantControlError);
    expect(failure).toMatchObject({ code });
    expect((failure as Error).cause).toBeUndefined();
  });
});

interface CapturedRequest {
  readonly body: Record<string, unknown>;
  readonly path: string;
}

function createClient(fetch: RestParticipantControlFetch): RestParticipantControlClient {
  return new RestParticipantControlClient(
    {
      instanceId: "inst_1",
      participantId: "part_1",
      runtimeKind: "generic_agent",
      serviceUrl: "http://tether.test",
    },
    {
      acquisitionIdFactory: () => "acq_1",
      fetch,
    },
  );
}

function createTimedClient(
  fetch: RestParticipantControlFetch,
  timers: RestParticipantControlTimerScheduler,
  options: Omit<RestParticipantControlClientOptions, "fetch" | "timers"> = {},
): RestParticipantControlClient {
  return new RestParticipantControlClient(
    {
      instanceId: "inst_1",
      participantId: "part_1",
      runtimeKind: "generic_agent",
      serviceUrl: "http://tether.test",
    },
    {
      acquisitionIdFactory: () => "acq_1",
      acquisitionTransportRetries: 0,
      now: () => Date.parse("2026-07-16T12:00:00.000Z"),
      ...options,
      fetch,
      timers,
    },
  );
}

function createControlFetch(requests: CapturedRequest[]): RestParticipantControlFetch {
  return async (url, init) => {
    const request = capture(url, init);
    requests.push(request);
    if (request.path.endsWith("/control/release")) {
      return new Response(JSON.stringify({ released: true }), {
        headers: { "content-type": "application/json" },
        status: 200,
      });
    }
    if (request.path.endsWith("/heartbeat")) {
      return new Response(
        JSON.stringify({
          controlEpoch: request.body.controlEpoch,
          leaseExpiresAt: "2026-07-16T12:02:00.000Z",
          participant: { participantId: "part_1" },
          renewAfterMs: 30_000,
        }),
        {
          headers: { "content-type": "application/json" },
          status: 200,
        },
      );
    }
    const acquisitionId =
      typeof request.body.acquisitionId === "string" ? request.body.acquisitionId : "acq_unknown";
    return acquisitionResponse(acquisitionId, requests.length);
  };
}

class FakeTimerScheduler implements RestParticipantControlTimerScheduler {
  private readonly entries: Array<{
    readonly callback: () => void;
    readonly delayMs: number;
    readonly handle: RestParticipantControlTimerHandle;
  }> = [];
  unrefCount = 0;

  /** Pending timer delays in scheduling order. */
  get delays(): number[] {
    return this.entries.map((entry) => entry.delayMs);
  }

  clearTimeout(handle: RestParticipantControlTimerHandle): void {
    const index = this.entries.findIndex((entry) => entry.handle === handle);
    if (index >= 0) {
      this.entries.splice(index, 1);
    }
  }

  /** Fires the earliest scheduled timer that is still pending. */
  fireNext(): void {
    const entry = this.entries.shift();
    if (!entry) {
      throw new Error("Expected a scheduled timer");
    }
    entry.callback();
  }

  /** Fires the earliest pending timer scheduled with exactly this delay. */
  fireDelay(delayMs: number): void {
    const index = this.entries.findIndex((entry) => entry.delayMs === delayMs);
    const entry = index >= 0 ? this.entries.splice(index, 1)[0] : undefined;
    if (!entry) {
      throw new Error(`Expected a scheduled timer of ${delayMs} ms`);
    }
    entry.callback();
  }

  setTimeout(callback: () => void, delayMs: number): RestParticipantControlTimerHandle {
    const handle: RestParticipantControlTimerHandle = {
      unref: () => {
        this.unrefCount += 1;
      },
    };
    this.entries.push({ callback, delayMs, handle });
    return handle;
  }
}

function acquisitionResponse(acquisitionId: string, controlEpoch: number): Response {
  return new Response(
    JSON.stringify({
      acquisitionId,
      acquisitionStatus: "claimed",
      controlEpoch,
      leaseExpiresAt: "2026-07-16T12:01:00.000Z",
      participant: { participantId: "part_1" },
      registrationStatus: "joined",
      renewAfterMs: 30_000,
    }),
    {
      headers: { "content-type": "application/json" },
      status: 201,
    },
  );
}

function capture(url: URL, init: RequestInit): CapturedRequest {
  const body = typeof init.body === "string" ? (JSON.parse(init.body) as unknown) : {};
  if (typeof body !== "object" || body === null || Array.isArray(body)) {
    throw new Error("Expected object request body");
  }
  return {
    body: body as Record<string, unknown>,
    path: url.pathname,
  };
}
