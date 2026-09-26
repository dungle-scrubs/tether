import { describe, expect, it, vi } from "vitest";

import {
  RestParticipantControlClient,
  type RestParticipantControlClientOptions,
  type RestParticipantControlContext,
  RestParticipantControlError,
  type RestParticipantControlFetch,
  type RestParticipantControlTimerHandle,
  type RestParticipantControlTimerScheduler,
} from "../src/rest-participant-control-client.js";

describe("RestParticipantControlClient", () => {
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
      expect(timers.unrefCount).toBe(2);
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

  it("notifies before scheduling a renewal transport retry", async () => {
    const events: unknown[] = [];
    const pendingAtNotification: number[] = [];
    const timers = new FakeTimerScheduler();
    const client = createClient(
      async (url, init) => {
        const request = capture(url, init);
        if (request.path.endsWith("/heartbeat")) {
          throw new Error("renewal transport failed");
        }
        return acquisitionResponse("acq_1", 1);
      },
      {
        now: () => Date.parse("2026-07-16T12:00:00.000Z"),
        onContextUncertain: (payload) => {
          pendingAtNotification.push(client.debugInfo().renewalTimerCount);
          events.push(payload);
        },
        timers,
      },
    );
    await client.context("sess_1");
    timers.fireNext();

    await vi.waitFor(() => expect(events).toHaveLength(1));
    expect(events[0]).toEqual({
      acquisitionId: "acq_1",
      controlEpoch: 1,
      instanceId: "inst_1",
      reason: "TRANSPORT",
      sessionId: "sess_1",
    });
    expect(Object.keys(events[0] as Record<string, unknown>).sort()).toEqual([
      "acquisitionId",
      "controlEpoch",
      "instanceId",
      "reason",
      "sessionId",
    ]);
    // At notification time, no recovery timer has been scheduled yet.
    // The expiry timer is independent of the renewal timer and still
    // pending, so check the renewal timer count rather than the total.
    expect(pendingAtNotification).toEqual([0]);
    expect(Object.isFrozen(events[0])).toBe(true);
    expect(timers.pendingCount).toBe(2);
    expect(client.debugInfo().renewalTimerCount).toBe(1);
    expect(client.debugInfo().renewalTimerCount).toBe(1);
    // The notified context is still installed: a fresh context() returns it.
    expect(await client.context("sess_1")).toMatchObject({
      acquisitionId: "acq_1",
      controlEpoch: 1,
    });
  });

  it.each([
    "CONTROL_EPOCH_STALE",
    "CONTROL_CONFLICT",
    "CONTROL_EPOCH_REQUIRED",
  ] as const)("notifies before invalidating %s renewal without recovery", async (code) => {
    const events: unknown[] = [];
    const activeAtNotification: number[] = [];
    const timers = new FakeTimerScheduler();
    const client = createClient(
      async (url, init) => {
        const request = capture(url, init);
        if (request.path.endsWith("/heartbeat")) {
          return new Response(JSON.stringify({ code }), { status: 409 });
        }
        return acquisitionResponse("acq_1", 1);
      },
      {
        now: () => Date.parse("2026-07-16T12:00:00.000Z"),
        onContextUncertain: (payload) => {
          activeAtNotification.push(client.debugInfo().activeContextCount);
          events.push(payload);
        },
        timers,
      },
    );
    await client.context("sess_1");
    timers.fireNext();

    await vi.waitFor(() => expect(events).toHaveLength(1));
    expect(events[0]).toMatchObject({
      acquisitionId: "acq_1",
      controlEpoch: 1,
      instanceId: "inst_1",
      reason: code,
      sessionId: "sess_1",
    });
    expect(activeAtNotification).toEqual([1]);
    expect(timers.pendingCount).toBe(0);
    expect(client.debugInfo().activeContextCount).toBe(0);
  });

  it.each([
    "malformed",
    "epoch mismatch",
  ])("notifies on %s renewal response and schedules recovery", async (kind) => {
    const events: unknown[] = [];
    const timers = new FakeTimerScheduler();
    const client = createClient(
      async (url, init) => {
        const request = capture(url, init);
        if (request.path.endsWith("/heartbeat")) {
          if (kind === "malformed") return new Response("{invalid", { status: 200 });
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
      {
        now: () => Date.parse("2026-07-16T12:00:00.000Z"),
        onContextUncertain: (payload) => events.push(payload),
        timers,
      },
    );
    await client.context("sess_1");
    timers.fireNext();

    await vi.waitFor(() => expect(events).toHaveLength(1));
    expect(events[0]).toMatchObject({ reason: "INVALID_RESPONSE" });
    // Renewal failure leaves the expiry timer pending alongside the
    // scheduled recovery timer.
    expect(timers.pendingCount).toBe(2);
    expect(client.debugInfo().renewalTimerCount).toBe(1);
    expect(timers.pendingCount - client.debugInfo().renewalTimerCount).toBe(1);
  });

  it("notifies when context() first observes lease expiry", async () => {
    const events: unknown[] = [];
    let now = Date.parse("2026-07-16T12:00:00.000Z");
    const timers = new FakeTimerScheduler();
    const client = createClient(createControlFetch([]), {
      acquisitionIdFactory: () => `acq_${(events.length + 1).toString()}`,
      now: () => now,
      onContextUncertain: (payload) => events.push(payload),
      timers,
    });

    const original = await client.context("sess_1");
    now = Date.parse(original.leaseExpiresAt) + 1;
    await client.context("sess_1");

    await vi.waitFor(() => expect(events).toHaveLength(1));
    expect(events[0]).toEqual({
      acquisitionId: "acq_1",
      controlEpoch: 1,
      instanceId: "inst_1",
      reason: "LEASE_EXPIRED",
      sessionId: "sess_1",
    });
  });

  it("deduplicates once per generation across error, success, and a subsequent error", async () => {
    const events: unknown[] = [];
    const requests: CapturedRequest[] = [];
    const timers = new FakeTimerScheduler();
    let heartbeatAttempts = 0;
    let now = Date.parse("2026-07-16T12:00:00.000Z");
    const client = createClient(
      async (url, init) => {
        const request = capture(url, init);
        requests.push(request);
        if (request.path.endsWith("/heartbeat")) {
          heartbeatAttempts += 1;
          if (heartbeatAttempts === 1) {
            throw new Error("first renewal failed");
          }
          if (heartbeatAttempts === 2) {
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
          throw new Error("third renewal failed");
        }
        return acquisitionResponse("acq_1", requests.length);
      },
      {
        now: () => now,
        onContextUncertain: (payload) => events.push(payload),
        timers,
      },
    );

    await client.context("sess_1");

    timers.fireNext();
    await vi.waitFor(() => expect(events).toHaveLength(1));
    expect(events[0]).toMatchObject({ reason: "TRANSPORT" });

    timers.fireNext();
    // Wait for the recovery renewal to settle. Two new timers (renewal and
    // expiry) are scheduled after a successful response, so unrefCount
    // increments by two from the post-recovery count.
    await vi.waitFor(() => expect(timers.unrefCount).toBe(5));
    // No restore event after successful renewal.
    expect(events).toHaveLength(1);

    timers.fireNext();
    await vi.waitFor(() => expect(timers.unrefCount).toBe(6));
    expect(events).toHaveLength(1);
    expect(events).toHaveLength(1);
    now = Date.parse("2026-07-16T12:02:00.001Z");
    await client.context("sess_1");
    expect(events).toHaveLength(1);
  });

  it.each([
    "success",
    "stale",
    "transport",
  ])("ignores late old %s after a replacement acquisition", async (outcome) => {
    const events: unknown[] = [];
    const timers = new FakeTimerScheduler();
    const pending = Promise.withResolvers<Response>();
    let acquisitions = 0;
    const client = createClient(
      async (url) => {
        if (url.pathname.endsWith("/heartbeat")) return pending.promise;
        return acquisitionResponse("acq_1", ++acquisitions);
      },
      {
        now: () => Date.parse("2026-07-16T12:00:00.000Z"),
        onContextUncertain: (payload) => events.push(payload),
        timers,
      },
    );
    const old = await client.context("sess_1");
    timers.fireNext();
    client.invalidate(old);
    const replacement = await client.context("sess_1");
    if (outcome === "transport") pending.reject(new Error("late failure"));
    else
      pending.resolve(
        outcome === "success"
          ? renewalResponse(1)
          : new Response(JSON.stringify({ code: "CONTROL_EPOCH_STALE" }), { status: 409 }),
      );
    await vi.waitFor(() => expect(client.debugInfo().boundary.activeOperations).toBe(0));
    expect(events).toEqual([]);
    expect(await client.context("sess_1")).toBe(replacement);
    expect(client.invalidate(old)).toBe(false);
    // Replacement installed both a renewal and an expiry timer.
    expect(timers.pendingCount).toBe(2);
    expect(client.debugInfo().renewalTimerCount).toBe(1);
    expect(timers.pendingCount - client.debugInfo().renewalTimerCount).toBe(1);
  });

  it("ignores an older renewal failure after another renewal installs the same tuple", async () => {
    const events: unknown[] = [];
    const timers = new FakeTimerScheduler();
    const pending = Promise.withResolvers<Response>();
    let renewals = 0;
    const client = createClient(
      async (url) => {
        if (url.pathname.endsWith("/heartbeat"))
          return ++renewals === 1 ? pending.promise : renewalResponse(1);
        return acquisitionResponse("acq_1", 1);
      },
      {
        now: () => Date.parse("2026-07-16T12:00:00.000Z"),
        onContextUncertain: (payload) => events.push(payload),
        timers,
      },
    );
    const old = await client.context("sess_1");
    const timerDelivery = timers.fireNext();
    // Duplicate delivery creates two in-flight renewals from the same snapshot.
    timerDelivery();
    // The second renewal completes successfully and schedules a fresh
    // renewal plus a fresh expiry timer for the renewed lease. The first
    // renewal is hung so it does not increment unrefCount.
    await vi.waitFor(() => expect(timers.unrefCount).toBe(4));
    const renewed = await client.context("sess_1");
    expect(renewed).not.toBe(old);
    expect(renewed.controlEpoch).toBe(old.controlEpoch);
    pending.resolve(new Response(JSON.stringify({ code: "CONTROL_EPOCH_STALE" }), { status: 409 }));
    await vi.waitFor(() => expect(client.debugInfo().boundary.activeOperations).toBe(0));
    expect(events).toEqual([]);
    expect(await client.context("sess_1")).toBe(renewed);
    expect(timers.pendingCount).toBe(2);
  });

  it("lets callback invalidation use a snapshot from before a successful renewal", async () => {
    const events: unknown[] = [];
    const invalidations: boolean[] = [];
    const timers = new FakeTimerScheduler();
    let original: RestParticipantControlContext;
    let renewals = 0;
    const client = createClient(
      async (url) => {
        if (url.pathname.endsWith("/heartbeat")) {
          if (++renewals === 1) return renewalResponse(1);
          throw new Error("renewal failed");
        }
        return acquisitionResponse("acq_1", 1);
      },
      {
        now: () => Date.parse("2026-07-16T12:00:00.000Z"),
        onContextUncertain: (payload) => {
          events.push(payload);
          invalidations.push(client.invalidate(original));
        },
        timers,
      },
    );
    original = await client.context("sess_1");
    timers.fireNext();
    // Renewal success schedules a new renewal timer and a fresh expiry
    // timer for the renewed lease.
    await vi.waitFor(() => expect(timers.unrefCount).toBe(4));
    expect(await client.context("sess_1")).not.toBe(original);
    timers.fireNext();
    await vi.waitFor(() => expect(events).toHaveLength(1));
    expect(invalidations).toEqual([true]);
    expect(timers.pendingCount).toBe(0);
    expect(client.debugInfo().activeContextCount).toBe(0);
    expect(client.invalidate(original)).toBe(false);
  });

  it("invalidates the exact context and suppresses retry when the callback throws", async () => {
    const events: unknown[] = [];
    const timers = new FakeTimerScheduler();
    const client = createClient(
      async (url, init) => {
        const request = capture(url, init);
        if (request.path.endsWith("/heartbeat")) {
          throw new Error("renewal transport failed");
        }
        return acquisitionResponse("acq_1", 1);
      },
      {
        now: () => Date.parse("2026-07-16T12:00:00.000Z"),
        onContextUncertain: (payload) => {
          events.push(payload);
          throw new Error("caller exploded inside the callback");
        },
        timers,
      },
    );

    await client.context("sess_1");
    timers.fireNext();

    await vi.waitFor(() => expect(events).toHaveLength(1));
    expect(timers.pendingCount).toBe(0);
    expect(client.debugInfo()).toMatchObject({
      lastFailureCode: "TRANSPORT",
      activeContextCount: 0,
      renewalTimerCount: 0,
    });
  });

  it("does not leak raw error data into the bounded uncertainty payload", async () => {
    const events: unknown[] = [];
    const timers = new FakeTimerScheduler();
    const client = createClient(
      async (url, init) => {
        const request = capture(url, init);
        if (request.path.endsWith("/heartbeat")) {
          const sensitive = new Error("connection reset token=SECRET-XYZ");
          sensitive.stack = "Error: connection reset token=SECRET-XYZ\n  at secret-location";
          throw sensitive;
        }
        return acquisitionResponse("acq_1", 1);
      },
      {
        now: () => Date.parse("2026-07-16T12:00:00.000Z"),
        onContextUncertain: (payload) => events.push(payload),
        timers,
      },
    );

    await client.context("sess_1");
    timers.fireNext();

    await vi.waitFor(() => expect(events).toHaveLength(1));
    const serialized = JSON.stringify(events[0]);
    expect(serialized).not.toContain("SECRET-XYZ");
    expect(serialized).not.toContain("connection reset");
    expect(serialized).not.toContain("secret-location");
    expect(Object.keys(events[0] as Record<string, unknown>).sort()).toEqual([
      "acquisitionId",
      "controlEpoch",
      "instanceId",
      "reason",
      "sessionId",
    ]);
  });

  /** A hung renewal cannot defer expiry notification until the next context() call. */
  it("notifies LEASE_EXPIRED before invalidation when heartbeat hangs past expiry", async () => {
    const events: unknown[] = [];
    const activeAtNotification: number[] = [];
    const timers = new FakeTimerScheduler();
    const client = createClient(
      async (url) => {
        if (url.pathname.endsWith("/heartbeat")) {
          return new Promise<Response>(() => undefined);
        }
        return acquisitionResponse("acq_1", 1);
      },
      {
        now: () => timers.now,
        onContextUncertain: (payload) => {
          activeAtNotification.push(client.debugInfo().activeContextCount);
          events.push(payload);
        },
        timers,
      },
    );
    await client.context("sess_1");
    const expiry = [...timers.pendingHandles].find(
      (handle) => timers.dueTimeFor(handle) === 60_000,
    );
    if (!expiry) throw new Error("Expected expiry timer");
    timers.fireHandle(expiry, false);
    expect(events).toEqual([]);
    expect(client.debugInfo().activeContextCount).toBe(1);
    expect(timers.pendingCount).toBe(2);
    timers.fireNext();
    expect(timers.now).toBe(Date.parse("2026-07-16T12:00:30.000Z"));
    expect(events).toEqual([]);
    expect(client.debugInfo().boundary.activeOperations).toBe(1);
    timers.fireNext();
    expect(timers.now).toBe(Date.parse("2026-07-16T12:01:00.000Z"));
    expect(events).toHaveLength(1);
    expect(events[0]).toMatchObject({
      acquisitionId: "acq_1",
      controlEpoch: 1,
      instanceId: "inst_1",
      reason: "LEASE_EXPIRED",
      sessionId: "sess_1",
    });
    expect(activeAtNotification).toEqual([1]);
    expect(client.debugInfo().activeContextCount).toBe(0);
  });

  it("notifies LEASE_EXPIRED again for a replacement generation", async () => {
    const events: unknown[] = [];
    const timers = new FakeTimerScheduler();
    let epoch = 1;
    const client = createClient(
      async (url) => {
        if (url.pathname.endsWith("/heartbeat")) {
          return new Promise<Response>(() => undefined);
        }
        return acquisitionResponse("acq_1", epoch++, new Date(timers.now + 60_000).toISOString());
      },
      {
        now: () => timers.now,
        onContextUncertain: (payload) => events.push(payload),
        timers,
      },
    );
    await client.context("sess_1");
    timers.fireNext();
    timers.fireNext();
    expect(events).toHaveLength(1);
    expect(events[0]).toMatchObject({ controlEpoch: 1, reason: "LEASE_EXPIRED" });

    // A new context() call must re-notify after expiry on the replacement.
    await client.context("sess_1");
    timers.fireNext();
    timers.fireNext();
    expect(events).toHaveLength(2);
    expect(events[1]).toMatchObject({ controlEpoch: 2, reason: "LEASE_EXPIRED" });
  });

  it("ignores a late expiry timer delivered after a successful renewal", async () => {
    const events: unknown[] = [];
    const timers = new FakeTimerScheduler();
    const client = createClient(createControlFetch([]), {
      now: () => timers.now,
      onContextUncertain: (payload) => events.push(payload),
      timers,
    });
    await client.context("sess_1");
    const captured = [...timers.pendingHandles];
    const initialExpiry = captured.find((handle) => {
      const dueTime = timers.dueTimeFor(handle);
      return dueTime !== undefined && dueTime > 45_000;
    });
    expect(initialExpiry).toBeDefined();

    timers.fireNext(); // Renewal fires; will succeed and clear the old expiry.
    // Wait for the renewal to complete and re-schedule a fresh expiry
    // timer for the renewed lease. The old expiry handle is no longer
    // current once the new timer is installed.
    await vi.waitFor(() => expect(timers.unrefCount).toBe(4));

    if (!initialExpiry) {
      throw new Error("Expected initial expiry handle to be captured");
    }
    timers.fireHandle(initialExpiry);
    expect(events).toEqual([]);
    expect(timers.pendingCount).toBe(2);
  });

  it.each([
    "expire",
    "release",
    "stop",
  ] as const)("re-arms a capped expiry timer before %s", async (action) => {
    const timers = new FakeTimerScheduler();
    const events: unknown[] = [];
    const maximum = 2_147_483_647;
    const deadline = timers.now + maximum + 60_000;
    const client = createClient(
      async (url) => {
        if (url.pathname.endsWith("/control/release"))
          return new Response(JSON.stringify({ released: true }), { status: 200 });
        return acquisitionResponse("acq_1", 1, new Date(deadline).toISOString());
      },
      {
        now: () => timers.now,
        onContextUncertain: (payload) => events.push(payload),
        timers,
      },
    );
    await client.context("sess_1");
    const capped = [...timers.pendingHandles].find(
      (handle) => timers.dueTimeFor(handle) === maximum,
    );
    if (!capped) throw new Error("Expected capped expiry timer");
    timers.fireHandle(capped);
    expect(events).toEqual([]);
    expect(client.debugInfo().activeContextCount).toBe(1);
    const rearmed = [...timers.pendingHandles].find(
      (handle) => timers.dueTimeFor(handle) === maximum + 60_000,
    );
    if (!rearmed) throw new Error("Expected re-armed expiry timer");
    if (action === "release") await client.release("sess_1");
    if (action === "stop") await client.stop();
    if (action !== "expire") expect(timers.pendingCount).toBe(0);
    timers.fireHandle(rearmed);
    expect(events).toEqual(
      action === "expire"
        ? [
            {
              sessionId: "sess_1",
              instanceId: "inst_1",
              acquisitionId: "acq_1",
              controlEpoch: 1,
              reason: "LEASE_EXPIRED",
            },
          ]
        : [],
    );
    expect(client.debugInfo().activeContextCount).toBe(0);
    expect(timers.pendingCount).toBe(0);
  });

  it("ignores a cancelled expiry callback after replacement acquisition", async () => {
    const timers = new FakeTimerScheduler();
    const events: unknown[] = [];
    let epoch = 0;
    const client = createClient(
      async () =>
        acquisitionResponse("acq_1", ++epoch, new Date(timers.now + 60_000).toISOString()),
      {
        now: () => timers.now,
        onContextUncertain: (payload) => events.push(payload),
        timers,
      },
    );
    const old = await client.context("sess_1");
    const expiry = [...timers.pendingHandles].find(
      (handle) => timers.dueTimeFor(handle) === 60_000,
    );
    if (!expiry) throw new Error("Expected expiry timer");
    client.invalidate(old);
    timers.setCurrentTime(5_000);
    const replacement = await client.context("sess_1");
    timers.fireHandle(expiry);
    expect(events).toEqual([]);
    expect(await client.context("sess_1")).toBe(replacement);
    expect(timers.pendingCount).toBe(2);
  });

  it("cancels the expiry timer on invalidate, release, and stop", async () => {
    // invalidate
    {
      const timers = new FakeTimerScheduler();
      const client = createClient(createControlFetch([]), {
        now: () => timers.now,
        onContextUncertain: () => undefined,
        timers,
      });
      const ctx = await client.context("sess_1");
      expect(timers.pendingCount).toBe(2);
      expect(client.debugInfo().renewalTimerCount).toBe(1);
      expect(timers.pendingCount - client.debugInfo().renewalTimerCount).toBe(1);
      client.invalidate(ctx);
      expect(timers.pendingCount).toBe(0);
    }
    // release
    {
      const timers = new FakeTimerScheduler();
      const client = createClient(createControlFetch([]), {
        now: () => timers.now,
        onContextUncertain: () => undefined,
        timers,
      });
      await client.context("sess_1");
      expect(timers.pendingCount).toBe(2);
      await client.release("sess_1");
      expect(timers.pendingCount).toBe(0);
    }
    // stop clears expiry timers synchronously even when release hangs.
    {
      const timers = new FakeTimerScheduler();
      const client = createClient(
        async (url, init) => {
          const body = typeof init.body === "string" ? (JSON.parse(init.body) as unknown) : null;
          if (url.pathname.endsWith("/control/release")) {
            return new Promise<Response>(() => undefined);
          }
          if (url.pathname.endsWith("/heartbeat")) return renewalResponse(1);
          return acquisitionResponse(
            body && typeof body === "object" && "acquisitionId" in body
              ? String((body as { acquisitionId: unknown }).acquisitionId)
              : "acq_1",
            1,
          );
        },
        {
          now: () => timers.now,
          onContextUncertain: () => undefined,
          shutdownPendingWaitMs: 0,
          timers,
        },
      );
      await client.context("sess_1");
      expect(timers.pendingCount).toBe(2);
      const stopPromise = client.stop();
      // Expiry timers clear synchronously so a hung release cannot keep
      // them active past stop. The renewal timer is cleared at the end
      // of stop after releases settle.
      expect(timers.pendingCount - client.debugInfo().renewalTimerCount).toBe(0);
      expect(timers.pendingCount).toBe(1);
      // The hung release keeps the stop promise pending; ignore.
      stopPromise.catch(() => undefined);
    }
  });

  it("does not schedule an expiry timer when no onContextUncertain is configured", async () => {
    const timers = new FakeTimerScheduler();
    const client = createClient(createControlFetch([]), {
      now: () => timers.now,
      timers,
    });
    await client.context("sess_1");
    expect(timers.pendingCount).toBe(1);
    expect(client.debugInfo().renewalTimerCount).toBe(1);
    expect(timers.pendingCount - client.debugInfo().renewalTimerCount).toBe(0);
  });

  it("invalidates the exact context when the callback throws on the expiry path", async () => {
    const events: unknown[] = [];
    const timers = new FakeTimerScheduler();
    const client = createClient(
      async (url) => {
        if (url.pathname.endsWith("/heartbeat")) {
          return new Promise<Response>(() => undefined);
        }
        return acquisitionResponse("acq_1", 1);
      },
      {
        now: () => timers.now,
        onContextUncertain: (payload) => {
          events.push(payload);
          throw new Error("caller exploded inside the callback");
        },
        timers,
      },
    );
    await client.context("sess_1");
    timers.fireNext();
    timers.fireNext();
    expect(events).toHaveLength(1);
    expect(events[0]).toMatchObject({ reason: "LEASE_EXPIRED" });
    expect(client.debugInfo().activeContextCount).toBe(0);
    expect(timers.pendingCount).toBe(0);
  });

  it("invalidates on LEASE_EXPIRED even after a renewal error already notified", async () => {
    const events: unknown[] = [];
    const timers = new FakeTimerScheduler();
    let renewals = 0;
    const client = createClient(
      async (url) => {
        if (url.pathname.endsWith("/heartbeat")) {
          renewals += 1;
          if (renewals === 1) throw new Error("first renewal failed");
          return new Promise<Response>(() => undefined);
        }
        return acquisitionResponse("acq_1", 1);
      },
      {
        now: () => timers.now,
        onContextUncertain: (payload) => events.push(payload),
        timers,
      },
    );
    await client.context("sess_1");
    const expiryHandle = [...timers.pendingHandles].find(
      (handle) => (timers.dueTimeFor(handle) ?? 0) > 45_000,
    );
    expect(expiryHandle).toBeDefined();

    timers.fireNext(); // Renewal fails; recovery scheduled; TRANSPORT notified.
    await vi.waitFor(() => expect(events).toHaveLength(1));
    expect(events[0]).toMatchObject({ reason: "TRANSPORT" });

    timers.fireNext(); // Recovery fires; subsequent renewal is hung.
    // The expiry timer is still pending. Firing it after the renewal
    // error must still invalidate even though TRANSPORT was already
    // delivered for this generation.
    if (!expiryHandle) {
      throw new Error("Expected expiry handle to be captured");
    }
    timers.fireHandle(expiryHandle);
    expect(events).toHaveLength(1);
    expect(client.debugInfo().activeContextCount).toBe(0);
  });

  it.each([
    401, 500,
  ] as const)("notifies before scheduling recovery on %s renewal failure", async (status) => {
    const events: unknown[] = [];
    const timers = new FakeTimerScheduler();
    const client = createClient(
      async (url, init) => {
        const request = capture(url, init);
        if (request.path.endsWith("/heartbeat")) {
          return new Response(JSON.stringify({}), { status });
        }
        return acquisitionResponse("acq_1", 1);
      },
      {
        now: () => timers.now,
        onContextUncertain: (payload) => events.push(payload),
        timers,
      },
    );
    await client.context("sess_1");
    timers.fireNext();
    await vi.waitFor(() => expect(events).toHaveLength(1));
    const expectedReason = status === 401 ? "AUTHENTICATION" : "PERSISTENCE";
    expect(events[0]).toMatchObject({ reason: expectedReason });
    // Recovery timer scheduled after the renewal failure; expiry timer
    // remains pending independently.
    expect(client.debugInfo().renewalTimerCount).toBe(1);
    expect(timers.pendingCount - client.debugInfo().renewalTimerCount).toBe(1);
  });
});

interface CapturedRequest {
  readonly body: Record<string, unknown>;
  readonly path: string;
}

function createClient(
  fetch: RestParticipantControlFetch,
  extraOptions: Partial<RestParticipantControlClientOptions> = {},
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
      fetch,
      ...extraOptions,
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
  private readonly callbacks = new Map<
    RestParticipantControlTimerHandle,
    { callback: () => void; dueTime: number }
  >();
  private readonly allCallbacks = new Map<
    RestParticipantControlTimerHandle,
    { callback: () => void; dueTime: number }
  >();
  private baseTime = 0;
  unrefCount = 0;

  get pendingCount(): number {
    return this.callbacks.size;
  }

  get now(): number {
    return Date.parse("2026-07-16T12:00:00.000Z") + this.baseTime;
  }

  setCurrentTime(value: number): void {
    this.baseTime = value;
  }

  setTimeout(callback: () => void, delayMs: number): RestParticipantControlTimerHandle {
    const handle = {
      unref: () => {
        this.unrefCount += 1;
      },
    };
    const entry = { callback, dueTime: this.baseTime + delayMs };
    this.callbacks.set(handle, entry);
    this.allCallbacks.set(handle, entry);
    return handle;
  }

  clearTimeout(handle: RestParticipantControlTimerHandle): void {
    this.callbacks.delete(handle);
  }

  fireNext(): () => void {
    let chosen: RestParticipantControlTimerHandle | undefined;
    let chosenDue = Number.POSITIVE_INFINITY;
    for (const [handle, entry] of this.callbacks) {
      if (entry.dueTime < chosenDue) {
        chosen = handle;
        chosenDue = entry.dueTime;
      }
    }
    if (!chosen) {
      throw new Error("Expected a scheduled renewal");
    }
    this.callbacks.delete(chosen);
    this.baseTime = Math.max(this.baseTime, chosenDue);
    const callback = this.allCallbacks.get(chosen)?.callback ?? (() => undefined);
    callback();
    return callback;
  }

  fireHandle(handle: RestParticipantControlTimerHandle, advanceClock = true): void {
    const entry = this.allCallbacks.get(handle);
    if (!entry) {
      throw new Error("Unknown timer handle");
    }
    this.callbacks.delete(handle);
    if (advanceClock) this.baseTime = Math.max(this.baseTime, entry.dueTime);
    entry.callback();
  }

  get pendingHandles(): IterableIterator<RestParticipantControlTimerHandle> {
    return this.callbacks.keys();
  }

  dueTimeFor(handle: RestParticipantControlTimerHandle): number | undefined {
    return this.allCallbacks.get(handle)?.dueTime;
  }
}

function renewalResponse(controlEpoch: number): Response {
  return new Response(
    JSON.stringify({
      controlEpoch,
      leaseExpiresAt: "2026-07-16T12:02:00.000Z",
      participant: { participantId: "part_1" },
      renewAfterMs: 30_000,
    }),
    { headers: { "content-type": "application/json" }, status: 200 },
  );
}

function acquisitionResponse(
  acquisitionId: string,
  controlEpoch: number,
  leaseExpiresAt = "2026-07-16T12:01:00.000Z",
): Response {
  return new Response(
    JSON.stringify({
      acquisitionId,
      acquisitionStatus: "claimed",
      controlEpoch,
      leaseExpiresAt,
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
