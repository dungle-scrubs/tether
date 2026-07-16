import { describe, expect, it, vi } from "vitest";

import {
  RestParticipantControlClient,
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
  private readonly callbacks: Array<() => void> = [];
  unrefCount = 0;

  clearTimeout(): void {}

  fireNext(): void {
    const callback = this.callbacks.shift();
    if (!callback) {
      throw new Error("Expected a scheduled renewal");
    }
    callback();
  }

  setTimeout(callback: () => void): RestParticipantControlTimerHandle {
    this.callbacks.push(callback);
    return {
      unref: () => {
        this.unrefCount += 1;
      },
    };
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
