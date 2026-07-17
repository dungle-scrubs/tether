import { describe, expect, it } from "vitest";

import { SerialEventDelivery } from "../src/serial-event-delivery.js";

interface TestEvent {
  readonly eventId: string;
  readonly seq: number;
}

describe("SerialEventDelivery", () => {
  it("settles a replay marker after its preceding event", async () => {
    const actions: string[] = [];
    let resolveReplay: (() => void) | undefined;
    const replaySettled = new Promise<void>((resolve) => {
      resolveReplay = resolve;
    });
    const delivery = new SerialEventDelivery<TestEvent>({
      handlerTimeoutMs: 1_000,
      initialSeq: 0,
      maxQueueSize: 10,
      onOutcome: (outcome) => {
        if (outcome.kind === "replay-complete") {
          actions.push("replay");
          resolveReplay?.();
        }
      },
    });
    delivery.onEvent((event) => {
      actions.push(`event:${event.seq}`);
    });

    delivery.enqueueEvent({ eventId: "evt_1", seq: 1 });
    delivery.enqueueReplayComplete();
    await replaySettled;

    expect(actions).toEqual(["event:1", "replay"]);
  });

  it("runs one guarded drain for an asynchronous socket burst", async () => {
    const firstHandler = createDeferred<void>();
    const secondHandled = createDeferred<void>();
    const entered: number[] = [];
    const exited: number[] = [];
    let inFlight = 0;
    let maxInFlight = 0;
    const delivery = new SerialEventDelivery<TestEvent>({
      handlerTimeoutMs: 1_000,
      initialSeq: 0,
      maxQueueSize: 10,
      onOutcome: (outcome) => {
        if (outcome.kind === "event-handled" && outcome.event.seq === 2) {
          secondHandled.resolve();
        }
      },
    });
    delivery.onEvent(async (event) => {
      entered.push(event.seq);
      inFlight += 1;
      maxInFlight = Math.max(maxInFlight, inFlight);
      if (event.seq === 1) {
        await firstHandler.promise;
      }
      inFlight -= 1;
      exited.push(event.seq);
    });

    delivery.enqueueEvent({ eventId: "evt_1", seq: 1 });
    delivery.enqueueEvent({ eventId: "evt_2", seq: 2 });
    await Promise.resolve();

    expect(entered).toEqual([1]);
    expect(exited).toEqual([]);
    firstHandler.resolve();
    await secondHandled.promise;

    expect(entered).toEqual([1, 2]);
    expect(exited).toEqual([1, 2]);
    expect(maxInFlight).toBe(1);
  });

  it("returns a typed failure when a synchronous handler throws", async () => {
    const delivered: number[] = [];
    const outcomes: string[] = [];
    const delivery = new SerialEventDelivery<TestEvent>({
      handlerTimeoutMs: 1_000,
      initialSeq: 0,
      maxQueueSize: 10,
      onOutcome: (outcome) => {
        outcomes.push(outcome.kind);
      },
    });
    delivery.onEvent((event) => {
      delivered.push(event.seq);
      if (event.seq === 1) {
        throw new Error("handler failed");
      }
    });

    delivery.enqueueEvent({ eventId: "evt_1", seq: 1 });
    delivery.enqueueEvent({ eventId: "evt_2", seq: 2 });
    await new Promise((resolve) => setTimeout(resolve, 0));

    expect(outcomes).toEqual(["handler-failed"]);
    expect(delivered).toEqual([1]);
  });

  it("returns a typed failure when an asynchronous handler rejects", async () => {
    const outcomes: string[] = [];
    const delivery = new SerialEventDelivery<TestEvent>({
      handlerTimeoutMs: 1_000,
      initialSeq: 0,
      maxQueueSize: 10,
      onOutcome: (outcome) => {
        outcomes.push(outcome.kind);
      },
    });
    delivery.onEvent(async () => {
      await Promise.resolve();
      throw new Error("async handler failed");
    });

    delivery.enqueueEvent({ eventId: "evt_1", seq: 1 });
    await new Promise((resolve) => setTimeout(resolve, 0));

    expect(outcomes).toEqual(["handler-failed"]);
  });

  it("applies handler membership changes to the next event", async () => {
    const firstHandler = createDeferred<void>();
    const secondHandled = createDeferred<void>();
    const actions: string[] = [];
    const delivery = new SerialEventDelivery<TestEvent>({
      handlerTimeoutMs: 1_000,
      initialSeq: 0,
      maxQueueSize: 10,
      onOutcome: (outcome) => {
        if (outcome.kind === "event-handled" && outcome.event.seq === 2) {
          secondHandled.resolve();
        }
      },
    });
    const unsubscribeFirst = delivery.onEvent(async (event) => {
      actions.push(`first:start:${event.seq}`);
      await firstHandler.promise;
      actions.push(`first:end:${event.seq}`);
    });

    delivery.enqueueEvent({ eventId: "evt_1", seq: 1 });
    await Promise.resolve();
    unsubscribeFirst();
    delivery.onEvent((event) => {
      actions.push(`second:${event.seq}`);
    });
    delivery.enqueueEvent({ eventId: "evt_2", seq: 2 });
    firstHandler.resolve();
    await secondHandled.promise;

    expect(actions).toEqual(["first:start:1", "first:end:1", "second:2"]);
  });

  it("holds an event until at least one handler is registered", async () => {
    const handled = createDeferred<void>();
    const delivery = new SerialEventDelivery<TestEvent>({
      handlerTimeoutMs: 1_000,
      initialSeq: 0,
      maxQueueSize: 10,
      onOutcome: (outcome) => {
        if (outcome.kind === "event-handled") {
          handled.resolve();
        }
      },
    });

    delivery.enqueueEvent({ eventId: "evt_1", seq: 1 });
    await Promise.resolve();

    expect(delivery.debugInfo()).toMatchObject({
      lastHandledSeq: 0,
      queueSize: 1,
    });

    delivery.onEvent(() => undefined);
    await handled.promise;

    expect(delivery.debugInfo()).toMatchObject({
      lastHandledSeq: 1,
      queueSize: 0,
    });
  });

  it("ignores a duplicate sequence without redelivery or cursor movement", async () => {
    const handled = createDeferred<void>();
    const delivered: number[] = [];
    const delivery = new SerialEventDelivery<TestEvent>({
      handlerTimeoutMs: 1_000,
      initialSeq: 0,
      maxQueueSize: 10,
      onOutcome: (outcome) => {
        if (outcome.kind === "event-handled") {
          handled.resolve();
        }
      },
    });
    delivery.onEvent((event) => {
      delivered.push(event.seq);
    });

    delivery.enqueueEvent({ eventId: "evt_1", seq: 1 });
    await handled.promise;
    delivery.enqueueEvent({ eventId: "evt_1_duplicate", seq: 1 });
    await Promise.resolve();

    expect(delivered).toEqual([1]);
    expect(delivery.debugInfo()).toMatchObject({
      lastHandledSeq: 1,
      lastReceivedSeq: 1,
      queueSize: 0,
    });
  });

  it("returns a typed terminal outcome before delivering a sequence gap", async () => {
    const delivered: number[] = [];
    const outcomes: string[] = [];
    const delivery = new SerialEventDelivery<TestEvent>({
      handlerTimeoutMs: 1_000,
      initialSeq: 0,
      maxQueueSize: 10,
      onOutcome: (outcome) => {
        outcomes.push(outcome.kind);
      },
    });
    delivery.onEvent((event) => {
      delivered.push(event.seq);
    });

    delivery.enqueueEvent({ eventId: "evt_2", seq: 2 });
    await Promise.resolve();

    expect(outcomes).toEqual(["non-contiguous-event"]);
    expect(delivered).toEqual([]);
    expect(delivery.debugInfo()).toMatchObject({
      lastHandledSeq: 0,
      lastReceivedSeq: 0,
      queueSize: 0,
    });
  });

  it("returns a typed timeout when the injected handler deadline expires", async () => {
    let deadline: (() => void) | undefined;
    const outcomes: string[] = [];
    const timedOut = createDeferred<void>();
    const delivery = new SerialEventDelivery<TestEvent>({
      clock: {
        clearTimeout: () => undefined,
        setTimeout: (callback, delayMs) => {
          expect(delayMs).toBe(25);
          deadline = callback;
          return "deadline-1";
        },
      },
      handlerTimeoutMs: 25,
      initialSeq: 0,
      maxQueueSize: 10,
      onOutcome: (outcome) => {
        outcomes.push(outcome.kind);
        if (outcome.kind === "handler-timeout") {
          timedOut.resolve();
        }
      },
    });
    delivery.onEvent(() => new Promise<void>(() => undefined));

    delivery.enqueueEvent({ eventId: "evt_1", seq: 1 });
    await Promise.resolve();
    deadline?.();
    await timedOut.promise;

    expect(outcomes).toEqual(["handler-timeout"]);
    expect(delivery.debugInfo()).toMatchObject({
      lastHandledSeq: 0,
      lastReceivedSeq: 1,
      queueSize: 1,
    });
  });

  it("rejects an event that would exceed the queue limit", async () => {
    const firstHandler = createDeferred<void>();
    const outcomes: string[] = [];
    const delivery = new SerialEventDelivery<TestEvent>({
      handlerTimeoutMs: 1_000,
      initialSeq: 0,
      maxQueueSize: 1,
      onOutcome: (outcome) => {
        outcomes.push(outcome.kind);
      },
    });
    delivery.onEvent(async () => {
      await firstHandler.promise;
    });

    delivery.enqueueEvent({ eventId: "evt_1", seq: 1 });
    await Promise.resolve();
    delivery.enqueueEvent({ eventId: "evt_2", seq: 2 });

    expect(outcomes).toEqual(["delivery-queue-overflow"]);
    expect(delivery.debugInfo()).toMatchObject({
      lastHandledSeq: 0,
      lastReceivedSeq: 1,
      queueSize: 1,
    });

    firstHandler.resolve();
  });

  it("exposes readonly queue and active-delivery diagnostics", async () => {
    const handler = createDeferred<void>();
    const handled = createDeferred<void>();
    const delivery = new SerialEventDelivery<TestEvent>({
      handlerTimeoutMs: 1_000,
      initialSeq: 0,
      maxQueueSize: 10,
      onOutcome: (outcome) => {
        if (outcome.kind === "event-handled") {
          handled.resolve();
        }
      },
    });
    delivery.onEvent(async () => {
      await handler.promise;
    });

    delivery.enqueueEvent({ eventId: "evt_1", seq: 1 });
    await Promise.resolve();
    await Promise.resolve();

    expect(delivery.debugInfo()).toEqual({
      activeDeliverySeq: 1,
      draining: true,
      halted: false,
      handlerCount: 1,
      lastHandledSeq: 0,
      lastReceivedSeq: 1,
      queueSize: 1,
    });

    handler.resolve();
    await handled.promise;

    expect(delivery.debugInfo()).toEqual({
      activeDeliverySeq: null,
      draining: false,
      halted: false,
      handlerCount: 1,
      lastHandledSeq: 1,
      lastReceivedSeq: 1,
      queueSize: 0,
    });
  });

  it("exposes an awaitable boundary for the active delivery settlement", async () => {
    const handler = createDeferred<void>();
    const delivery = new SerialEventDelivery<TestEvent>({
      handlerTimeoutMs: 1_000,
      initialSeq: 0,
      maxQueueSize: 10,
      onOutcome: () => undefined,
    });
    delivery.onEvent(async () => {
      await handler.promise;
    });
    delivery.enqueueEvent({ eventId: "evt_1", seq: 1 });
    await Promise.resolve();
    let settled = false;
    const settlement = delivery.waitForSettlement().then(() => {
      settled = true;
    });

    await Promise.resolve();
    expect(settled).toBe(false);
    handler.resolve();
    await settlement;

    expect(settled).toBe(true);
    expect(delivery.debugInfo().lastHandledSeq).toBe(1);
  });

  it("stops admission and discards work queued behind the active delivery", async () => {
    const handler = createDeferred<void>();
    const delivered: number[] = [];
    const delivery = new SerialEventDelivery<TestEvent>({
      handlerTimeoutMs: 1_000,
      initialSeq: 0,
      maxQueueSize: 10,
      onOutcome: () => undefined,
    });
    delivery.onEvent(async (event) => {
      delivered.push(event.seq);
      if (event.seq === 1) {
        await handler.promise;
      }
    });
    delivery.enqueueEvent({ eventId: "evt_1", seq: 1 });
    delivery.enqueueEvent({ eventId: "evt_2", seq: 2 });
    await Promise.resolve();

    delivery.stop();
    delivery.enqueueEvent({ eventId: "evt_3", seq: 3 });
    handler.resolve();
    await delivery.waitForSettlement();

    expect(delivered).toEqual([1]);
    expect(delivery.debugInfo()).toMatchObject({
      halted: true,
      lastHandledSeq: 1,
      lastReceivedSeq: 2,
      queueSize: 0,
    });
  });
});

interface Deferred<TValue> {
  readonly promise: Promise<TValue>;
  readonly resolve: (value: TValue | PromiseLike<TValue>) => void;
}

/** Creates a promise whose settlement is controlled by the test. */
function createDeferred<TValue>(): Deferred<TValue> {
  let resolve: Deferred<TValue>["resolve"] | undefined;
  const promise = new Promise<TValue>((promiseResolve) => {
    resolve = promiseResolve;
  });
  if (!resolve) {
    throw new Error("Deferred resolver was not initialized");
  }
  return { promise, resolve };
}
