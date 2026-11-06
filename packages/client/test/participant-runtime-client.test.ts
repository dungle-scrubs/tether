import { EventEmitter } from "node:events";

import { Effect, Fiber } from "effect";
import { afterEach, describe, expect, it, vi } from "vitest";
import WebSocket from "ws";
import {
  buildParticipantRuntimeStreamUrl,
  ParticipantRuntimeClient,
  type ParticipantRuntimeClientConfig,
  ParticipantRuntimeClientConfigurationError,
  type ParticipantRuntimeCommandError,
  type ParticipantRuntimeCommandTimeoutError,
  ParticipantRuntimeCursorPersistError,
  type ParticipantRuntimeCursorStore,
  ParticipantRuntimeShutdownError,
  type ParticipantRuntimeWebSocketFactory,
  type ParticipantTaskExecutor,
  ParticipantTaskExecutionError,
  type RunParticipantRuntimeHooks,
  resolveCommandTimeoutMs,
  resolveResumeSeq,
  resolveServiceAuthToken,
  runParticipantRuntime,
  type SessionEvent,
  TaskCancellationRegistry,
  type TaskRecord,
  taskFromClaimableEvent,
} from "../src/index.js";
import { ParticipantCursorWriter } from "../src/participant-cursor-writer.js";

type RunTaskClaimFlowInput = Parameters<ParticipantRuntimeClient["runTaskClaimFlow"]>[0];

type PrivateReconnectRuntimeClient = ParticipantRuntimeClient & {
  readonly reconnectFailureCount: number;
  readonly reconnectPromise: Promise<void> | null;
  readonly reconnectSuccessCount: number;
  requestReconnect(initialDelayMs?: number | null): Promise<void>;
  readonly stopped: boolean;
  reconnectWithBackoff(initialDelayMs?: number | null): Promise<void>;
};

type PrivateCommandRuntimeClient = ParticipantRuntimeClient & {
  readonly commandTimeoutMs: number;
  readonly pendingCommands: Map<
    string,
    {
      readonly op: string;
      readonly reject: (error: Error) => void;
      readonly requestId: string;
      readonly resolve: (result: unknown) => void;
      readonly taskId?: string;
      readonly timeout: ReturnType<typeof setTimeout>;
    }
  >;
  buildCommandRequest(
    buildCommand: (requestId: string) => Record<string, unknown>,
  ): Effect.Effect<unknown, Error>;
  handleMessage(data: string): void;
  rejectPendingCommands(error: Error): void;
  sendCommand(buildCommand: (requestId: string) => Record<string, unknown>): Promise<unknown>;
};

type PrivateReplayRuntimeClient = ParticipantRuntimeClient & {
  readonly currentControlEpoch: number | null;
  handleMessage(data: string): void;
};

type PrivateCursorRuntimeClient = ParticipantRuntimeClient & {
  readonly currentControlEpoch: number | null;
  handleMessage(data: string): void;
  readonly recentEvents: readonly SessionEvent[];
  replaceDelivery(afterSeq: number): void;
  resolveInitialResumeSeq(): Promise<number>;
};

interface CursorFixture {
  readonly client: PrivateCursorRuntimeClient;
  readonly errors: readonly Error[];
  readonly writes: readonly number[];
}

interface CancellationFixture {
  readonly abortActive: () => void;
  readonly cancellation: RunTaskClaimFlowInput["cancellation"];
  readonly signal: AbortSignal;
  readonly setCancelled: () => void;
}

interface RuntimeClientFixture {
  readonly actions: string[];
  readonly client: ParticipantRuntimeClient;
  readonly failures: readonly Record<string, unknown>[];
  readonly diagnostics: readonly string[];
  readonly setClaimedTask: (task: TaskRecord | null) => void;
  readonly setRefreshTaskClaim: (refreshTaskClaim: () => Promise<TaskRecord | null>) => void;
}

interface TaskLoopFixture {
  readonly actions: string[];
  readonly beginReplacementBeforeClose: () => void;
  readonly client: ParticipantRuntimeClient;
  readonly closeSupersededTransport: () => void;
  readonly closeTransport: () => void;
  readonly completeReplay: () => void;
  readonly emit: (event: SessionEvent) => void;
  readonly failReplay: (error: Error) => void;
  readonly reconnectAttempts: () => number;
}

interface CommandFixture {
  readonly client: PrivateCommandRuntimeClient;
  readonly sentMessages: readonly Record<string, unknown>[];
}

interface RunParticipantRuntimeFixture {
  readonly appendedEventIds: readonly string[];
  readonly client: ParticipantRuntimeClient;
  readonly emitError: (error: Error) => void;
}

interface RunParticipantRuntimeForTestOptions {
  readonly hooks?: RunParticipantRuntimeHooks;
  readonly onCloseAndWait?: () => void | Promise<void>;
  readonly onOpen?: () => void | Promise<void>;
  readonly onRunClaimableTasks?: () => void | Promise<void>;
  readonly onWaitForReplayComplete?: () => void | Promise<void>;
}

const baseConfig: ParticipantRuntimeClientConfig = {
  afterSeq: 0,
  capabilities: { workKinds: ["text"] },
  displayName: "Runtime Client",
  instanceId: "inst_runtime_client",
  participantId: "part_runtime_client",
  runtimeKind: "generic_agent",
  serviceUrl: "https://tether.test",
  sessionId: "sess_runtime_client",
};

/** Creates a task executor that can trigger an optional side effect before completing. */
function createExecutor(beforeReturn: () => void = () => undefined): ParticipantTaskExecutor {
  return async () => {
    beforeReturn();
    return { result: { ok: true } };
  };
}

/** Runs the high-level participant helper against the current connect mock. */
async function runParticipantRuntimeForTest(
  options: RunParticipantRuntimeForTestOptions,
): Promise<void> {
  await runParticipantRuntime({
    afterSeq: 0,
    capabilities: { workKinds: ["text"] },
    claimRefreshMs: 1_000,
    displayName: "Runtime",
    executor: createExecutor(),
    hooks: options.hooks,
    instanceId: "inst_runtime",
    once: true,
    participantId: "part_runtime",
    runtimeKind: "generic_agent",
    serviceUrl: "http://tether.test",
    sessionId: "sess_runtime",
    shouldClaimTask: () => true,
    workKinds: ["text"],
  });
}

/** Creates a minimal ParticipantRuntimeClient double for runner lifecycle tests. */
function createRunParticipantRuntimeFixture(
  options: RunParticipantRuntimeForTestOptions = {},
): RunParticipantRuntimeFixture {
  const errorHandlers = new Set<(error: Error) => void>();
  const appendedEventIds: string[] = [];
  const client = {
    appendEvent: async (event: { readonly eventId?: string }) => {
      if (event.eventId) {
        appendedEventIds.push(event.eventId);
      }
    },
    closeAndWait: async () => {
      await options.onCloseAndWait?.();
    },
    debugInfo: () => ({
      connectCount: 1,
      eventBacklogSize: 0,
      eventHandlerCount: 0,
      lastObservedSeq: 0,
      moduleName: "ParticipantRuntimeClient",
      pendingCommandCount: 0,
      recentBoundaryEvents: [],
      reconnectFailureCount: 2,
      reconnectSuccessCount: 0,
      socketReadyState: WebSocket.OPEN,
      stopped: false,
    }),
    onError: (handler: (error: Error) => void) => {
      errorHandlers.add(handler);
      return () => {
        errorHandlers.delete(handler);
      };
    },
    open: async () => {
      await options.onOpen?.();
      return client as ParticipantRuntimeClient;
    },
    runClaimableTasks: async (taskOptions: { readonly replayBarrier?: () => Promise<void> }) => {
      await options.onWaitForReplayComplete?.();
      await taskOptions.replayBarrier?.();
      await options.onRunClaimableTasks?.();
    },
  } satisfies Partial<ParticipantRuntimeClient>;
  return {
    appendedEventIds,
    client: client as ParticipantRuntimeClient,
    emitError: (error) => {
      for (const handler of errorHandlers) {
        handler(error);
      }
    },
  };
}

const baseTask: TaskRecord = {
  cancelledAt: null,
  claimExpiredAt: null,
  claimExpiredBy: null,
  claimExpiresAt: null,
  claimId: "claim_runtime_client",
  claimedAt: null,
  claimedBy: null,
  completedAt: null,
  createdAt: "2026-06-05T00:00:00.000Z",
  failedAt: null,
  failure: null,
  input: null,
  kind: "text",
  objective: "Summarize the session",
  releasedAt: null,
  releasedBy: null,
  result: null,
  sessionId: "sess_runtime_client",
  taskId: "task_runtime_client",
};

const otherTask: TaskRecord = {
  ...baseTask,
  objective: "Handle a second task",
  taskId: "task_runtime_client_other",
};

/** Builds a private ParticipantRuntimeClient test double for task-flow tests. */
function createRuntimeClientFixture(
  options: {
    readonly afterAppend?: (appendCount: number) => void;
    readonly afterClaim?: () => void;
    readonly appendEvent?: () => Promise<void>;
    readonly claimTask?: () => Promise<TaskRecord | null>;
    readonly completeTask?: () => Promise<void>;
    readonly controlEpoch?: number;
    readonly lastHandledSeq?: number;
    readonly recentEvents?: readonly SessionEvent[];
  } = {},
): RuntimeClientFixture {
  let appendCount = 0;
  let claimedTask: TaskRecord | null = {
    ...baseTask,
    claimExpiresAt: "2099-06-05T00:00:30.000Z",
  };
  /** Supplies the current server claim during refresh, replaceable by each test. */
  let refreshTaskClaim = async (): Promise<TaskRecord | null> => baseTask;
  const actions: string[] = [];
  const diagnostics: string[] = [];
  const failures: Record<string, unknown>[] = [];
  const client = Object.assign(Object.create(ParticipantRuntimeClient.prototype), {
    appendEvent: async () => {
      appendCount += 1;
      actions.push("append");
      options.afterAppend?.(appendCount);
      await options.appendEvent?.();
    },
    claimTask: async () => {
      actions.push("claim");
      options.afterClaim?.();
      if (options.claimTask) {
        return options.claimTask();
      }
      return claimedTask;
    },
    completeTask: async () => {
      actions.push("complete");
      await options.completeTask?.();
    },
    config: baseConfig,
    currentControlEpoch: options.controlEpoch ?? null,
    failTask: async (_taskId: string, failure: Record<string, unknown>) => {
      actions.push("fail");
      failures.push(failure);
    },
    observability: {
      debug: (_boundary: string, message: string) => {
        diagnostics.push(message);
      },
      traceBoundary: async <TValue>(
        _operation: string,
        _input: Record<string, unknown>,
        action: () => Promise<TValue>,
      ) => action(),
    },
    lastHandledSeq: options.lastHandledSeq ?? Number.MAX_SAFE_INTEGER,
    recentEvents: [...(options.recentEvents ?? [])],
    refreshTaskClaim: async () => {
      return refreshTaskClaim();
    },
  }) as unknown as ParticipantRuntimeClient;

  return {
    actions,
    client,
    diagnostics,
    failures,
    setClaimedTask: (task) => {
      claimedTask = task;
    },
    setRefreshTaskClaim: (nextRefreshTaskClaim) => {
      refreshTaskClaim = nextRefreshTaskClaim;
    },
  };
}

/** Creates a mutable cancellation context matching the runtime flow contract. */
function createCancellationFixture(
  options: { readonly cancelled?: boolean } = {},
): CancellationFixture {
  let cancelled = options.cancelled ?? false;
  const controller = new AbortController();
  /** Changes the cancellation flag and aborts the matching executor signal. */
  const setCancelled = (): void => {
    cancelled = true;
    controller.abort();
  };
  return {
    abortActive: setCancelled,
    cancellation: {
      abortActive: setCancelled,
      isCancelled: () => cancelled,
      signal: controller.signal,
    },
    setCancelled,
    signal: controller.signal,
  };
}

/** Keeps task execution active until the task cancellation signal is aborted. */
async function waitForAbortExecutor(context: { readonly signal: AbortSignal }): Promise<{
  readonly result: Record<string, unknown>;
}> {
  if (!context.signal.aborted) {
    await new Promise<void>((resolve) => {
      context.signal.addEventListener("abort", () => resolve(), { once: true });
    });
  }
  return { result: { ok: false } };
}

/** Builds a claimable task-created session event fixture. */
function createTaskCreatedEvent(task: TaskRecord, seq: number): SessionEvent {
  return {
    createdAt: "2026-06-05T00:00:00.000Z",
    eventId: `evt_task_created_${seq}`,
    payload: { task },
    producerId: "tether",
    seq,
    sessionId: task.sessionId,
    type: "task.created",
  };
}

/** Builds an approval event used to guard awaited private projection commits. */
function createApprovalRecordedEvent(seq: number): SessionEvent {
  return {
    createdAt: "2026-06-05T00:00:00.000Z",
    eventId: `evt_approval_recorded_${seq}`,
    payload: { approvalId: `approval_${seq}` },
    producerId: "tether",
    seq,
    sessionId: baseTask.sessionId,
    type: "approval.recorded",
  };
}

/** Builds a claimable task-released session event fixture. */
function createTaskReleasedEvent(task: TaskRecord, seq: number): SessionEvent {
  return {
    createdAt: "2026-06-05T00:00:00.000Z",
    eventId: `evt_task_released_${seq}`,
    payload: { participantId: "part_runtime_client", task },
    producerId: "tether",
    seq,
    sessionId: task.sessionId,
    type: "task.released",
  };
}

interface Deferred<TValue> {
  readonly promise: Promise<TValue>;
  readonly resolve: (value?: TValue | PromiseLike<TValue>) => void;
}

interface RejectableDeferred<TValue> extends Deferred<TValue> {
  readonly reject: (error: Error) => void;
}

/** Creates a manually settled promise that can model transport failure. */
function createRejectableDeferred<TValue>(): RejectableDeferred<TValue> {
  /** Captures the promise reject callback during construction. */
  let reject: (error: Error) => void = () => undefined;
  /** Captures the promise resolve callback during construction. */
  let resolve: (value: TValue | PromiseLike<TValue>) => void = () => undefined;
  const promise = new Promise<TValue>((promiseResolve, promiseReject) => {
    reject = promiseReject;
    resolve = promiseResolve;
  });
  return { promise, reject, resolve };
}

/** Creates a manually controlled promise for async loop tests. */
function createDeferred<TValue>(): Deferred<TValue> {
  /** Captures the promise resolve callback during construction. */
  let resolve: (value: TValue | PromiseLike<TValue>) => void = () => undefined;
  const promise = new Promise<TValue>((promiseResolve) => {
    resolve = promiseResolve;
  });
  return { promise, resolve };
}

/** Builds a task-loop client fixture with controllable replay and close promises. */
function createTaskLoopFixture(
  options: { readonly runTaskClaimFlow?: (input: RunTaskClaimFlowInput) => Promise<void> } = {},
): TaskLoopFixture {
  const actions: string[] = [];
  const handlers = new Set<(event: SessionEvent) => void>();
  let replay = createRejectableDeferred<void>();
  replay.promise.catch(() => undefined);
  let close = createDeferred<void>();
  let supersededClose: ReturnType<typeof createDeferred<void>> | null = null;
  let replacementInProgress = false;
  let reconnectAttempts = 0;
  const client = Object.assign(Object.create(ParticipantRuntimeClient.prototype), {
    close: () => {
      client.stopped = true;
      actions.push("close");
    },
    connectionGeneration: 1,
    onEvent: (handler: (event: SessionEvent) => void) => {
      handlers.add(handler);
      return () => {
        handlers.delete(handler);
        actions.push("unsubscribe");
      };
    },
    pausedReason: null,
    reconnectWithBackoff: async () => {
      reconnectAttempts += 1;
      actions.push("reconnect");
      if (replacementInProgress) {
        replacementInProgress = false;
        return;
      }
      client.connectionGeneration += 1;
      replay = createRejectableDeferred<void>();
      replay.promise.catch(() => undefined);
      close = createDeferred<void>();
    },
    runTaskClaimFlow: async (input: RunTaskClaimFlowInput) => {
      if (options.runTaskClaimFlow) {
        await options.runTaskClaimFlow(input);
        return;
      }
      actions.push(`task:${input.task.taskId}`);
    },
    waitForClose: async () => close.promise,
    waitForReplayComplete: async () => replay.promise,
    stopped: false,
  }) as unknown as ParticipantRuntimeClient & {
    connectionGeneration: number;
    stopped: boolean;
  };

  return {
    actions,
    beginReplacementBeforeClose: () => {
      supersededClose = close;
      replacementInProgress = true;
      client.connectionGeneration += 1;
      replay = createRejectableDeferred<void>();
      replay.promise.catch(() => undefined);
      close = createDeferred<void>();
    },
    client,
    closeSupersededTransport: () => supersededClose?.resolve(),
    closeTransport: () => close.resolve(),
    completeReplay: () => replay.resolve(),
    emit: (event) => {
      for (const handler of handlers) {
        handler(event);
      }
    },
    failReplay: (error) => replay.reject(error),
    reconnectAttempts: () => reconnectAttempts,
  };
}

/** Builds a durable-cursor fixture around private client persistence methods. */
function createCursorFixture(
  options: {
    readonly afterSeq?: number;
    readonly cursorStore?: ParticipantRuntimeCursorStore;
    readonly eventCount?: number;
    readonly intervalMs?: number;
    readonly stored?: number | null;
    readonly withHandler?: boolean;
    readonly withStore?: boolean;
  } = {},
): CursorFixture {
  const writes: number[] = [];
  const errors: Error[] = [];
  const afterSeq = options.afterSeq ?? 0;
  const withStore = options.withStore ?? true;
  const withHandler = options.withHandler ?? true;
  const replay = createRejectableDeferred<void>();
  replay.promise.catch(() => undefined);
  const cursorStore =
    options.cursorStore ??
    (withStore
      ? ({
          read: () => options.stored ?? null,
          write: (seq: number) => {
            writes.push(seq);
          },
        } satisfies ParticipantRuntimeCursorStore)
      : undefined);
  const eventHandlers = new Set<(event: SessionEvent) => void>();
  if (withHandler) {
    eventHandlers.add(() => undefined);
  }
  const cursorWriter = new ParticipantCursorWriter({
    acknowledgedSeq: afterSeq,
    onError: (error) => errors.push(error),
    retryAttempts: 1,
    retryBaseDelayMs: 100,
    retryMaxDelayMs: 2_000,
    ...(cursorStore === undefined ? {} : { store: cursorStore }),
    writeTimeoutMs: 5_000,
  });
  const client = Object.assign(Object.create(ParticipantRuntimeClient.prototype), {
    config: { ...baseConfig, afterSeq, cursorStore },
    connectionGeneration: 0,
    currentControlEpoch: null,
    cursorPersistEventCount: options.eventCount ?? 50,
    cursorPersistIntervalMs: options.intervalMs ?? 1_000,
    cursorPersistTimer: null,
    cursorWriter,
    deliveryUnsubscribers: new Map(),
    deliveryFailureCount: 0,
    deliveryRecoveryAttempts: 0,
    deliveryRecoverySeq: null,
    errorBacklog: [],
    errorHandlers: new Set<(error: Error) => void>([(error) => errors.push(error)]),
    eventBacklog: [],
    eventHandlers,
    eventsSinceCursorPersist: 0,
    lastHandledSeq: afterSeq,
    lastPersistedSeq: afterSeq,
    handlerTimeoutMs: 30_000,
    maxDeliveryQueueBytes: 16 * 1024 * 1024,
    maxDeliveryQueueSize: 2_000,
    maxRecoveryAttempts: 5,
    observability: {
      debug: () => undefined,
      debugInfo: () => ({}),
    },
    pendingCommands: new Map(),
    pausedReason: null,
    reconnectFailureCount: 0,
    reconnectPromise: null,
    reconnectSuccessCount: 0,
    recoveryCount: 0,
    rejectReplayComplete: replay.reject,
    replayComplete: replay.promise,
    replayCompleteSettled: false,
    recentEvents: [],
    resolveReplayComplete: replay.resolve,
    socket: null,
    stopped: true,
  }) as PrivateCursorRuntimeClient;
  client.replaceDelivery(afterSeq);
  return { client, errors, writes };
}

/** Feeds one observed event envelope through the private message handler. */
function emitCursorEvent(client: PrivateCursorRuntimeClient, seq: number): void {
  client.handleMessage(
    JSON.stringify({ event: createTaskCreatedEvent(baseTask, seq), op: "event" }),
  );
}

/** Builds a command-correlation fixture around private client methods. */
function createCommandFixture(
  options: { readonly commandTimeoutMs?: number; readonly send?: (encoded: string) => void } = {},
): CommandFixture {
  const sentMessages: Record<string, unknown>[] = [];
  const client = Object.assign(Object.create(ParticipantRuntimeClient.prototype), {
    commandTimeoutMs: options.commandTimeoutMs ?? 15_000,
    config: { ...baseConfig, commandTimeoutMs: options.commandTimeoutMs ?? 15_000 },
    cursorWriter: {
      debugInfo: () => ({
        acknowledgedSeq: 0,
        failureCount: 0,
        inFlightSeq: null,
        pendingSeq: null,
      }),
    },
    delivery: {
      debugInfo: () => ({
        activeDeliverySeq: null,
        draining: false,
        halted: false,
        handlerCount: 0,
        lastHandledSeq: 0,
        lastReceivedSeq: 0,
        queueSize: 0,
      }),
    },
    eventBacklog: [],
    eventHandlers: new Set<(event: SessionEvent) => void>(),
    lastObservedSeq: 0,
    observability: {
      debugInfo: () => ({}),
    },
    pendingCommands: new Map(),
    reconnectFailureCount: 0,
    reconnectSuccessCount: 0,
    requireOpenSocket: () => ({
      send: (encoded: string) => {
        if (options.send) {
          options.send(encoded);
          return;
        }
        sentMessages.push(JSON.parse(encoded) as Record<string, unknown>);
      },
    }),
    resolveReplayComplete: () => undefined,
    socket: null,
    stopped: false,
  }) as PrivateCommandRuntimeClient;
  return { client, sentMessages };
}

/** Lets queued Promise continuations settle without depending on timer state. */
async function flushMicrotasks(iterations = 50): Promise<void> {
  for (let index = 0; index < iterations; index += 1) {
    await Promise.resolve();
  }
}

/** Waits for async state changes without relying on real or fake timer mode. */
async function waitForMicrotasks(predicate: () => boolean, iterations = 100): Promise<void> {
  for (let index = 0; index < iterations; index += 1) {
    if (predicate()) {
      return;
    }
    await Promise.resolve();
  }
  throw new Error("Timed out waiting for microtask condition");
}

/** Reads a generated WebSocket request id from a sent command fixture. */
function readRequestId(message: Record<string, unknown> | undefined): string {
  const requestId = message?.requestId;
  if (typeof requestId !== "string") {
    throw new Error("Expected command request id");
  }
  return requestId;
}

/** In-memory participant WebSocket with controllable server frames and closure. */
class ParticipantFakeWebSocket extends EventEmitter {
  deferClose = false;
  readyState = WebSocket.CONNECTING;
  readonly sent: string[] = [];

  /** Opens asynchronously so callers can attach transport listeners first. */
  constructor(
    readonly url: string,
    openingError?: Error,
  ) {
    super();
    queueMicrotask(() => {
      if (openingError) {
        this.readyState = WebSocket.CLOSED;
        this.emit("error", openingError);
        this.emit("close");
        return;
      }
      this.readyState = WebSocket.OPEN;
      this.emit("open");
    });
  }

  /** Supports deferred closure for generation-replacement race tests. */
  close(): void {
    if (this.deferClose) {
      this.readyState = WebSocket.CLOSING;
      return;
    }
    this.emitClose();
  }

  /** Delivers the server-side close notification. */
  emitClose(): void {
    this.readyState = WebSocket.CLOSED;
    this.emit("close");
  }

  /** Delivers the normal replay completion frame. */
  emitReplayComplete(): void {
    this.emit("message", JSON.stringify({ op: "replay.complete" }));
  }

  /** Delivers a canonical event through the real runtime message handler. */
  emitServerEvent(event: SessionEvent): void {
    this.emit("message", JSON.stringify({ event, op: "event" }));
  }

  /** Records outgoing commands and lets the synthetic server reply. */
  send(data: string): void {
    this.sent.push(data);
    this.emit("client-command", data);
  }

  /** Uses the same close notification as an interrupted transport. */
  terminate(): void {
    this.close();
  }
}

/** Creates participant fake sockets while preserving the public factory type. */
function createParticipantWebSocketFactory(
  sockets: ParticipantFakeWebSocket[],
): ParticipantRuntimeWebSocketFactory {
  return (url) => {
    const socket = new ParticipantFakeWebSocket(url);
    sockets.push(socket);
    return socket as unknown as WebSocket;
  };
}

/** Allows already-queued promise continuations to run. */
async function flushPromises(): Promise<void> {
  await Promise.resolve();
}

/** Waits for one synchronous condition. */
async function waitFor(predicate: () => boolean): Promise<void> {
  const startedAt = Date.now();
  while (Date.now() - startedAt < 1_000) {
    if (predicate()) {
      return;
    }
    await new Promise((resolve) => setTimeout(resolve, 1));
  }
  throw new Error("Timed out waiting for condition");
}

describe("ParticipantRuntimeClient auth token", () => {
  it("opens participant transport through an injected WebSocket factory", async () => {
    const sockets: ParticipantFakeWebSocket[] = [];
    const client = await ParticipantRuntimeClient.connect({
      ...baseConfig,
      webSocketFactory: createParticipantWebSocketFactory(sockets),
    });

    expect(sockets).toHaveLength(1);
    expect(new URL(sockets[0]?.url ?? "http://missing").searchParams.get("participantId")).toBe(
      baseConfig.participantId,
    );
    client.close();
  });

  it("attaches an explicit access token to the WebSocket URL", () => {
    const url = new URL(
      buildParticipantRuntimeStreamUrl({
        ...baseConfig,
        authToken: "runtime-token",
      }),
    );

    expect(url.searchParams.get("access_token")).toBe("runtime-token");
  });

  it("resolves service auth token environment names in precedence order", () => {
    expect(
      resolveServiceAuthToken(undefined, {
        SERVICE_AUTH_TOKEN: "service-token",
        TETHER_AUTH_TOKEN: "legacy-token",
      }),
    ).toBe("service-token");
    expect(resolveServiceAuthToken(undefined, { TETHER_AUTH_TOKEN: "legacy-token" })).toBe(
      "legacy-token",
    );
    expect(resolveServiceAuthToken("explicit-token", { SERVICE_AUTH_TOKEN: "service-token" })).toBe(
      "explicit-token",
    );
  });
});

describe("ParticipantRuntimeClient command timeout config", () => {
  it("defaults command response waits to fifteen seconds", () => {
    expect(resolveCommandTimeoutMs(undefined)).toBe(15_000);
  });

  it.each([
    0,
    -1,
    Number.NaN,
    Number.POSITIVE_INFINITY,
  ])("rejects invalid commandTimeoutMs value %s", (commandTimeoutMs) => {
    expect(() => resolveCommandTimeoutMs(commandTimeoutMs)).toThrow(
      ParticipantRuntimeClientConfigurationError,
    );
  });
});

describe("runParticipantRuntime hooks", () => {
  afterEach(() => {
    vi.restoreAllMocks();
  });

  it("registers replay subscribers before opening a non-empty high-level stream", async () => {
    const sockets: ParticipantFakeWebSocket[] = [];
    const actions: string[] = [];
    const runtime = runParticipantRuntime({
      ...baseConfig,
      claimRefreshMs: 1_000,
      executor: createExecutor(),
      hooks: {
        onClientReady: (client) => {
          actions.push("client-ready");
          const unsubscribeFirst = client.onEvent((event) => {
            actions.push(`first:${event.seq}`);
          });
          const unsubscribeSecond = client.onEvent((event) => {
            actions.push(`second:${event.seq}`);
          });
          return () => {
            unsubscribeSecond();
            unsubscribeFirst();
          };
        },
        onReplayComplete: () => {
          actions.push("replay-hook");
        },
      },
      once: true,
      shouldClaimTask: () => false,
      webSocketFactory: (url) => {
        actions.push("socket-open");
        const socket = new ParticipantFakeWebSocket(url);
        sockets.push(socket);
        queueMicrotask(() => {
          socket.emitServerEvent(createTaskCreatedEvent(baseTask, 1));
          socket.emitServerEvent(createTaskCreatedEvent(otherTask, 2));
          socket.emitReplayComplete();
        });
        return socket as unknown as WebSocket;
      },
      workKinds: ["text"],
    });

    try {
      await flushMicrotasks();
      expect(actions.slice(0, 2)).toEqual(["client-ready", "socket-open"]);
      await runtime;
      expect(actions).toEqual([
        "client-ready",
        "socket-open",
        "first:1",
        "second:1",
        "first:2",
        "second:2",
        "replay-hook",
      ]);
    } finally {
      sockets[0]?.emitClose();
      await runtime.catch(() => undefined);
    }
  });

  it("uses the same pre-open and post-replay hook order for empty replay", async () => {
    const actions: string[] = [];
    await runParticipantRuntime({
      ...baseConfig,
      claimRefreshMs: 1_000,
      executor: createExecutor(),
      hooks: {
        onClientReady: () => {
          actions.push("client-ready");
        },
        onReplayComplete: () => {
          actions.push("replay-hook");
        },
      },
      once: true,
      webSocketFactory: (url) => {
        actions.push("socket-open");
        const socket = new ParticipantFakeWebSocket(url);
        queueMicrotask(() => socket.emitReplayComplete());
        return socket as unknown as WebSocket;
      },
      workKinds: ["text"],
    });

    expect(actions).toEqual(["client-ready", "socket-open", "replay-hook"]);
  });

  it("installs a structured stderr error handler by default", async () => {
    const stderrWrite = vi.spyOn(process.stderr, "write").mockImplementation(() => true);
    let runtime: RunParticipantRuntimeFixture;
    runtime = createRunParticipantRuntimeFixture({
      onRunClaimableTasks: () => runtime.emitError(new Error("stream failed")),
    });
    vi.spyOn(ParticipantRuntimeClient, "create").mockResolvedValue(runtime.client);

    await runParticipantRuntimeForTest({});

    expect(stderrWrite).toHaveBeenCalledWith(
      expect.stringContaining('"type":"participant_runtime.error"'),
    );
    expect(stderrWrite).toHaveBeenCalledWith(expect.stringContaining('"sessionId":"sess_runtime"'));
    expect(stderrWrite).toHaveBeenCalledWith(
      expect.stringContaining('"participantId":"part_runtime"'),
    );
    expect(stderrWrite).toHaveBeenCalledWith(
      expect.stringContaining('"runtimeKind":"generic_agent"'),
    );
    expect(stderrWrite).toHaveBeenCalledWith(expect.stringContaining('"errorName":"Error"'));
    expect(stderrWrite).toHaveBeenCalledWith(
      expect.stringContaining('"errorMessage":"stream failed"'),
    );
  });

  it("uses a supplied error hook and suppresses the default handler", async () => {
    const handled: string[] = [];
    const stderrWrite = vi.spyOn(process.stderr, "write").mockImplementation(() => true);
    let runtime: RunParticipantRuntimeFixture;
    runtime = createRunParticipantRuntimeFixture({
      onRunClaimableTasks: () => runtime.emitError(new Error("custom path")),
    });
    vi.spyOn(ParticipantRuntimeClient, "create").mockResolvedValue(runtime.client);

    await runParticipantRuntimeForTest({
      hooks: {
        onError: (error) => handled.push(error.message),
      },
    });

    expect(handled).toEqual(["custom path"]);
    expect(stderrWrite).not.toHaveBeenCalled();
  });

  it("allows callers to explicitly disable default error handling", async () => {
    const stderrWrite = vi.spyOn(process.stderr, "write").mockImplementation(() => true);
    let runtime: RunParticipantRuntimeFixture;
    runtime = createRunParticipantRuntimeFixture({
      onRunClaimableTasks: () => runtime.emitError(new Error("muted")),
    });
    vi.spyOn(ParticipantRuntimeClient, "create").mockResolvedValue(runtime.client);

    await runParticipantRuntimeForTest({ hooks: { onError: null } });

    expect(stderrWrite).not.toHaveBeenCalled();
  });

  it("waits for replay before running replay hooks and the claim loop", async () => {
    const actions: string[] = [];
    const runtime = createRunParticipantRuntimeFixture({
      onRunClaimableTasks: () => actions.push("claim-loop"),
      onWaitForReplayComplete: () => actions.push("replay"),
    });
    vi.spyOn(ParticipantRuntimeClient, "create").mockResolvedValue(runtime.client);

    await runParticipantRuntimeForTest({
      hooks: {
        onReplayComplete: async (client) => {
          actions.push("hook");
          await client.appendEvent({
            eventId: "evt_status",
            payload: {},
            producerId: "part_runtime",
            sessionId: "sess_runtime",
            type: "agent.output",
          });
        },
      },
    });

    expect(actions).toEqual(["replay", "hook", "claim-loop"]);
    expect(runtime.appendedEventIds).toEqual(["evt_status"]);
  });

  it("runs replay hook cleanup after the claim loop exits", async () => {
    const actions: string[] = [];
    const runtime = createRunParticipantRuntimeFixture({
      onRunClaimableTasks: () => actions.push("claim-loop"),
    });
    vi.spyOn(ParticipantRuntimeClient, "create").mockResolvedValue(runtime.client);

    await runParticipantRuntimeForTest({
      hooks: {
        onReplayComplete: () => () => {
          actions.push("cleanup");
        },
      },
    });

    expect(actions).toEqual(["claim-loop", "cleanup"]);
  });

  it("composes lifecycle cleanup in reverse acquisition order", async () => {
    const actions: string[] = [];
    const runtime = createRunParticipantRuntimeFixture();
    vi.spyOn(ParticipantRuntimeClient, "create").mockResolvedValue(runtime.client);

    await runParticipantRuntimeForTest({
      hooks: {
        onClientReady: () => {
          actions.push("client-ready");
          return async () => {
            await Promise.resolve();
            actions.push("client-cleanup");
          };
        },
        onReplayComplete: () => {
          actions.push("replay-ready");
          return () => {
            actions.push("replay-cleanup");
          };
        },
      },
    });

    expect(actions).toEqual(["client-ready", "replay-ready", "replay-cleanup", "client-cleanup"]);
  });

  it("does not open a socket when onClientReady fails", async () => {
    let socketCount = 0;

    await expect(
      runParticipantRuntime({
        ...baseConfig,
        claimRefreshMs: 1_000,
        executor: createExecutor(),
        hooks: {
          onClientReady: () => {
            throw new Error("client setup failed");
          },
        },
        once: true,
        webSocketFactory: (url) => {
          socketCount += 1;
          return new ParticipantFakeWebSocket(url) as unknown as WebSocket;
        },
        workKinds: ["text"],
      }),
    ).rejects.toThrow("client setup failed");
    expect(socketCount).toBe(0);
  });

  it("releases already-acquired runner subscriptions when onClientReady fails", async () => {
    const handledErrors: string[] = [];
    const runtime = createRunParticipantRuntimeFixture();
    vi.spyOn(ParticipantRuntimeClient, "create").mockResolvedValue(runtime.client);

    await expect(
      runParticipantRuntimeForTest({
        hooks: {
          onClientReady: () => {
            throw new Error("client setup failed");
          },
          onError: (error) => handledErrors.push(error.message),
        },
      }),
    ).rejects.toThrow("client setup failed");
    runtime.emitError(new Error("after shutdown"));

    expect(handledErrors).toEqual([]);
  });

  it("does not skip graceful closure when lifecycle cleanup fails", async () => {
    const actions: string[] = [];
    const runtime = createRunParticipantRuntimeFixture({
      onCloseAndWait: () => actions.push("close-and-wait"),
    });
    vi.spyOn(ParticipantRuntimeClient, "create").mockResolvedValue(runtime.client);

    await expect(
      runParticipantRuntimeForTest({
        hooks: {
          onClientReady: () => () => {
            actions.push("cleanup");
            throw new Error("cleanup failed");
          },
        },
      }),
    ).rejects.toThrow("cleanup failed");

    expect(actions).toEqual(["cleanup", "close-and-wait"]);
  });

  it("prevents buffered task dispatch when onReplayComplete fails", async () => {
    const sockets: ParticipantFakeWebSocket[] = [];
    const actions: string[] = [];
    let executorCalls = 0;
    const runtime = runParticipantRuntime({
      ...baseConfig,
      claimRefreshMs: 1_000,
      executor: async () => {
        executorCalls += 1;
        return { result: {} };
      },
      hooks: {
        onClientReady: () => () => {
          actions.push("client-cleanup");
        },
        onReplayComplete: () => {
          actions.push("replay-hook");
          throw new Error("replay setup failed");
        },
      },
      once: true,
      shouldClaimTask: () => true,
      webSocketFactory: (url) => {
        const socket = new ParticipantFakeWebSocket(url);
        sockets.push(socket);
        queueMicrotask(() => {
          socket.emitServerEvent(createTaskCreatedEvent(baseTask, 1));
          socket.emitReplayComplete();
        });
        return socket as unknown as WebSocket;
      },
      workKinds: ["text"],
    });

    await expect(runtime).rejects.toThrow("replay setup failed");
    expect(executorCalls).toBe(0);
    expect(actions).toEqual(["replay-hook", "client-cleanup"]);
    expect(sockets[0]?.readyState).toBe(WebSocket.CLOSED);
  });

  it("waits for replayed once-mode task settlement before graceful shutdown", async () => {
    const sockets: ParticipantFakeWebSocket[] = [];
    const taskStarted = createDeferred<void>();
    const taskFinished = createDeferred<void>();
    const runtime = runParticipantRuntime({
      ...baseConfig,
      claimRefreshMs: 1_000,
      executor: createExecutor(),
      hooks: {
        onClientReady: (client) => {
          vi.spyOn(client, "runTaskClaimFlow").mockImplementation(async () => {
            taskStarted.resolve();
            await taskFinished.promise;
          });
        },
      },
      once: true,
      shouldClaimTask: () => true,
      webSocketFactory: (url) => {
        const socket = new ParticipantFakeWebSocket(url);
        sockets.push(socket);
        queueMicrotask(() => {
          socket.emitServerEvent(createTaskCreatedEvent(baseTask, 1));
          socket.emitReplayComplete();
        });
        return socket as unknown as WebSocket;
      },
      workKinds: ["text"],
    });
    await taskStarted.promise;
    let runtimeSettled = false;
    void runtime.then(() => {
      runtimeSettled = true;
    });
    await flushMicrotasks();
    expect(runtimeSettled).toBe(false);

    taskFinished.resolve();
    await runtime;

    expect(sockets[0]?.readyState).toBe(WebSocket.CLOSED);
  });
});

describe("ParticipantRuntimeClient.runTaskClaimFlow", () => {
  afterEach(() => {
    vi.useRealTimers();
  });

  it("filters executor recent events against handled progress", async () => {
    const recentEvents = [1, 2, 3].map((seq) => createTaskCreatedEvent(baseTask, seq));
    const runtime = createRuntimeClientFixture({ lastHandledSeq: 2, recentEvents });
    let executorSeqs: readonly number[] = [];

    await runtime.client.runTaskClaimFlow({
      cancellation: createCancellationFixture().cancellation,
      claimRefreshMs: 1_000,
      executor: async (context) => {
        executorSeqs = context.recentEvents.map((event) => event.seq);
        return { result: { ok: true } };
      },
      task: baseTask,
    });

    expect(executorSeqs).toEqual([1]);
  });

  it("passes the connection control epoch to the claimed task executor", async () => {
    const runtime = createRuntimeClientFixture({ controlEpoch: 7 });
    let executorControlEpoch: number | undefined;

    await runtime.client.runTaskClaimFlow({
      cancellation: createCancellationFixture().cancellation,
      claimRefreshMs: 1_000,
      executor: async (context) => {
        executorControlEpoch = context.controlEpoch;
        return { result: { ok: true } };
      },
      task: baseTask,
    });

    expect(executorControlEpoch).toBe(7);
  });

  it("does not claim work that was cancelled before claim", async () => {
    const runtime = createRuntimeClientFixture();
    const cancellation = createCancellationFixture({ cancelled: true });

    await runtime.client.runTaskClaimFlow({
      cancellation: cancellation.cancellation,
      claimRefreshMs: 1_000,
      executor: createExecutor(),
      task: baseTask,
    });

    expect(runtime.actions).toEqual([]);
  });

  it("does not publish progress when cancellation arrives after claim", async () => {
    const runtime = createRuntimeClientFixture({
      afterClaim: () => cancellation.setCancelled(),
    });
    const cancellation = createCancellationFixture();

    await runtime.client.runTaskClaimFlow({
      cancellation: cancellation.cancellation,
      claimRefreshMs: 1_000,
      executor: createExecutor(),
      task: baseTask,
    });

    expect(runtime.actions).toEqual(["claim"]);
  });

  it("does not complete when cancellation arrives after executor", async () => {
    const runtime = createRuntimeClientFixture();
    const cancellation = createCancellationFixture();

    await runtime.client.runTaskClaimFlow({
      cancellation: cancellation.cancellation,
      claimRefreshMs: 1_000,
      executor: createExecutor(() => cancellation.setCancelled()),
      task: baseTask,
    });

    expect(runtime.actions).toEqual(["claim", "append"]);
  });

  it("does not complete when cancellation arrives after output", async () => {
    const runtime = createRuntimeClientFixture({
      afterAppend: (count) => {
        if (count === 2) {
          cancellation.setCancelled();
        }
      },
    });
    const cancellation = createCancellationFixture();

    await runtime.client.runTaskClaimFlow({
      cancellation: cancellation.cancellation,
      claimRefreshMs: 1_000,
      executor: async () => ({ output: "Task output", result: { ok: true } }),
      task: baseTask,
    });

    expect(runtime.actions).toEqual(["claim", "append", "append"]);
  });

  it("does not fail or execute work when claim transport rejects", async () => {
    const runtime = createRuntimeClientFixture({
      claimTask: async () => {
        throw new Error("Socket closed during claim");
      },
    });
    const executor = vi.fn(createExecutor());

    await runtime.client.runTaskClaimFlow({
      cancellation: createCancellationFixture().cancellation,
      claimRefreshMs: 1_000,
      executor,
      task: baseTask,
    });

    expect(executor).not.toHaveBeenCalled();
    expect(runtime.actions).toEqual(["claim"]);
    expect(runtime.failures).toEqual([]);
    expect(runtime.diagnostics).toContain("task.claim_transport_unknown");
  });

  it("persists bounded structured executor failure metadata", async () => {
    const runtime = createRuntimeClientFixture();

    await runtime.client.runTaskClaimFlow({
      cancellation: createCancellationFixture().cancellation,
      claimRefreshMs: 1_000,
      executor: async () => {
        throw new ParticipantTaskExecutionError("Summary generation failed", {
          attempt: 2,
          code: "poison_range",
          retryable: false,
        });
      },
      task: baseTask,
    });

    expect(runtime.failures).toEqual([{ attempt: 2, code: "poison_range", retryable: false }]);
  });

  it("does not start a refresh loop or leave stale cancellation state when claim transport rejects", async () => {
    vi.useFakeTimers();
    const runtime = createRuntimeClientFixture({
      claimTask: async () => {
        throw new Error("Command timed out during claim");
      },
    });
    let refreshCount = 0;
    runtime.setRefreshTaskClaim(async () => {
      refreshCount += 1;
      return baseTask;
    });

    await runtime.client.runTaskClaimFlow({
      cancellation: createCancellationFixture().cancellation,
      claimRefreshMs: 10,
      executor: createExecutor(),
      task: baseTask,
    });
    await vi.advanceTimersByTimeAsync(50);

    expect(refreshCount).toBe(0);
    expect(runtime.actions).toEqual(["claim"]);
    expect(runtime.failures).toEqual([]);
    expect(runtime.diagnostics).toContain("task.claim_transport_unknown");
  });

  it("does not fail or execute work when initial progress publish rejects", async () => {
    const runtime = createRuntimeClientFixture({
      appendEvent: async () => {
        throw new Error("Socket closed during setup");
      },
    });
    const executor = vi.fn(createExecutor());

    await runtime.client.runTaskClaimFlow({
      cancellation: createCancellationFixture().cancellation,
      claimRefreshMs: 1_000,
      executor,
      task: baseTask,
    });

    expect(executor).not.toHaveBeenCalled();
    expect(runtime.actions).toEqual(["claim", "append"]);
    expect(runtime.failures).toEqual([]);
    expect(runtime.diagnostics).toContain("task.setup_failed_before_executor");
  });

  it("does not fail a task when completion transport rejects after executor success", async () => {
    const runtime = createRuntimeClientFixture({
      completeTask: async () => {
        throw new Error("Socket closed during completion");
      },
    });

    await runtime.client.runTaskClaimFlow({
      cancellation: createCancellationFixture().cancellation,
      claimRefreshMs: 1_000,
      executor: async () => ({ output: "Task output", result: { ok: true } }),
      task: baseTask,
    });

    expect(runtime.actions).toEqual(["claim", "append", "append", "complete"]);
    expect(runtime.failures).toEqual([]);
    expect(runtime.diagnostics).toContain("task.completion_transport_failed");
  });

  it("does not fail the task when an executor error races with cancellation", async () => {
    const runtime = createRuntimeClientFixture();
    const cancellation = createCancellationFixture();

    await runtime.client.runTaskClaimFlow({
      cancellation: cancellation.cancellation,
      claimRefreshMs: 1_000,
      executor: async () => {
        cancellation.setCancelled();
        throw new Error("Executor failed after cancellation");
      },
      task: baseTask,
    });

    expect(runtime.actions).toEqual(["claim", "append"]);
  });

  it("stops refreshing the task claim after task completion", async () => {
    vi.useFakeTimers();
    const runtime = createRuntimeClientFixture();
    let refreshCount = 0;
    runtime.setRefreshTaskClaim(async () => {
      refreshCount += 1;
      runtime.actions.push("refresh");
      return baseTask;
    });

    await runtime.client.runTaskClaimFlow({
      cancellation: createCancellationFixture().cancellation,
      claimRefreshMs: 10,
      executor: createExecutor(),
      task: baseTask,
    });
    await vi.advanceTimersByTimeAsync(50);

    expect(refreshCount).toBe(0);
    expect(runtime.actions).toEqual(["claim", "append", "complete"]);
  });

  it("stops refreshing the task claim after task failure", async () => {
    vi.useFakeTimers();
    const runtime = createRuntimeClientFixture();
    let refreshCount = 0;
    runtime.setRefreshTaskClaim(async () => {
      refreshCount += 1;
      runtime.actions.push("refresh");
      return baseTask;
    });

    await runtime.client.runTaskClaimFlow({
      cancellation: createCancellationFixture().cancellation,
      claimRefreshMs: 10,
      executor: async () => {
        throw new Error("Executor failed");
      },
      task: baseTask,
    });
    await vi.advanceTimersByTimeAsync(50);

    expect(refreshCount).toBe(0);
    expect(runtime.actions).toEqual(["claim", "append", "fail"]);
  });

  it("preserves executor failure messages when failing a task", async () => {
    const runtime = createRuntimeClientFixture();

    await runtime.client.runTaskClaimFlow({
      cancellation: createCancellationFixture().cancellation,
      claimRefreshMs: 1_000,
      executor: async () => {
        throw new Error("Episode resolver request timed out");
      },
      task: baseTask,
    });

    expect(runtime.actions).toEqual(["claim", "append", "fail"]);
    expect(runtime.failures).toEqual([{ error: "Episode resolver request timed out" }]);
  });

  it("aborts active work when a task claim refresh is rejected", async () => {
    vi.useFakeTimers();
    const runtime = createRuntimeClientFixture();
    const cancellation = createCancellationFixture();
    runtime.setRefreshTaskClaim(async () => {
      runtime.actions.push("refresh");
      return null;
    });

    const flow = runtime.client.runTaskClaimFlow({
      cancellation: cancellation.cancellation,
      claimRefreshMs: 10,
      executor: waitForAbortExecutor,
      task: baseTask,
    });
    await vi.advanceTimersByTimeAsync(10);
    await flow;

    expect(cancellation.signal.aborted).toBe(true);
    expect(runtime.actions).toEqual(["claim", "append", "refresh"]);
  });

  it("keeps active work running after the first task claim refresh transport failure", async () => {
    vi.useFakeTimers();
    const runtime = createRuntimeClientFixture();
    const cancellation = createCancellationFixture();
    let refreshCount = 0;
    runtime.setRefreshTaskClaim(async () => {
      refreshCount += 1;
      runtime.actions.push("refresh");
      if (refreshCount === 1) {
        throw new Error("Refresh failed");
      }
      return null;
    });

    const flow = runtime.client.runTaskClaimFlow({
      cancellation: cancellation.cancellation,
      claimRefreshMs: 10,
      executor: waitForAbortExecutor,
      task: baseTask,
    });
    await vi.advanceTimersByTimeAsync(10);

    expect(cancellation.signal.aborted).toBe(false);
    expect(runtime.actions).toEqual(["claim", "append", "refresh"]);
    await vi.advanceTimersByTimeAsync(10);
    await flow;

    expect(cancellation.signal.aborted).toBe(true);
    expect(runtime.actions).toEqual(["claim", "append", "refresh", "refresh"]);
  });

  it("resets consecutive refresh transport failures after a successful refresh", async () => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date("2026-06-05T00:00:00.000Z"));
    const runtime = createRuntimeClientFixture();
    const cancellation = createCancellationFixture();
    runtime.setClaimedTask({ ...baseTask, claimExpiresAt: "2026-06-05T00:00:01.000Z" });
    const outcomes: readonly ("fail" | "success" | "lost")[] = [
      "fail",
      "success",
      "fail",
      "fail",
      "lost",
    ];
    let refreshCount = 0;
    runtime.setRefreshTaskClaim(async () => {
      runtime.actions.push("refresh");
      const outcome = outcomes[refreshCount] ?? "lost";
      refreshCount += 1;
      if (outcome === "fail") {
        throw new Error("Refresh failed");
      }
      return outcome === "success"
        ? { ...baseTask, claimExpiresAt: "2026-06-05T00:01:00.000Z" }
        : null;
    });

    const flow = runtime.client.runTaskClaimFlow({
      cancellation: cancellation.cancellation,
      claimRefreshMs: 10,
      executor: waitForAbortExecutor,
      task: baseTask,
    });

    await vi.advanceTimersByTimeAsync(40);
    expect(cancellation.signal.aborted).toBe(false);
    await vi.advanceTimersByTimeAsync(10);
    await flow;

    expect(cancellation.signal.aborted).toBe(true);
    expect(runtime.actions).toEqual([
      "claim",
      "append",
      "refresh",
      "refresh",
      "refresh",
      "refresh",
      "refresh",
    ]);
  });

  it("aborts active work after three consecutive task claim refresh transport failures", async () => {
    vi.useFakeTimers();
    const runtime = createRuntimeClientFixture();
    const cancellation = createCancellationFixture();
    runtime.setRefreshTaskClaim(async () => {
      runtime.actions.push("refresh");
      throw new Error("Refresh failed");
    });

    const flow = runtime.client.runTaskClaimFlow({
      cancellation: cancellation.cancellation,
      claimRefreshMs: 10,
      executor: waitForAbortExecutor,
      task: baseTask,
    });
    await vi.advanceTimersByTimeAsync(30);
    await flow;

    expect(cancellation.signal.aborted).toBe(true);
    expect(runtime.actions).toEqual(["claim", "append", "refresh", "refresh", "refresh"]);
  });

  it("aborts active work when the local claim lease deadline has passed", async () => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date("2026-06-05T00:00:05.000Z"));
    const runtime = createRuntimeClientFixture();
    const cancellation = createCancellationFixture();
    runtime.setClaimedTask({ ...baseTask, claimExpiresAt: "2026-06-05T00:00:04.000Z" });
    runtime.setRefreshTaskClaim(async () => {
      runtime.actions.push("refresh");
      throw new Error("Refresh failed");
    });

    const flow = runtime.client.runTaskClaimFlow({
      cancellation: cancellation.cancellation,
      claimRefreshMs: 10,
      executor: waitForAbortExecutor,
      task: baseTask,
    });
    await vi.advanceTimersByTimeAsync(10);
    await flow;

    expect(cancellation.signal.aborted).toBe(true);
    expect(runtime.actions).toEqual(["claim", "append", "refresh"]);
  });
});

describe("ParticipantRuntimeClient.runClaimableTasks", () => {
  it("extracts released task events as claimable tasks", () => {
    const event = createTaskReleasedEvent(baseTask, 1);

    expect(taskFromClaimableEvent(event)?.taskId).toBe("task_runtime_client");
  });

  it("passes scheduled task identity to the claimability callback", async () => {
    const fixture = createTaskLoopFixture();
    const scheduledTask: TaskRecord = {
      ...baseTask,
      schedule: {
        identityVersion: 2,
        scheduleWindow: {
          algorithmVersion: 1,
          endMs: 1_700_002_800_000,
          intervalMs: 3_600_000,
          startMs: 1_699_999_200_000,
        },
        scopeKey: "scope_01JEMAIL",
      },
    };
    let receivedSchedule: TaskRecord["schedule"];
    const loop = fixture.client.runClaimableTasks({
      claimRefreshMs: 1_000,
      executor: createExecutor(),
      once: true,
      shouldClaimTask: (task) => {
        receivedSchedule = task.schedule;
        return false;
      },
    });

    fixture.emit(createTaskCreatedEvent(scheduledTask, 1));
    fixture.completeReplay();
    await loop;

    expect(receivedSchedule).toEqual(scheduledTask.schedule);
    expect(fixture.actions).toEqual(["close", "unsubscribe"]);
  });

  it("buffers replayed claimable tasks until replay completes in once mode", async () => {
    const fixture = createTaskLoopFixture();
    const loop = fixture.client.runClaimableTasks({
      claimRefreshMs: 1_000,
      executor: createExecutor(),
      once: true,
      shouldClaimTask: () => true,
    });

    fixture.emit(createTaskCreatedEvent(baseTask, 1));
    await flushPromises();

    expect(fixture.actions).toEqual([]);
    fixture.completeReplay();
    await loop;

    expect(fixture.actions).toEqual(["task:task_runtime_client", "close", "unsubscribe"]);
  });

  it("holds replayed claimable tasks behind the asynchronous post-replay barrier", async () => {
    const fixture = createTaskLoopFixture();
    const barrier = createDeferred<void>();
    const loop = fixture.client.runClaimableTasks({
      claimRefreshMs: 1_000,
      executor: createExecutor(),
      once: true,
      replayBarrier: async () => barrier.promise,
      shouldClaimTask: () => true,
    });

    fixture.emit(createTaskCreatedEvent(baseTask, 1));
    fixture.completeReplay();
    await flushMicrotasks();
    expect(fixture.actions).toEqual([]);

    barrier.resolve();
    await loop;

    expect(fixture.actions).toEqual(["task:task_runtime_client", "close", "unsubscribe"]);
  });

  it("starts long-running task work outside event handler settlement", async () => {
    const sockets: ParticipantFakeWebSocket[] = [];
    const taskStarted = createDeferred<void>();
    const taskFinished = createDeferred<void>();
    const client = await ParticipantRuntimeClient.create({
      ...baseConfig,
      webSocketFactory: createParticipantWebSocketFactory(sockets),
    });
    vi.spyOn(client, "runTaskClaimFlow").mockImplementation(async () => {
      taskStarted.resolve();
      await taskFinished.promise;
    });
    const loop = client.runClaimableTasks({
      claimRefreshMs: 1_000,
      executor: createExecutor(),
      once: true,
      shouldClaimTask: () => true,
    });
    await client.open();
    sockets[0]?.emitServerEvent(createTaskCreatedEvent(baseTask, 1));
    sockets[0]?.emitReplayComplete();
    await taskStarted.promise;

    expect(client.debugInfo()).toMatchObject({
      activeDeliverySeq: null,
      lastHandledSeq: 1,
      lastReceivedSeq: 1,
    });
    let loopSettled = false;
    void loop.then(() => {
      loopSettled = true;
    });
    await flushMicrotasks();
    expect(loopSettled).toBe(false);

    taskFinished.resolve();
    await loop;
    await client.closeAndWait();
  });

  it("processes live claimable tasks after replay completes", async () => {
    const fixture = createTaskLoopFixture();
    const loop = fixture.client.runClaimableTasks({
      claimRefreshMs: 1_000,
      executor: createExecutor(),
      once: false,
      shouldClaimTask: () => true,
    });

    fixture.completeReplay();
    await flushPromises();
    fixture.emit(createTaskCreatedEvent(baseTask, 1));
    await waitFor(() => fixture.actions.includes("task:task_runtime_client"));
    fixture.client.close();
    fixture.closeTransport();
    await loop;

    expect(fixture.actions).toEqual(["task:task_runtime_client", "close", "unsubscribe"]);
  });

  it("processes live released tasks after replay completes", async () => {
    const fixture = createTaskLoopFixture();
    const loop = fixture.client.runClaimableTasks({
      claimRefreshMs: 1_000,
      executor: createExecutor(),
      once: false,
      shouldClaimTask: () => true,
    });

    fixture.completeReplay();
    await flushPromises();
    fixture.emit(createTaskReleasedEvent(baseTask, 1));
    await waitFor(() => fixture.actions.includes("task:task_runtime_client"));
    fixture.client.close();
    fixture.closeTransport();
    await loop;

    expect(fixture.actions).toEqual(["task:task_runtime_client", "close", "unsubscribe"]);
  });

  it("buffers replayed released tasks until replay completes in once mode", async () => {
    const fixture = createTaskLoopFixture();
    const loop = fixture.client.runClaimableTasks({
      claimRefreshMs: 1_000,
      executor: createExecutor(),
      once: true,
      shouldClaimTask: () => true,
    });

    fixture.emit(createTaskReleasedEvent(baseTask, 1));
    await flushPromises();

    expect(fixture.actions).toEqual([]);
    fixture.completeReplay();
    await loop;

    expect(fixture.actions).toEqual(["task:task_runtime_client", "close", "unsubscribe"]);
  });

  it("settles once mode and unsubscribes when claim setup returns without executor work", async () => {
    const fixture = createTaskLoopFixture({
      runTaskClaimFlow: async (input) => {
        fixture.actions.push(`claim-setup:${input.task.taskId}`);
      },
    });
    const loop = fixture.client.runClaimableTasks({
      claimRefreshMs: 1_000,
      executor: createExecutor(),
      once: true,
      shouldClaimTask: () => true,
    });

    fixture.emit(createTaskCreatedEvent(baseTask, 1));
    fixture.completeReplay();
    await loop;

    expect(fixture.actions).toEqual(["claim-setup:task_runtime_client", "close", "unsubscribe"]);
  });

  it("rejects stale terminal released tasks before custom selection", async () => {
    const fixture = createTaskLoopFixture();
    const loop = fixture.client.runClaimableTasks({
      claimRefreshMs: 1_000,
      executor: createExecutor(),
      once: false,
      shouldClaimTask: () => true,
    });

    fixture.completeReplay();
    await flushPromises();
    fixture.emit(
      createTaskReleasedEvent(
        { ...baseTask, completedAt: "2026-06-05T00:02:00.000Z", result: { ok: true } },
        1,
      ),
    );
    await flushPromises();
    fixture.client.close();
    fixture.closeTransport();
    await loop;

    expect(fixture.actions).toEqual(["close", "unsubscribe"]);
  });

  it("reconnects after transport closes and resumes live processing", async () => {
    const fixture = createTaskLoopFixture();
    const loop = fixture.client.runClaimableTasks({
      claimRefreshMs: 1_000,
      executor: createExecutor(),
      once: false,
      shouldClaimTask: () => true,
    });

    fixture.completeReplay();
    await flushPromises();
    fixture.closeTransport();
    await waitFor(() => fixture.reconnectAttempts() === 1);
    fixture.completeReplay();
    await flushPromises();
    fixture.emit(createTaskCreatedEvent(otherTask, 2));
    await waitFor(() => fixture.actions.includes("task:task_runtime_client_other"));
    fixture.client.close();
    fixture.closeTransport();
    await loop;

    expect(fixture.actions).toEqual([
      "reconnect",
      "task:task_runtime_client_other",
      "close",
      "unsubscribe",
    ]);
  });

  it("reconnects when replay rejects before the transport close settles", async () => {
    const fixture = createTaskLoopFixture();
    const loop = fixture.client.runClaimableTasks({
      claimRefreshMs: 1_000,
      executor: createExecutor(),
      once: false,
      shouldClaimTask: () => true,
    });

    fixture.failReplay(new Error("socket failed before replay"));
    await flushMicrotasks();
    expect(fixture.reconnectAttempts()).toBe(0);
    fixture.closeTransport();
    await waitFor(() => fixture.reconnectAttempts() === 1);
    fixture.completeReplay();
    await flushMicrotasks();
    fixture.client.close();
    fixture.closeTransport();
    await loop;

    expect(fixture.actions).toEqual(["reconnect", "close", "unsubscribe"]);
  });

  it("fences a stale replay barrier after transport replacement", async () => {
    const fixture = createTaskLoopFixture();
    const oldBarrier = createDeferred<void>();
    let barrierCalls = 0;
    const loop = fixture.client.runClaimableTasks({
      claimRefreshMs: 1_000,
      executor: createExecutor(),
      once: false,
      replayBarrier: async () => {
        barrierCalls += 1;
        if (barrierCalls === 1) {
          await oldBarrier.promise;
        }
      },
      shouldClaimTask: () => true,
    });

    fixture.emit(createTaskCreatedEvent(baseTask, 1));
    fixture.completeReplay();
    await waitFor(() => barrierCalls === 1);
    fixture.closeTransport();
    await waitFor(() => fixture.reconnectAttempts() === 1);
    fixture.emit(createTaskCreatedEvent(otherTask, 2));
    oldBarrier.resolve();
    await flushMicrotasks();
    expect(fixture.actions).toEqual(["reconnect"]);

    fixture.completeReplay();
    await waitFor(() => fixture.actions.includes("task:task_runtime_client_other"));
    fixture.client.close();
    fixture.closeTransport();
    await loop;

    expect(fixture.actions).toEqual([
      "reconnect",
      "task:task_runtime_client_other",
      "close",
      "unsubscribe",
    ]);
  });

  it("buffers replacement-generation tasks until the replacement replay barrier completes", async () => {
    const fixture = createTaskLoopFixture();
    const replacementBarrier = createDeferred<void>();
    let barrierCalls = 0;
    const loop = fixture.client.runClaimableTasks({
      claimRefreshMs: 1_000,
      executor: createExecutor(),
      once: false,
      replayBarrier: async () => {
        barrierCalls += 1;
        if (barrierCalls === 2) {
          await replacementBarrier.promise;
        }
      },
      shouldClaimTask: () => true,
    });

    fixture.completeReplay();
    await waitFor(() => barrierCalls === 1);
    fixture.beginReplacementBeforeClose();
    fixture.emit(createTaskCreatedEvent(otherTask, 2));
    await flushMicrotasks();

    expect(fixture.actions).toEqual([]);
    fixture.closeSupersededTransport();
    await waitFor(() => fixture.reconnectAttempts() === 1);
    fixture.completeReplay();
    await waitFor(() => barrierCalls === 2);
    expect(fixture.actions).toEqual(["reconnect"]);

    replacementBarrier.resolve();
    await waitFor(() => fixture.actions.includes("task:task_runtime_client_other"));
    fixture.client.close();
    fixture.closeTransport();
    await loop;
  });
});

describe("TaskCancellationRegistry", () => {
  it("bounds cancelled ids for tasks that were never processed", () => {
    const registry = new TaskCancellationRegistry({ maxCancelledTaskIds: 2 });

    registry.cancel("task_1");
    registry.cancel("task_2");
    registry.cancel("task_3");

    expect(registry.isCancelled("task_1")).toBe(false);
    expect(registry.isCancelled("task_2")).toBe(true);
    expect(registry.isCancelled("task_3")).toBe(true);
    expect(registry.debugInfo()).toEqual({
      activeTaskCount: 0,
      cancelledTaskIdCount: 2,
      inactiveCancelledTaskIdCount: 2,
      maxCancelledTaskIds: 2,
    });
  });

  it("keeps active controllers abortable after inactive cancellation memory exceeds the bound", () => {
    const registry = new TaskCancellationRegistry({ maxCancelledTaskIds: 2 });
    const activeSignal = registry.begin("task_active");

    registry.cancel("task_active");
    registry.cancel("task_1");
    registry.cancel("task_2");
    registry.cancel("task_3");

    expect(activeSignal.aborted).toBe(true);
    expect(registry.isCancelled("task_active")).toBe(true);
    expect(registry.debugInfo()).toEqual({
      activeTaskCount: 1,
      cancelledTaskIdCount: 3,
      inactiveCancelledTaskIdCount: 2,
      maxCancelledTaskIds: 2,
    });

    registry.end("task_active");
    expect(registry.isCancelled("task_active")).toBe(false);
  });
});

describe("ParticipantRuntimeClient reconnect scheduling", () => {
  it("coalesces concurrent participant reconnect requests", async () => {
    const reconnect = createDeferred<void>();
    let reconnectCalls = 0;
    const client = Object.assign(Object.create(ParticipantRuntimeClient.prototype), {
      reconnectPromise: null,
      reconnectWithBackoff: async () => {
        reconnectCalls += 1;
        await reconnect.promise;
      },
    }) as PrivateReconnectRuntimeClient;

    const first = client.requestReconnect();
    const second = client.requestReconnect();

    expect(first).toBe(second);
    expect(reconnectCalls).toBe(1);
    reconnect.resolve();
    await first;
    expect(client.reconnectPromise).toBeNull();
  });

  it("uses bounded reconnect backoff and reopens from the last observed event sequence", async () => {
    const openedAfterSeqs: number[] = [];
    const errors: Error[] = [];
    const client = Object.assign(Object.create(ParticipantRuntimeClient.prototype), {
      config: { ...baseConfig, reconnect: { baseDelayMs: 1, maxDelayMs: 2 } },
      errorHandlers: new Set<(error: Error) => void>([(error) => errors.push(error)]),
      lastHandledSeq: 42,
      openTransport: async (afterSeq: number) => {
        openedAfterSeqs.push(afterSeq);
        if (openedAfterSeqs.length < 3) {
          throw new Error(`Reconnect attempt ${openedAfterSeqs.length} failed`);
        }
      },
      pausedReason: null,
      recoveryFailureCount: 0,
      reconnectFailureCount: 0,
      reconnectSuccessCount: 0,
      stopped: false,
    }) as PrivateReconnectRuntimeClient;

    await client.reconnectWithBackoff(0);

    expect(openedAfterSeqs).toEqual([42, 42, 42]);
    expect(errors.map((error) => error.message)).toEqual([
      "Reconnect attempt 1 failed",
      "Reconnect attempt 2 failed",
    ]);
    expect(client.reconnectFailureCount).toBe(2);
    expect(client.reconnectSuccessCount).toBe(1);
  });

  it("recovers failed delivery through one replacement participant socket", async () => {
    const sockets: ParticipantFakeWebSocket[] = [];
    const errors: Error[] = [];
    const delivered: number[] = [];
    const client = await ParticipantRuntimeClient.connect({
      ...baseConfig,
      reconnect: { baseDelayMs: 0, maxDelayMs: 0 },
      webSocketFactory: createParticipantWebSocketFactory(sockets),
    });
    client.onError((error) => {
      errors.push(error);
    });
    client.onEvent((event) => {
      delivered.push(event.seq);
      if (delivered.length === 1) {
        throw new Error("unsafe raw handler detail");
      }
    });
    const firstSocket = sockets[0];
    if (!firstSocket) {
      throw new Error("Missing first participant socket");
    }
    firstSocket.deferClose = true;

    firstSocket.emitServerEvent(createTaskCreatedEvent(baseTask, 1));
    await waitFor(() => errors.length === 1);
    await new Promise((resolve) => setTimeout(resolve, 20));
    expect(sockets).toHaveLength(1);
    firstSocket.emitClose();
    await waitFor(() => sockets.length === 2);
    const replacementSocket = sockets[1];
    if (!replacementSocket) {
      throw new Error("Missing replacement participant socket");
    }
    const replacementReplay = client.waitForReplayComplete();
    replacementSocket.emitServerEvent(createTaskCreatedEvent(baseTask, 1));
    replacementSocket.emitReplayComplete();
    await replacementReplay;

    expect(delivered).toEqual([1, 1]);
    expect(errors).toHaveLength(1);
    expect(errors[0]).toMatchObject({
      attempt: 1,
      eventId: "evt_task_created_1",
      reason: "event_handler_failed",
      seq: 1,
    });
    expect(errors[0]?.message).not.toContain("unsafe raw handler detail");
    expect(client.debugInfo()).toMatchObject({
      connectionGeneration: 2,
      lastHandledSeq: 1,
      pausedReason: null,
      recoveryCount: 1,
    });
    expect(sockets).toHaveLength(2);
    client.close();
  });

  it("pauses after five failed deliveries without opening a sixth socket", async () => {
    const sockets: ParticipantFakeWebSocket[] = [];
    const errors: Error[] = [];
    const client = await ParticipantRuntimeClient.connect({
      ...baseConfig,
      reconnect: { baseDelayMs: 0, maxDelayMs: 0 },
      webSocketFactory: createParticipantWebSocketFactory(sockets),
    });
    client.onError((error) => {
      errors.push(error);
    });
    client.onEvent(() => {
      throw new Error("poison event payload must stay private");
    });

    for (let attempt = 1; attempt <= 5; attempt += 1) {
      const socket = sockets[attempt - 1];
      if (!socket) {
        throw new Error(`Missing participant socket for attempt ${attempt}`);
      }
      socket.emitServerEvent(createTaskCreatedEvent(baseTask, 1));
      if (attempt < 5) {
        await waitFor(() => sockets.length === attempt + 1);
      } else {
        await waitFor(() => client.debugInfo().pausedReason !== null);
      }
    }

    expect(sockets).toHaveLength(5);
    expect(errors).toHaveLength(6);
    expect(errors.slice(0, 5).map((error) => Reflect.get(error, "attempt"))).toEqual([
      1, 2, 3, 4, 5,
    ]);
    expect(errors.at(-1)).toMatchObject({ reason: "delivery_retry_exhausted" });
    expect(errors.every((error) => !error.message.includes("poison event payload"))).toBe(true);
    expect(client.debugInfo()).toMatchObject({
      eventDeliveryFailureCount: 5,
      pausedReason: "delivery_retry_exhausted",
      recoveryCount: 4,
    });
    client.close();
  });

  it("explicit reconnect clears Paused State after handler remediation", async () => {
    const sockets: ParticipantFakeWebSocket[] = [];
    let failDelivery = true;
    const client = await ParticipantRuntimeClient.connect({
      ...baseConfig,
      eventDelivery: { maxRecoveryAttempts: 1 },
      reconnect: { baseDelayMs: 0, maxDelayMs: 0 },
      webSocketFactory: createParticipantWebSocketFactory(sockets),
    });
    client.onEvent(() => {
      if (failDelivery) {
        throw new Error("handler requires remediation");
      }
    });
    const firstSocket = sockets[0];
    if (!firstSocket) {
      throw new Error("Missing initial participant socket");
    }
    firstSocket.emitServerEvent(createTaskCreatedEvent(baseTask, 1));
    await waitFor(() => client.debugInfo().pausedReason !== null);

    failDelivery = false;
    await client.reconnect();
    const replacementSocket = sockets[1];
    if (!replacementSocket) {
      throw new Error("Missing remediated participant socket");
    }
    const replay = client.waitForReplayComplete();
    replacementSocket.emitServerEvent(createTaskCreatedEvent(baseTask, 1));
    replacementSocket.emitReplayComplete();
    await replay;

    expect(client.debugInfo()).toMatchObject({
      lastHandledSeq: 1,
      pausedReason: null,
    });
    client.close();
  });

  it("defaults the participant handler deadline to 30 seconds", async () => {
    const sockets: ParticipantFakeWebSocket[] = [];
    const errors: Error[] = [];
    const client = await ParticipantRuntimeClient.connect({
      ...baseConfig,
      reconnect: { baseDelayMs: 0, maxDelayMs: 0 },
      webSocketFactory: createParticipantWebSocketFactory(sockets),
    });
    client.onError((error) => {
      errors.push(error);
    });
    client.onEvent(() => new Promise<void>(() => undefined));
    vi.useFakeTimers();
    try {
      const socket = sockets[0];
      if (!socket) {
        throw new Error("Missing participant timeout socket");
      }
      socket.emitServerEvent(createTaskCreatedEvent(baseTask, 1));
      await flushMicrotasks();
      await vi.advanceTimersByTimeAsync(29_999);
      expect(errors).toEqual([]);

      await vi.advanceTimersByTimeAsync(1);
      await waitForMicrotasks(() => errors.length === 1);

      expect(errors[0]).toMatchObject({
        reason: "event_handler_timeout",
        timeoutMs: 30_000,
      });
      // The never-settling handler holds the settlement barrier, so no
      // replacement transport may open behind the timed-out invocation.
      await flushMicrotasks();
      expect(sockets).toHaveLength(1);
    } finally {
      vi.useRealTimers();
      client.close();
    }
  });

  it("holds replay behind the settlement barrier so a timed-out handler never runs concurrently", async () => {
    const sockets: ParticipantFakeWebSocket[] = [];
    const errors: Error[] = [];
    let releaseFirst: (() => void) | undefined;
    const firstBlocked = new Promise<void>((resolve) => {
      releaseFirst = resolve;
    });
    const invocations: number[] = [];
    let inFlight = 0;
    let sawOverlap = false;
    const client = await ParticipantRuntimeClient.connect({
      ...baseConfig,
      eventDelivery: { handlerTimeoutMs: 10 },
      reconnect: { baseDelayMs: 0, maxDelayMs: 0 },
      webSocketFactory: createParticipantWebSocketFactory(sockets),
    });
    client.onError((error) => {
      errors.push(error);
    });
    client.onEvent(async (event) => {
      invocations.push(event.seq);
      inFlight += 1;
      if (inFlight > 1) {
        sawOverlap = true;
      }
      try {
        if (invocations.length === 1) {
          await firstBlocked;
        }
      } finally {
        inFlight -= 1;
      }
    });
    const firstSocket = sockets[0];
    if (!firstSocket) {
      throw new Error("Missing first participant socket");
    }

    firstSocket.emitServerEvent(createTaskCreatedEvent(baseTask, 1));
    await waitFor(() => errors.length === 1);
    expect(errors[0]).toMatchObject({
      reason: "event_handler_timeout",
      timeoutMs: 10,
    });
    // The timed-out invocation is still running, so the settlement barrier
    // must hold the replacement transport back.
    await new Promise((resolve) => setTimeout(resolve, 30));
    expect(sockets).toHaveLength(1);

    releaseFirst?.();
    await waitFor(() => sockets.length === 2);
    const replacementSocket = sockets[1];
    if (!replacementSocket) {
      throw new Error("Missing replacement participant socket");
    }
    replacementSocket.emitServerEvent(createTaskCreatedEvent(baseTask, 1));
    replacementSocket.emitReplayComplete();
    await waitFor(() => invocations.length === 2);

    expect(sawOverlap).toBe(false);
    expect(invocations).toEqual([1, 1]);
    expect(client.debugInfo()).toMatchObject({
      lastHandledSeq: 1,
      pausedReason: null,
    });
    client.close();
  });

  it("honors an explicit participant handler deadline", async () => {
    const sockets: ParticipantFakeWebSocket[] = [];
    const errors: Error[] = [];
    const client = await ParticipantRuntimeClient.connect({
      ...baseConfig,
      eventDelivery: { handlerTimeoutMs: 25, maxRecoveryAttempts: 1 },
      webSocketFactory: createParticipantWebSocketFactory(sockets),
    });
    client.onError((error) => {
      errors.push(error);
    });
    client.onEvent(() => new Promise<void>(() => undefined));
    vi.useFakeTimers();
    try {
      const socket = sockets[0];
      if (!socket) {
        throw new Error("Missing explicit-timeout participant socket");
      }
      socket.emitServerEvent(createTaskCreatedEvent(baseTask, 1));
      await flushMicrotasks();
      await vi.advanceTimersByTimeAsync(24);
      expect(errors).toEqual([]);

      await vi.advanceTimersByTimeAsync(1);
      await waitForMicrotasks(() => client.debugInfo().pausedReason !== null);

      expect(errors[0]).toMatchObject({
        reason: "event_handler_timeout",
        timeoutMs: 25,
      });
      expect(errors.at(-1)).toMatchObject({ reason: "delivery_retry_exhausted" });
    } finally {
      vi.useRealTimers();
      client.close();
    }
  });

  it("defaults the participant delivery queue limit to 2,000 events", async () => {
    const sockets: ParticipantFakeWebSocket[] = [];
    const errors: Error[] = [];
    const client = await ParticipantRuntimeClient.connect({
      ...baseConfig,
      webSocketFactory: createParticipantWebSocketFactory(sockets),
    });
    client.onError((error) => {
      errors.push(error);
    });
    const socket = sockets[0];
    if (!socket) {
      throw new Error("Missing default-queue participant socket");
    }

    for (let seq = 1; seq <= 2_001; seq += 1) {
      socket.emitServerEvent(createTaskCreatedEvent(baseTask, seq));
    }

    expect(errors).toHaveLength(1);
    expect(errors[0]).toMatchObject({
      reason: "delivery_queue_overflow",
      safeDetails: { maxQueueSize: 2_000, observedQueueSize: 2_001 },
    });
    expect(client.debugInfo()).toMatchObject({
      eventBacklogSize: 2_000,
      lastReceivedSeq: 2_000,
      pausedReason: "delivery_queue_overflow",
    });
    client.close();
  });

  it("honors an explicit participant delivery queue limit", async () => {
    const sockets: ParticipantFakeWebSocket[] = [];
    const client = await ParticipantRuntimeClient.connect({
      ...baseConfig,
      eventDelivery: { maxQueueSize: 2 },
      webSocketFactory: createParticipantWebSocketFactory(sockets),
    });
    const socket = sockets[0];
    if (!socket) {
      throw new Error("Missing explicit-queue participant socket");
    }

    socket.emitServerEvent(createTaskCreatedEvent(baseTask, 1));
    socket.emitServerEvent(createTaskCreatedEvent(baseTask, 2));
    socket.emitServerEvent(createTaskCreatedEvent(baseTask, 3));

    expect(client.debugInfo()).toMatchObject({
      eventBacklogSize: 2,
      lastReceivedSeq: 2,
      pausedReason: "delivery_queue_overflow",
    });
    client.close();
  });

  it("fences stale participant frames, replay, errors, closes, and commands", async () => {
    const sockets: ParticipantFakeWebSocket[] = [];
    const delivered: string[] = [];
    let failDelivery = true;
    const client = await ParticipantRuntimeClient.connect({
      ...baseConfig,
      reconnect: { baseDelayMs: 0, maxDelayMs: 0 },
      webSocketFactory: createParticipantWebSocketFactory(sockets),
    });
    client.onEvent((event) => {
      delivered.push(event.eventId);
      if (failDelivery) {
        failDelivery = false;
        throw new Error("replace this generation");
      }
    });
    const firstSocket = sockets[0];
    if (!firstSocket) {
      throw new Error("Missing stale-generation participant socket");
    }
    firstSocket.deferClose = true;
    firstSocket.emitServerEvent(createTaskCreatedEvent(baseTask, 1));
    await new Promise((resolve) => setTimeout(resolve, 20));
    expect(sockets).toHaveLength(1);
    firstSocket.emitClose();
    await waitFor(() => sockets.length === 2);
    const replacementSocket = sockets[1];
    if (!replacementSocket) {
      throw new Error("Missing current-generation participant socket");
    }
    const replay = client.waitForReplayComplete();
    const command = client.claimTask(baseTask.taskId);
    const request = JSON.parse(replacementSocket.sent[0] ?? "{}") as Record<string, unknown>;

    firstSocket.emitServerEvent({
      ...createTaskCreatedEvent(baseTask, 1),
      eventId: "evt_stale_generation",
    });
    firstSocket.emitReplayComplete();
    firstSocket.emit("error", new Error("stale socket failure"));
    replacementSocket.emit(
      "message",
      JSON.stringify({
        command: "task.claim",
        op: "command.result",
        requestId: request.requestId,
        task: baseTask,
      }),
    );
    replacementSocket.emitServerEvent(createTaskCreatedEvent(baseTask, 1));
    replacementSocket.emitReplayComplete();

    await expect(command).resolves.toMatchObject({ taskId: baseTask.taskId });
    await replay;
    expect(delivered).toEqual(["evt_task_created_1", "evt_task_created_1"]);
    expect(client.debugInfo()).toMatchObject({
      connectionGeneration: 2,
      lastHandledSeq: 1,
      pendingCommandCount: 0,
    });
    client.close();
  });

  it("rejects commands interrupted by delivery recovery as outcome unknown", async () => {
    const sockets: ParticipantFakeWebSocket[] = [];
    let command: Promise<TaskRecord | null> | undefined;
    const client = await ParticipantRuntimeClient.connect({
      ...baseConfig,
      reconnect: { baseDelayMs: 0, maxDelayMs: 0 },
      webSocketFactory: createParticipantWebSocketFactory(sockets),
    });
    client.onEvent(() => {
      command = client.claimTask(baseTask.taskId);
      command.catch(() => undefined);
      throw new Error("delivery failed after command send");
    });
    const socket = sockets[0];
    if (!socket) {
      throw new Error("Missing command-ambiguity participant socket");
    }

    socket.emitServerEvent(createTaskCreatedEvent(baseTask, 1));
    await waitFor(() => sockets.length === 2);

    if (!command) {
      throw new Error("Participant handler did not send its command");
    }
    await expect(command).rejects.toMatchObject({
      name: "ParticipantRuntimeCommandOutcomeUnknownError",
      op: "task.claim",
      requestId: expect.stringMatching(/^req_/),
      taskId: baseTask.taskId,
    });
    client.close();
  });

  it("drains participant errors emitted before onError registration", async () => {
    const sockets: ParticipantFakeWebSocket[] = [];
    const client = await ParticipantRuntimeClient.connect({
      ...baseConfig,
      webSocketFactory: createParticipantWebSocketFactory(sockets),
    });
    const socket = sockets[0];
    if (!socket) {
      throw new Error("Missing error-backlog participant socket");
    }
    socket.emitServerEvent(createTaskCreatedEvent(baseTask, 2));

    const errors: Error[] = [];
    client.onError((error) => {
      errors.push(error);
    });

    expect(errors).toHaveLength(1);
    expect(errors[0]).toMatchObject({ reason: "non_contiguous_event" });
    client.close();
  });

  it("pauses replay_window_exceeded without automatic reconnect", async () => {
    const sockets: ParticipantFakeWebSocket[] = [];
    const client = await ParticipantRuntimeClient.connect({
      ...baseConfig,
      reconnect: { baseDelayMs: 0, maxDelayMs: 0 },
      webSocketFactory: createParticipantWebSocketFactory(sockets),
    });
    const socket = sockets[0];
    if (!socket) {
      throw new Error("Missing replay-window participant socket");
    }
    const replay = client.waitForReplayComplete();

    socket.emit(
      "message",
      JSON.stringify({
        error: "unsafe server detail",
        limit: 2_000,
        op: "error",
        reason: "replay_window_exceeded",
        secret: "must-not-cross-client-boundary",
      }),
    );

    await expect(replay).rejects.toMatchObject({
      reason: "replay_window_exceeded",
      safeDetails: { limit: 2_000 },
    });
    await flushMicrotasks();
    expect(sockets).toHaveLength(1);
    expect(client.debugInfo().pausedReason).toBe("replay_window_exceeded");
    client.close();
  });

  it("logs safe structured delivery recovery without raw causes or transport secrets", async () => {
    const sockets: ParticipantFakeWebSocket[] = [];
    const consoleLog = vi.spyOn(console, "log").mockImplementation(() => undefined);
    vi.stubEnv("DEBUG", "1");
    const client = await ParticipantRuntimeClient.connect({
      ...baseConfig,
      authToken: "secret-participant-token",
      reconnect: { baseDelayMs: 0, maxDelayMs: 0 },
      webSocketFactory: createParticipantWebSocketFactory(sockets),
    });
    try {
      client.onEvent(() => {
        throw new Error("raw handler payload secret");
      });
      const socket = sockets[0];
      if (!socket) {
        throw new Error("Missing structured-log participant socket");
      }
      socket.emitServerEvent(createTaskCreatedEvent(baseTask, 1));
      await waitFor(() => sockets.length === 2);

      const logs = consoleLog.mock.calls.map(([entry]) => String(entry)).join("\n");
      expect(logs).toContain("participant_event_delivery.failed");
      expect(logs).toContain("participant_event_delivery.recovering");
      expect(logs).not.toContain("raw handler payload secret");
      expect(logs).not.toContain("secret-participant-token");
      expect(logs).not.toContain(baseTask.objective);
    } finally {
      client.close();
      vi.unstubAllEnvs();
      consoleLog.mockRestore();
    }
  });

  it("uses configured reconnect backoff for delivery recovery", async () => {
    const sockets: ParticipantFakeWebSocket[] = [];
    const random = vi.spyOn(Math, "random").mockReturnValue(1);
    const client = await ParticipantRuntimeClient.connect({
      ...baseConfig,
      reconnect: { baseDelayMs: 10, maxDelayMs: 10 },
      webSocketFactory: createParticipantWebSocketFactory(sockets),
    });
    client.onEvent(() => {
      throw new Error("recover with backoff");
    });
    vi.useFakeTimers();
    try {
      const socket = sockets[0];
      if (!socket) {
        throw new Error("Missing recovery-backoff participant socket");
      }
      socket.emitServerEvent(createTaskCreatedEvent(baseTask, 1));
      await flushMicrotasks();

      await vi.advanceTimersByTimeAsync(9);
      expect(sockets).toHaveLength(1);
      await vi.advanceTimersByTimeAsync(1);
      await waitForMicrotasks(() => sockets.length === 2);
      expect(sockets).toHaveLength(2);
    } finally {
      vi.useRealTimers();
      random.mockRestore();
      client.close();
    }
  });
});

describe("ParticipantRuntimeClient command correlation", () => {
  afterEach(() => {
    vi.useRealTimers();
  });

  it("resolves and removes only the matching pending command", async () => {
    vi.useFakeTimers();
    const fixture = createCommandFixture();
    const first = fixture.client.sendCommand((requestId) => ({
      op: "task.claim",
      requestId,
      taskId: "task_first",
    }));
    const second = fixture.client.sendCommand((requestId) => ({
      op: "task.refresh",
      requestId,
      taskId: "task_second",
    }));
    const firstRequestId = readRequestId(fixture.sentMessages[0]);
    const secondRequestId = readRequestId(fixture.sentMessages[1]);

    fixture.client.handleMessage(
      JSON.stringify({
        command: "task.refresh",
        ok: true,
        op: "command.result",
        requestId: secondRequestId,
      }),
    );

    await expect(second).resolves.toMatchObject({ command: "task.refresh" });
    expect(fixture.client.pendingCommands.has(firstRequestId)).toBe(true);
    expect(fixture.client.pendingCommands.has(secondRequestId)).toBe(false);

    fixture.client.handleMessage(
      JSON.stringify({
        command: "task.claim",
        ok: true,
        op: "command.result",
        requestId: firstRequestId,
      }),
    );
    await expect(first).resolves.toMatchObject({ command: "task.claim" });
    expect(fixture.client.pendingCommands.size).toBe(0);
    await vi.advanceTimersByTimeAsync(15_000);
    expect(fixture.client.debugInfo().pendingCommandCount).toBe(0);
  });

  it("rejects matching pending command errors with command context", async () => {
    vi.useFakeTimers();
    const fixture = createCommandFixture();
    const command = fixture.client.sendCommand((requestId) => ({
      op: "task.claim",
      requestId,
      taskId: "task_first",
    }));
    const requestId = readRequestId(fixture.sentMessages[0]);
    const rejection = expect(command).rejects.toMatchObject({
      message: "Claim failed",
      name: "ParticipantRuntimeCommandError",
      op: "task.claim",
      pendingCommandCount: 0,
      requestId,
      taskId: "task_first",
    } satisfies Partial<ParticipantRuntimeCommandError>);

    fixture.client.handleMessage(
      JSON.stringify({
        command: "task.claim",
        error: "Claim failed",
        op: "error",
        requestId,
        taskId: "task_first",
      }),
    );

    await rejection;
    expect(fixture.client.pendingCommands.size).toBe(0);
    await vi.advanceTimersByTimeAsync(15_000);
    expect(fixture.client.debugInfo().pendingCommandCount).toBe(0);
  });

  it("leaves unmatched command responses pending until timeout", async () => {
    vi.useFakeTimers();
    const fixture = createCommandFixture({ commandTimeoutMs: 25 });
    const command = fixture.client.sendCommand((requestId) => ({
      op: "task.claim",
      requestId,
      taskId: "task_first",
    }));
    const requestId = readRequestId(fixture.sentMessages[0]);

    fixture.client.handleMessage(
      JSON.stringify({
        command: "task.claim",
        ok: true,
        op: "command.result",
        requestId: "req_unmatched",
      }),
    );
    expect(fixture.client.pendingCommands.has(requestId)).toBe(true);
    const rejection = expect(command).rejects.toMatchObject({
      message: "Timed out waiting for task.claim command response",
      name: "ParticipantRuntimeCommandTimeoutError",
      op: "task.claim",
      pendingCommandCount: 0,
      requestId,
      taskId: "task_first",
      timeoutMs: 25,
    } satisfies Partial<ParticipantRuntimeCommandTimeoutError>);
    await vi.advanceTimersByTimeAsync(25);
    await rejection;
    expect(fixture.client.pendingCommands.size).toBe(0);
    expect(fixture.client.debugInfo().pendingCommandCount).toBe(0);
  });

  it("clears all pending command timers on socket close or error cleanup", async () => {
    vi.useFakeTimers();
    const fixture = createCommandFixture({ commandTimeoutMs: 25 });
    const first = fixture.client.sendCommand((requestId) => ({
      op: "task.claim",
      requestId,
      taskId: "task_first",
    }));
    const second = fixture.client.sendCommand((requestId) => ({
      op: "task.refresh",
      requestId,
      taskId: "task_second",
    }));

    fixture.client.rejectPendingCommands(new Error("Socket closed"));
    await expect(first).rejects.toThrow("Socket closed");
    await expect(second).rejects.toThrow("Socket closed");
    expect(fixture.client.pendingCommands.size).toBe(0);
    await vi.advanceTimersByTimeAsync(25);
    expect(fixture.client.debugInfo().pendingCommandCount).toBe(0);
  });

  it("cleans pending command state when socket send fails", async () => {
    vi.useFakeTimers();
    const fixture = createCommandFixture({
      commandTimeoutMs: 25,
      send: () => {
        throw new Error("Socket send failed");
      },
    });

    await expect(
      fixture.client.sendCommand((requestId) => ({
        op: "task.claim",
        requestId,
        taskId: "task_first",
      })),
    ).rejects.toThrow("Socket send failed");
    expect(fixture.client.pendingCommands.size).toBe(0);
    await vi.advanceTimersByTimeAsync(25);
    expect(fixture.client.debugInfo().pendingCommandCount).toBe(0);
  });

  it("cleans pending command state when the waiting effect is interrupted", async () => {
    vi.useFakeTimers();
    const fixture = createCommandFixture({ commandTimeoutMs: 25 });
    const fiber = Effect.runFork(
      fixture.client.buildCommandRequest((requestId) => ({
        op: "task.claim",
        requestId,
        taskId: "task_first",
      })),
    );
    await flushPromises();

    expect(fixture.client.pendingCommands.size).toBe(1);
    await Effect.runPromise(Fiber.interrupt(fiber));
    expect(fixture.client.pendingCommands.size).toBe(0);
    await vi.advanceTimersByTimeAsync(25);
    expect(fixture.client.debugInfo().pendingCommandCount).toBe(0);
  });
});

/** Builds one synthetic history event without task lifecycle semantics. */
function restHistoryEvent(seq: number): SessionEvent {
  return {
    createdAt: "2026-06-05T00:00:00.000Z",
    eventId: `evt_history_${seq}`,
    payload: {},
    producerId: "fixture",
    seq,
    sessionId: baseConfig.sessionId,
    type: "fixture.history",
  };
}

/** Synthetic bounded REST response with no server or calendar dependencies. */
function restHistoryPage(after: number, limit: number, head: number) {
  const count = Math.max(0, Math.min(limit, head - after));
  const events = Array.from({ length: count }, (_, i) => restHistoryEvent(after + i + 1));
  return {
    events,
    pagination: {
      afterSeq: after,
      hasMore: after + count < head,
      limit,
      nextAfterSeq: after + count,
      returned: count,
    },
  };
}

/** Supplies the participant's matching wire replay marker and control epoch. */
function emitFencedReplay(socket: ParticipantFakeWebSocket): void {
  socket.emit(
    "message",
    JSON.stringify({
      op: "replay.complete",
      controlEpoch: 7,
      participantId: baseConfig.participantId,
      instanceId: baseConfig.instanceId,
    }),
  );
}

/** Acts as the synthetic server for the existing fenced claim and completion commands. */
function replyToTaskCommands(socket: ParticipantFakeWebSocket, tasks: readonly TaskRecord[]): void {
  socket.on("client-command", (data: string) => {
    const command = JSON.parse(data) as {
      readonly op: string;
      readonly requestId: string;
      readonly taskId?: string;
    };
    const task = tasks.find((candidate) => candidate.taskId === command.taskId);
    socket.emit(
      "message",
      JSON.stringify({
        op: "command.result",
        command: command.op,
        requestId: command.requestId,
        ...(command.op === "task.claim" || command.op === "task.refresh"
          ? {
              task: task
                ? {
                    ...task,
                    claimId: `claim_${task.taskId}`,
                    claimedBy: baseConfig.participantId,
                    claimExpiresAt: "2099-01-01T00:00:00.000Z",
                  }
                : null,
            }
          : {}),
      }),
    );
  });
}

describe("ParticipantRuntimeClient REST replay", () => {
  const clients: ParticipantRuntimeClient[] = [];

  afterEach(async () => {
    for (const client of clients.splice(0)) {
      await client.closeAndWait();
    }
    vi.unstubAllGlobals();
    vi.unstubAllEnvs();
    vi.restoreAllMocks();
  });

  /** Creates a public runtime client against synthetic HTTP and WebSocket boundaries. */
  async function fixture(
    input: {
      readonly afterSeq?: number;
      readonly head?: number;
      readonly config?: Partial<ParticipantRuntimeClientConfig>;
      readonly page?: (
        after: number,
        limit: number,
        call: number,
        signal: AbortSignal,
        phase: "discovery" | "history",
      ) => unknown | Promise<unknown>;
      readonly tasks?: () => unknown | Promise<unknown>;
    } = {},
  ) {
    const sockets: ParticipantFakeWebSocket[] = [];
    const requests: {
      after: number;
      limit: number;
      signal: AbortSignal;
      phase: "discovery" | "history";
    }[] = [];
    const writes: number[] = [];
    const errors: Error[] = [];
    vi.stubGlobal(
      "fetch",
      vi.fn(async (url: URL, init: RequestInit) => {
        if (url.pathname.endsWith("/tasks")) {
          const body = (await input.tasks?.()) ?? { tasks: [] };
          return body instanceof Response ? body : Response.json(body);
        }
        const after = Number(url.searchParams.get("after"));
        const limit = Number(url.searchParams.get("limit"));
        const signal = init.signal as AbortSignal;
        const phase = sockets.at(-1)?.readyState === WebSocket.OPEN ? "history" : "discovery";
        requests.push({ after, limit, signal, phase });
        const body = input.page
          ? await input.page(after, limit, requests.length, signal, phase)
          : restHistoryPage(after, limit, input.head ?? 0);
        return body instanceof Response ? body : Response.json(body);
      }),
    );
    const client = await ParticipantRuntimeClient.create({
      ...baseConfig,
      afterSeq: input.afterSeq ?? 0,
      authToken: null,
      cursorPersist: { eventCount: 1 },
      cursorStore: {
        read: () => null,
        write: (seq) => {
          writes.push(seq);
        },
      },
      replayMode: "rest",
      webSocketFactory: createParticipantWebSocketFactory(sockets),
      ...input.config,
    } as ParticipantRuntimeClientConfig);
    clients.push(client);
    client.onError((error) => errors.push(error));
    /** Opens REST recovery and supplies matching wire authority before waiting for replay. */
    const open = async () => {
      await client.open();
      const socket = sockets.at(-1);
      if (!socket) throw new Error("Missing REST replay socket");
      return socket;
    };
    return { client, errors, open, requests, sockets, writes };
  }

  it("discovers a witnessed finite head even when every probe sees a growing tail", async () => {
    const f = await fixture({
      page: (after, limit, call) => {
        if (call > 110) throw new Error("Synthetic safe-integer probe ceiling exceeded");
        const head = after >= Number.MAX_SAFE_INTEGER - 2 ? Number.MAX_SAFE_INTEGER : after + 2;
        const page = restHistoryPage(after, limit, head);
        return {
          ...page,
          pagination: {
            ...page.pagination,
            hasMore: page.pagination.nextAfterSeq < Number.MAX_SAFE_INTEGER,
          },
        };
      },
    });
    const socket = await f.open();
    expect(new URL(socket.url).searchParams.get("after")).toBe("9007199254740991");
    expect(f.requests.length).toBeLessThanOrEqual(107);
    expect(f.requests.every(({ after, limit }) => Number.isSafeInteger(after) && limit === 1)).toBe(
      true,
    );
    expect(f.requests.some(({ after }) => after === 9_007_199_254_740_990)).toBe(true);
    expect(f.client.debugInfo().lastHandledSeq).toBe(0);
    expect(f.writes).toEqual([]);
  });

  it("probes the safe-integer boundary without guessing or acknowledging the transport head", async () => {
    const f = await fixture({ afterSeq: 9_007_199_254_740_989, head: 9_007_199_254_740_991 });
    const socket = await f.open();
    expect(new URL(socket.url).searchParams.get("after")).toBe("9007199254740991");
    expect(f.requests.every(({ after, limit }) => Number.isSafeInteger(after) && limit === 1)).toBe(
      true,
    );
    expect(f.client.debugInfo().lastHandledSeq).toBe(9_007_199_254_740_989);
    expect(f.writes).toEqual([]);
  });

  it("reports the admission byte budget for a REST response overflow in errors and diagnostics", async () => {
    const diagnostics: Record<string, unknown>[] = [];
    vi.stubEnv("DEBUG", "true");
    vi.spyOn(console, "log").mockImplementation((encoded: unknown) => {
      if (typeof encoded !== "string") return;
      const entry = JSON.parse(encoded) as {
        readonly message: string;
        readonly data: Record<string, unknown>;
      };
      if (entry.message === "participant_event_delivery.paused") diagnostics.push(entry.data);
    });
    let observedBytes = 0;
    const f = await fixture({
      config: {
        eventDelivery: { maxQueueBytes: 64 },
      },
      page: (after, limit) => {
        const page = restHistoryPage(after, limit, 1);
        const body = {
          ...page,
          events: page.events.map((event) => ({
            ...event,
            payload: { padding: "x".repeat(5_000) },
          })),
        };
        observedBytes = new TextEncoder().encode(JSON.stringify(body)).byteLength;
        return body;
      },
    });
    await expect(f.open()).rejects.toMatchObject({
      reason: "delivery_byte_overflow",
      safeDetails: { maxQueueBytes: 64 },
    });
    expect(f.errors.at(-1)).toMatchObject({
      safeDetails: {
        maxQueueBytes: 64,
        observedQueueBytes: observedBytes,
        maxResponseBytes: 4_160,
      },
    });
    expect(diagnostics).toHaveLength(1);
    expect(diagnostics[0]).toMatchObject({
      maxQueueBytes: 64,
      observedQueueBytes: observedBytes,
      maxResponseBytes: 4_160,
    });
    expect(diagnostics[0]).not.toHaveProperty("limit");
    expect(f.writes).toEqual([]);
  });

  it("halves a valid history page after streamed HTTP byte overflow", async () => {
    const handled: number[] = [];
    let pulls = 0;
    let cancellations = 0;
    const f = await fixture({
      config: { eventDelivery: { maxQueueBytes: 1_200 } },
      page: (after, limit, _call, _signal, phase) => {
        const page = restHistoryPage(after, limit, 20);
        const body = {
          ...page,
          events: page.events.map((event) => ({ ...event, payload: { text: "x".repeat(180) } })),
        };
        if (phase !== "history" || after !== 0 || limit !== 20) return body;
        const bytes = new TextEncoder().encode(JSON.stringify(body));
        let offset = 0;
        return new Response(
          new ReadableStream<Uint8Array>(
            {
              pull: (controller) => {
                pulls += 1;
                const chunk = bytes.subarray(offset, offset + 1_024);
                offset += chunk.length;
                if (chunk.length === 0) controller.close();
                else controller.enqueue(chunk);
              },
              cancel: () => {
                cancellations += 1;
                expect(handled).toEqual([]);
                expect(f.writes).toEqual([]);
              },
            },
            { highWaterMark: 0 },
          ),
        );
      },
    });
    f.client.onEvent((event) => {
      handled.push(event.seq);
    });
    emitFencedReplay(await f.open());
    await f.client.waitForReplayComplete();
    await waitFor(() => f.client.debugInfo().lastPersistedSeq === 20);
    expect(pulls).toBe(6);
    expect(cancellations).toBe(1);
    expect(handled).toEqual([
      1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20,
    ]);
    expect(
      f.requests
        .filter(({ phase, after }) => phase === "history" && after === 0)
        .map(({ limit }) => limit),
    ).toEqual([20, 10, 5, 2]);
    expect(f.sockets).toHaveLength(1);
  });

  it("carries the accepted history limit forward instead of restarting the full page window", async () => {
    const f = await fixture({
      head: 12,
      config: { eventDelivery: { maxQueueBytes: 600 } },
    });
    const handled: number[] = [];
    f.client.onEvent((event) => {
      handled.push(event.seq);
    });
    emitFencedReplay(await f.open());
    await f.client.waitForReplayComplete();
    expect(handled).toEqual([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]);
    expect(
      f.requests
        .filter(({ phase, after }) => phase === "history" && after === 0)
        .map(({ limit }) => limit),
    ).toEqual([12, 6, 3, 1]);
    expect(f.requests.find(({ phase, after }) => phase === "history" && after === 1)?.limit).toBe(
      2,
    );
    expect(
      f.requests
        .filter(({ phase, after }) => phase === "history" && after > 0)
        .map(({ limit }) => limit),
    ).toEqual([2, 2, 2, 2, 2, 1]);
    expect(f.sockets).toHaveLength(1);
    expect(f.client.debugInfo().lastHandledSeq).toBe(12);
  });

  it("halves a history page when live arrivals consume its available item slots", async () => {
    const pageReady = createDeferred<void>();
    const releasePage = createDeferred<void>();
    const f = await fixture({
      config: { eventDelivery: { maxQueueSize: 3 } },
      page: async (after, limit, _call, _signal, phase) => {
        if (phase === "history" && after === 0 && limit === 2) {
          pageReady.resolve();
          await releasePage.promise;
        }
        return restHistoryPage(after, limit, 3);
      },
    });
    const handled: number[] = [];
    f.client.onEvent((event) => {
      handled.push(event.seq);
    });
    const socket = await f.open();
    emitFencedReplay(socket);
    await pageReady.promise;
    socket.emitServerEvent(restHistoryEvent(4));
    releasePage.resolve();
    await f.client.waitForReplayComplete();
    await waitFor(() => f.client.debugInfo().lastPersistedSeq === 4);
    expect(handled).toEqual([1, 2, 3, 4]);
    expect(
      f.requests
        .filter(({ phase }) => phase === "history")
        .slice(0, 2)
        .map(({ after, limit }) => [after, limit]),
    ).toEqual([
      [0, 2],
      [0, 1],
    ]);
    expect(f.sockets).toHaveLength(1);
    expect(f.writes).toEqual([1, 2, 3, 4]);
    expect(f.client.debugInfo().pausedReason).toBeNull();
  });

  it.each([
    "headers",
    "body",
  ] as const)("halves a timed-out history read at the same cursor before reconnecting [%s]", async (phaseOfTimeout) => {
    const f = await fixture({
      head: 4,
      config: { commandTimeoutMs: 5 },
      page: (after, limit, _call, signal, phase) => {
        if (phase === "history" && limit > 1) {
          if (phaseOfTimeout === "body")
            return new Response(new ReadableStream<Uint8Array>({ pull: () => undefined }));
          return new Promise<never>((_resolve, reject) => {
            signal.addEventListener("abort", () => reject(signal.reason), { once: true });
          });
        }
        return restHistoryPage(after, limit, 4);
      },
    });
    const handled: number[] = [];
    f.client.onEvent((event) => {
      handled.push(event.seq);
    });
    emitFencedReplay(await f.open());
    await f.client.waitForReplayComplete();
    expect(handled).toEqual([1, 2, 3, 4]);
    expect(
      f.requests
        .filter(({ phase }) => phase === "history")
        .slice(0, 3)
        .map(({ after, limit }) => [after, limit]),
    ).toEqual([
      [0, 4],
      [0, 2],
      [0, 1],
    ]);
    expect(f.sockets).toHaveLength(1);
    expect(f.writes).toEqual([1, 2, 3, 4]);
  });

  it("resumes page growth after a transient timeout halves one history request", async () => {
    let timedOut = false;
    const f = await fixture({
      head: 20,
      config: { commandTimeoutMs: 5, eventDelivery: { maxQueueSize: 9 } },
      page: (after, limit, _call, signal, phase) => {
        if (phase === "history" && !timedOut) {
          timedOut = true;
          return new Promise<never>((_resolve, reject) => {
            signal.addEventListener("abort", () => reject(signal.reason), { once: true });
          });
        }
        return restHistoryPage(after, limit, 20);
      },
    });
    const handled: number[] = [];
    f.client.onEvent((event) => {
      handled.push(event.seq);
    });
    try {
      emitFencedReplay(await f.open());
      await f.client.waitForReplayComplete();
      expect(
        f.requests
          .filter(({ phase }) => phase === "history")
          .map(({ after, limit }) => [after, limit]),
      ).toEqual([
        [0, 8],
        [0, 4],
        [4, 8],
        [12, 8],
      ]);
      expect(handled).toEqual(Array.from({ length: 20 }, (_, index) => index + 1));
      expect(f.sockets).toHaveLength(1);
      expect(f.client.debugInfo().lastHandledSeq).toBe(20);
    } finally {
      await f.client.closeAndWait();
    }
  });

  it("reconnects a single-row deadline failure after halving without advancing the failed cursor", async () => {
    const f = await fixture({
      config: { commandTimeoutMs: 5, reconnect: { baseDelayMs: 0, maxDelayMs: 0 } },
      page: (after, limit, _call, signal, phase) => {
        if (phase === "history" && f.sockets.length === 1)
          return new Promise<never>((_resolve, reject) => {
            signal.addEventListener("abort", () => reject(signal.reason), { once: true });
          });
        return restHistoryPage(after, limit, 2);
      },
    });
    const handled: number[] = [];
    f.client.onEvent((event) => {
      handled.push(event.seq);
    });
    const oldReplay = f.client.waitForReplayComplete();
    oldReplay.catch(() => undefined);
    emitFencedReplay(await f.open());
    await expect(oldReplay).rejects.toMatchObject({ code: "TRANSPORT" });
    expect(handled).toEqual([]);
    expect(f.writes).toEqual([]);
    expect(
      f.requests
        .filter(({ phase }) => phase === "history")
        .map(({ after, limit }) => [after, limit]),
    ).toEqual([
      [0, 2],
      [0, 1],
    ]);
    await waitFor(() => f.sockets.length === 2);
    const replacement = f.sockets[1];
    if (!replacement) throw new Error("Missing deadline recovery socket");
    emitFencedReplay(replacement);
    await f.client.waitForReplayComplete();
    expect(handled).toEqual([1, 2]);
    expect(f.client.debugInfo()).toMatchObject({ lastHandledSeq: 2, pausedReason: null });
    expect(f.sockets).toHaveLength(2);
  });

  it("retries a byte-heavy history page at the same cursor without partially admitting it", async () => {
    const handled: number[] = [];
    let retried = false;
    const f = await fixture({
      config: { eventDelivery: { maxQueueBytes: 600 } },
      page: (after, limit, _call, _signal, phase) => {
        if (phase === "history" && after === 0 && limit === 1) {
          retried = true;
          expect(handled).toEqual([]);
          expect(f.writes).toEqual([]);
        }
        const page = restHistoryPage(after, limit, 2);
        return {
          ...page,
          events: page.events.map((event) => ({ ...event, payload: { text: "x".repeat(180) } })),
        };
      },
    });
    f.client.onEvent((event) => {
      handled.push(event.seq);
    });
    const socket = await f.open();
    emitFencedReplay(socket);
    await f.client.waitForReplayComplete();
    expect(retried).toBe(true);
    expect(handled).toEqual([1, 2]);
    expect(
      f.requests
        .filter(({ after, phase }) => after === 0 && phase === "history")
        .map(({ limit }) => limit),
    ).toEqual([2, 1]);
    expect(f.client.debugInfo().lastHandledSeq).toBe(2);
  });

  it("keeps a single over-budget event terminal without acknowledging or retrying it", async () => {
    const f = await fixture({
      config: { eventDelivery: { maxQueueBytes: 600 } },
      page: (after, limit) => {
        const page = restHistoryPage(after, limit, 1);
        return {
          ...page,
          events: page.events.map((event) => ({ ...event, payload: { text: "x".repeat(1_000) } })),
        };
      },
    });
    await expect(f.open()).rejects.toMatchObject({ reason: "delivery_byte_overflow" });
    expect(f.requests).toHaveLength(1);
    expect(f.writes).toEqual([]);
  });

  it("rejects a response exceeding its requested row limit instead of adapting around malformed input", async () => {
    const f = await fixture({
      page: (after, limit) => restHistoryPage(after, limit + 1, after + limit + 1),
    });
    await expect(f.open()).rejects.toMatchObject({ reason: "invalid_server_envelope" });
    expect(f.requests).toHaveLength(1);
    expect(f.writes).toEqual([]);
  });

  it("retries a transient failed reconnect discovery through the existing backoff owner", async () => {
    let failed = false;
    const f = await fixture({
      config: { reconnect: { baseDelayMs: 0, maxDelayMs: 0 } },
      page: (after, limit) => {
        if (!failed) {
          failed = true;
          throw new TypeError("synthetic discovery transport failure");
        }
        return restHistoryPage(after, limit, 0);
      },
    });
    await f.client.reconnect();
    expect(f.requests).toHaveLength(2);
    expect(f.sockets).toHaveLength(1);
    expect(f.client.debugInfo()).toMatchObject({
      pausedReason: null,
      reconnectFailureCount: 1,
      reconnectSuccessCount: 1,
      lastHandledSeq: 0,
    });
    expect(f.writes).toEqual([]);
  });

  it("retries after a partial history delivery from only the successful handled cursor", async () => {
    let failed = false;
    const f = await fixture({
      config: { eventDelivery: { maxQueueSize: 2 }, reconnect: { baseDelayMs: 0, maxDelayMs: 0 } },
      page: (after, limit, _call, _signal, phase) => {
        if (phase === "history" && after === 1 && !failed) {
          failed = true;
          throw new TypeError("synthetic second history page transport failure");
        }
        return restHistoryPage(after, limit, 2);
      },
    });
    const handled: number[] = [];
    f.client.onEvent((event) => {
      handled.push(event.seq);
    });
    const socket = await f.open();
    emitFencedReplay(socket);
    await waitFor(() => f.sockets.length === 2);
    const replacement = f.sockets[1];
    if (!replacement) throw new Error("Missing transient history replacement socket");
    expect(f.client.debugInfo().lastHandledSeq).toBe(1);
    emitFencedReplay(replacement);
    await f.client.waitForReplayComplete();
    expect(handled).toEqual([1, 2]);
    expect(
      f.requests.filter(({ phase, after }) => phase === "history" && after === 0),
    ).toHaveLength(1);
    expect(f.client.debugInfo()).toMatchObject({ lastHandledSeq: 2, pausedReason: null });
  });

  it("charges only selected snapshot tasks to the retained-task byte budget", async () => {
    const ignored = {
      ...baseTask,
      taskId: "task_unselected_large",
      input: { padding: "x".repeat(1_200) },
    };
    const f = await fixture({
      config: { eventDelivery: { maxQueueBytes: 800 } },
      tasks: () => ({ tasks: [ignored, baseTask] }),
    });
    const selected: string[] = [];
    const executed: string[] = [];
    const loop = f.client.runClaimableTasks({
      claimRefreshMs: 1_000,
      executor: async ({ task }) => {
        executed.push(task.taskId);
        return { result: {} };
      },
      once: true,
      shouldClaimTask: (task) => {
        selected.push(task.taskId);
        return task.taskId !== ignored.taskId;
      },
    });
    loop.catch(() => undefined);
    const socket = await f.open();
    replyToTaskCommands(socket, [baseTask]);
    emitFencedReplay(socket);
    await loop;
    expect(selected).toEqual(["task_unselected_large", baseTask.taskId]);
    expect(executed).toEqual([baseTask.taskId]);
    expect(f.client.debugInfo().pausedReason).toBeNull();
    expect(f.writes).toEqual([]);
  });

  it("rejects WebSocket once mode when terminal pause wins before the task drain", async () => {
    const f = await fixture({ config: { replayMode: "websocket" } });
    let completed = false;
    const loop = f.client.runClaimableTasks({
      claimRefreshMs: 1_000,
      executor: createExecutor(),
      once: true,
      shouldClaimTask: () => true,
    });
    loop.catch(() => undefined);
    const socket = await f.open();
    replyToTaskCommands(socket, [baseTask]);
    socket.on("client-command", (data: string) => {
      if ((JSON.parse(data) as { readonly op: string }).op !== "task.complete") return;
      completed = true;
      socket.deferClose = true;
      socket.emitServerEvent(restHistoryEvent(3));
    });
    try {
      socket.emitServerEvent(createTaskCreatedEvent(baseTask, 1));
      emitFencedReplay(socket);
      await expect(loop).rejects.toThrow("Participant task loop entered Paused State");
      expect(completed).toBe(true);
      expect(f.client.debugInfo().pausedReason).toBe("non_contiguous_event");
      expect(f.writes).toEqual([1]);
    } finally {
      socket.emitClose();
    }
  });

  it("rejects REST once mode when terminal pause wins before the idle drain", async () => {
    const f = await fixture({ tasks: () => ({ tasks: [baseTask] }) });
    let completed = false;
    const loop = f.client.runClaimableTasks({
      claimRefreshMs: 1_000,
      executor: createExecutor(),
      once: true,
      shouldClaimTask: () => true,
    });
    loop.catch(() => undefined);
    const socket = await f.open();
    replyToTaskCommands(socket, [baseTask]);
    socket.on("client-command", (data: string) => {
      if ((JSON.parse(data) as { readonly op: string }).op !== "task.complete") return;
      completed = true;
      socket.deferClose = true;
      socket.emitServerEvent(restHistoryEvent(2));
    });
    try {
      emitFencedReplay(socket);
      await expect(loop).rejects.toThrow("Participant task loop entered Paused State");
      expect(completed).toBe(true);
      expect(f.client.debugInfo().pausedReason).toBe("non_contiguous_event");
      expect(f.writes).toEqual([]);
    } finally {
      socket.emitClose();
    }
  });

  it("emits a failed REST reconnect open error exactly once", async () => {
    const f = await fixture({
      config: { reconnect: { baseDelayMs: 0, maxDelayMs: 0 } },
      page: (after, limit, call) =>
        call === 1
          ? Response.json({ error: "synthetic temporary service failure" }, { status: 503 })
          : restHistoryPage(after, limit, 0),
    });
    await f.client.reconnect();
    expect(f.sockets).toHaveLength(1);
    const socket = f.sockets[0];
    if (!socket) throw new Error("Missing recovered socket");
    emitFencedReplay(socket);
    await f.client.waitForReplayComplete();
    expect(f.errors).toHaveLength(1);
    expect(f.errors[0]).toMatchObject({
      code: "PERSISTENCE",
      details: { operation: "list-events", status: 503 },
    });
    expect(f.requests).toHaveLength(2);
    expect(f.writes).toEqual([]);
  });

  it("resets recovery delay after a handled cursor advance before another history failure", async () => {
    vi.spyOn(Math, "random").mockReturnValue(1);
    vi.useFakeTimers();
    vi.setSystemTime(0);
    let refusedDiscovery = 0;
    let refusedHistory = false;
    const handled: number[] = [];
    const f = await fixture({
      config: { reconnect: { baseDelayMs: 100, maxDelayMs: 800 } },
      page: (after, limit, _call, _signal, phase) => {
        if (phase === "discovery" && refusedDiscovery < 2) {
          refusedDiscovery += 1;
          return Response.json({ error: "synthetic temporary discovery refusal" }, { status: 503 });
        }
        if (phase === "history" && after === 0) return restHistoryPage(after, 1, 2);
        if (phase === "history" && after === 1 && !refusedHistory) {
          refusedHistory = true;
          return Response.json({ error: "synthetic temporary history refusal" }, { status: 503 });
        }
        return restHistoryPage(after, limit, 2);
      },
    });
    f.client.onEvent((event) => {
      handled.push(event.seq);
    });
    try {
      const connecting = f.client.reconnect();
      await vi.advanceTimersByTimeAsync(300);
      await connecting;
      expect(f.sockets).toHaveLength(1);
      const socket = f.sockets[0];
      if (!socket) throw new Error("Missing progress recovery socket");
      const replay = f.client.waitForReplayComplete();
      replay.catch(() => undefined);
      emitFencedReplay(socket);
      await vi.advanceTimersByTimeAsync(0);
      expect(handled).toEqual([1]);
      expect(f.client.debugInfo().lastHandledSeq).toBe(1);
      await expect(replay).rejects.toMatchObject({
        code: "PERSISTENCE",
        details: { status: 503, operation: "list-events" },
      });
      await vi.advanceTimersByTimeAsync(99);
      expect(f.sockets).toHaveLength(1);
      await vi.advanceTimersByTimeAsync(1);
      expect(f.sockets).toHaveLength(2);
      const replacement = f.sockets[1];
      if (!replacement) throw new Error("Missing cursor-progress replacement socket");
      emitFencedReplay(replacement);
      await f.client.waitForReplayComplete();
      expect(handled).toEqual([1, 2]);
      expect(f.client.debugInfo().lastHandledSeq).toBe(2);
    } finally {
      await f.client.closeAndWait();
      await vi.advanceTimersByTimeAsync(800);
      vi.useRealTimers();
    }
  });

  it("counts refused opens but not post-open REST snapshot failures in reconnect diagnostics", async () => {
    vi.spyOn(Math, "random").mockReturnValue(1);
    vi.useFakeTimers();
    vi.setSystemTime(0);
    let attempts = 0;
    let snapshots = 0;
    const f = await fixture({
      config: {
        reconnect: { baseDelayMs: 100, maxDelayMs: 800 },
        webSocketFactory: (url) => {
          attempts += 1;
          const socket = new ParticipantFakeWebSocket(
            url,
            attempts === 2 ? new Error("synthetic refused upgrade") : undefined,
          );
          socket.once("open", () => queueMicrotask(() => emitFencedReplay(socket)));
          return socket as unknown as WebSocket;
        },
      },
      tasks: () => (++snapshots === 1 ? Response.json({}, { status: 503 }) : { tasks: [] }),
    });
    const loop = f.client.runClaimableTasks({
      claimRefreshMs: 1_000,
      executor: createExecutor(),
      once: false,
      shouldClaimTask: () => true,
    });
    loop.catch(() => undefined);
    try {
      await f.client.open();
      await flushMicrotasks();
      expect(snapshots).toBe(1);
      expect(f.client.debugInfo().reconnectFailureCount).toBe(0);
      await vi.advanceTimersByTimeAsync(100);
      expect(f.client.debugInfo().reconnectFailureCount).toBe(1);
      await vi.advanceTimersByTimeAsync(200);
      expect(snapshots).toBe(2);
      expect(f.client.debugInfo().reconnectFailureCount).toBe(1);
    } finally {
      f.client.close();
      await vi.advanceTimersByTimeAsync(800);
      await loop;
      await f.client.closeAndWait();
      vi.useRealTimers();
    }
  });

  it("grows one recovery backoff across a snapshot failure followed by refused WebSocket upgrades", async () => {
    vi.spyOn(Math, "random").mockReturnValue(1);
    vi.useFakeTimers();
    vi.setSystemTime(0);
    const attempts: number[] = [];
    const f = await fixture({
      config: {
        reconnect: { baseDelayMs: 100, maxDelayMs: 800 },
        webSocketFactory: (url) => {
          attempts.push(Date.now());
          const socket = new ParticipantFakeWebSocket(
            url,
            attempts.length > 1 ? new Error("synthetic refused upgrade") : undefined,
          );
          if (attempts.length === 1)
            socket.once("open", () => queueMicrotask(() => emitFencedReplay(socket)));
          return socket as unknown as WebSocket;
        },
      },
      tasks: () => Response.json({ error: "synthetic service unavailable" }, { status: 503 }),
    });
    const loop = f.client.runClaimableTasks({
      claimRefreshMs: 1_000,
      executor: createExecutor(),
      once: false,
      shouldClaimTask: () => true,
    });
    loop.catch(() => undefined);
    try {
      await f.client.open();
      await flushMicrotasks();
      await vi.advanceTimersByTimeAsync(700);
      expect(attempts).toEqual([0, 100, 300, 700]);
      expect(f.client.debugInfo().pausedReason).toBeNull();
      expect(f.writes).toEqual([]);
    } finally {
      await f.client.closeAndWait();
      await vi.advanceTimersByTimeAsync(800);
      await loop;
      vi.useRealTimers();
    }
  });

  it("grows snapshot transient backoff across successful wire opens until real recovery", async () => {
    vi.spyOn(Math, "random").mockReturnValue(1);
    vi.useFakeTimers();
    vi.setSystemTime(0);
    const snapshots: number[] = [];
    const sockets: ParticipantFakeWebSocket[] = [];
    let unavailable = true;
    const f = await fixture({
      config: {
        reconnect: { baseDelayMs: 100, maxDelayMs: 400 },
        webSocketFactory: (url) => {
          const socket = new ParticipantFakeWebSocket(url);
          sockets.push(socket);
          socket.once("open", () => queueMicrotask(() => emitFencedReplay(socket)));
          return socket as unknown as WebSocket;
        },
      },
      tasks: () => {
        snapshots.push(Date.now());
        return unavailable
          ? Response.json({ error: "synthetic service unavailable" }, { status: 503 })
          : { tasks: [] };
      },
    });
    const loop = f.client.runClaimableTasks({
      claimRefreshMs: 1_000,
      executor: createExecutor(),
      once: false,
      shouldClaimTask: () => true,
    });
    loop.catch(() => undefined);
    try {
      await f.client.open();
      await flushMicrotasks();
      expect(snapshots).toEqual([0]);
      await vi.advanceTimersByTimeAsync(1_000);
      expect(snapshots).toEqual([0, 100, 300, 700]);
      unavailable = false;
      await vi.advanceTimersByTimeAsync(100);
      expect(snapshots).toEqual([0, 100, 300, 700, 1_100]);
      unavailable = true;
      sockets.at(-1)?.emitClose();
      await vi.advanceTimersByTimeAsync(200);
      expect(snapshots).toEqual([0, 100, 300, 700, 1_100, 1_200, 1_300]);
      expect(f.client.debugInfo()).toMatchObject({ pausedReason: null, lastHandledSeq: 0 });
      expect(f.writes).toEqual([]);
    } finally {
      await f.client.closeAndWait();
      await vi.advanceTimersByTimeAsync(400);
      await loop;
      vi.useRealTimers();
    }
  });

  it.each([
    503,
    502,
    500,
    504,
    429,
    408,
    "body",
  ])("retries transient snapshot failure %s before task dispatch", async (failure) => {
    let snapshots = 0;
    const f = await fixture({
      config: { reconnect: { baseDelayMs: 0, maxDelayMs: 0 } },
      tasks: () => {
        if (++snapshots > 1) return { tasks: [] };
        if (failure === "body")
          return new Response(
            new ReadableStream<Uint8Array>({
              start: (controller) =>
                controller.error(new Error("synthetic body transport failure")),
            }),
          );
        return Response.json(
          { error: "synthetic temporary service failure" },
          { status: Number(failure) },
        );
      },
    });
    const loop = f.client.runClaimableTasks({
      claimRefreshMs: 1_000,
      executor: createExecutor(),
      once: true,
      shouldClaimTask: () => true,
    });
    loop.catch(() => undefined);
    const socket = await f.open();
    emitFencedReplay(socket);
    await waitFor(() => f.sockets.length === 2);
    const replacement = f.sockets[1];
    if (!replacement) throw new Error("Missing transient snapshot replacement socket");
    emitFencedReplay(replacement);
    await loop;
    expect(snapshots).toBe(2);
    expect(f.client.debugInfo().pausedReason).toBeNull();
    expect(f.writes).toEqual([]);
  });

  it("keeps unsupported snapshot HTTP 501 terminal instead of retrying it", async () => {
    let snapshots = 0;
    const f = await fixture({
      config: { reconnect: { baseDelayMs: 100, maxDelayMs: 100 } },
      tasks: () => {
        snapshots += 1;
        return Response.json({ error: "synthetic unsupported operation" }, { status: 501 });
      },
    });
    const loop = f.client.runClaimableTasks({
      claimRefreshMs: 1_000,
      executor: createExecutor(),
      once: true,
      shouldClaimTask: () => true,
    });
    loop.catch(() => undefined);
    emitFencedReplay(await f.open());
    await flushMicrotasks();
    expect(f.client.debugInfo().pausedReason).toBe("rest_replay_failed");
    await expect(loop).rejects.toThrow();
    expect(snapshots).toBe(1);
    expect(f.sockets).toHaveLength(1);
    expect(f.writes).toEqual([]);
  });

  it.each([
    401, 403,
  ])("keeps snapshot authentication failure %s terminal and actionable", async (status) => {
    let snapshots = 0;
    const f = await fixture({
      tasks: () => {
        snapshots += 1;
        return Response.json({ error: "synthetic auth refusal" }, { status });
      },
    });
    const loop = f.client.runClaimableTasks({
      claimRefreshMs: 1_000,
      executor: createExecutor(),
      once: true,
      shouldClaimTask: () => true,
    });
    const rejected = expect(loop).rejects.toThrow();
    const socket = await f.open();
    emitFencedReplay(socket);
    await rejected;
    expect(snapshots).toBe(1);
    expect(f.sockets).toHaveLength(1);
    expect(f.client.debugInfo().pausedReason).toBe("rest_replay_failed");
  });

  it("delivers more than the WS replay limit from zero, then live events, without seeding the cursor", async () => {
    const f = await fixture({ head: 2_105 });
    const handled: number[] = [];
    f.client.onEvent((event) => {
      handled.push(event.seq);
    });
    const socket = await f.open();
    expect(new URL(socket.url).searchParams.get("after")).toBe("2105");
    expect(f.client.debugInfo().lastHandledSeq).toBe(0);
    expect(f.writes).toEqual([]);
    socket.emitServerEvent(restHistoryEvent(2_106));
    emitFencedReplay(socket);
    await f.client.waitForReplayComplete();
    expect(handled).toEqual(Array.from({ length: 2_106 }, (_, i) => i + 1));
    socket.emitServerEvent(restHistoryEvent(2_107));
    await waitFor(() => f.client.debugInfo().lastHandledSeq === 2_107);
    expect(handled.at(-1)).toBe(2_107);
    expect(f.requests.every((request) => request.limit > 0 && request.limit <= 1_000)).toBe(true);
    expect(f.requests.filter((request) => request.after === 0)).toHaveLength(2);
  });

  it("waits for actual asynchronous handler progress before fetching another history page or persisting", async () => {
    const f = await fixture({ head: 1_005 });
    const entered = createDeferred<void>();
    const release = createDeferred<void>();
    f.client.onEvent(async (event) => {
      if (event.seq === 1) {
        entered.resolve();
        await release.promise;
      }
    });
    const socket = await f.open();
    const scanCalls = f.requests.length;
    let replaySettled = false;
    const replay = f.client.waitForReplayComplete().then(() => {
      replaySettled = true;
    });
    emitFencedReplay(socket);
    try {
      await waitFor(() => f.client.debugInfo().activeDeliverySeq === 1);
      await entered.promise;
      expect(f.requests).toHaveLength(scanCalls + 1);
      expect(f.writes).toEqual([]);
      expect(f.client.debugInfo().lastHandledSeq).toBe(0);
      expect(replaySettled).toBe(false);
    } finally {
      release.resolve();
    }
    await replay;
    expect(f.client.debugInfo().lastHandledSeq).toBe(1_005);
  });

  it("waits for the wire epoch and dispatches command responses while a replay handler awaits them", async () => {
    const f = await fixture({ head: 1 });
    const handled: number[] = [];
    f.client.onEvent(async (event) => {
      handled.push(event.seq);
      await f.client.appendEvent({
        eventId: "evt_command",
        payload: {},
        producerId: baseConfig.participantId,
        sessionId: baseConfig.sessionId,
        type: "fixture.output",
      });
    });
    const socket = await f.open();
    await new Promise<void>((resolve) => setImmediate(resolve));
    expect(handled).toEqual([]);
    expect(socket.sent).toEqual([]);
    emitFencedReplay(socket);
    await waitFor(() => socket.sent.length === 1);
    const command = JSON.parse(socket.sent[0] ?? "null") as { requestId: string };
    socket.emit(
      "message",
      JSON.stringify({
        command: "event.publish",
        op: "command.result",
        requestId: command.requestId,
      }),
    );
    await f.client.waitForReplayComplete();
    expect(handled).toEqual([1]);
    expect(f.client.debugInfo().pendingCommandCount).toBe(0);
  });

  it("stops later queued history handlers after terminal pause while the first handler settles", async () => {
    const f = await fixture({ head: 3 });
    const release = createDeferred<void>();
    const entered = createDeferred<void>();
    const settled = createDeferred<void>();
    const handled: number[] = [];
    f.client.onEvent(async (event) => {
      handled.push(event.seq);
      if (event.seq === 1) {
        entered.resolve();
        await release.promise;
        settled.resolve();
      }
    });
    const socket = await f.open();
    const rejected = expect(f.client.waitForReplayComplete()).rejects.toMatchObject({
      reason: "replay_gap_unrepaired",
    });
    emitFencedReplay(socket);
    try {
      await entered.promise;
      expect(f.client.debugInfo().eventBacklogSize).toBe(3);
      socket.emit(
        "message",
        JSON.stringify({
          op: "error",
          error: "synthetic terminal pause",
          reason: "replay_gap_unrepaired",
        }),
      );
      await rejected;
      release.resolve();
      await settled.promise;
      await new Promise<void>((resolve) => setImmediate(resolve));
      expect(handled).toEqual([1]);
      expect(f.client.debugInfo().lastHandledSeq).toBe(1);
      expect(f.writes.every((seq) => seq <= 1)).toBe(true);
    } finally {
      release.resolve();
    }
  });

  it.each([
    "close",
    "reconnect",
  ])("cancels a stalled runtime response body on %s without late delivery or retry", async (action) => {
    let stalled = false;
    let pulled = false;
    let cancelled = false;
    const f = await fixture({
      page: (after, limit, _call, _signal, phase) => {
        if (phase === "history" && !stalled) {
          stalled = true;
          return new Response(
            new ReadableStream<Uint8Array>(
              {
                pull: () => {
                  pulled = true;
                },
                cancel: () => {
                  cancelled = true;
                },
              },
              { highWaterMark: 0 },
            ),
          );
        }
        return restHistoryPage(after, limit, stalled ? 0 : 1);
      },
    });
    const handled: number[] = [];
    f.client.onEvent((event) => {
      handled.push(event.seq);
    });
    const socket = await f.open();
    const oldReplay = f.client.waitForReplayComplete();
    oldReplay.catch(() => undefined);
    emitFencedReplay(socket);
    await waitFor(() => pulled);
    if (action === "close") await f.client.closeAndWait();
    else {
      await f.client.reconnect();
      const replacement = f.sockets[1];
      if (!replacement) throw new Error("Missing stalled-body replacement socket");
      emitFencedReplay(replacement);
      await f.client.waitForReplayComplete();
    }
    await expect(oldReplay).rejects.toThrow();
    expect(cancelled).toBe(true);
    expect(handled).toEqual([]);
    expect(f.writes).toEqual([]);
    expect(f.sockets).toHaveLength(action === "close" ? 1 : 2);
  });

  it("discards queued WebSocket delivery during handler recovery before socket closure", async () => {
    const failed = createRejectableDeferred<void>();
    const entered = createDeferred<void>();
    const handled: number[] = [];
    const f = await fixture({
      config: { replayMode: "websocket", reconnect: { baseDelayMs: 0, maxDelayMs: 0 } },
    });
    f.client.onEvent(async (event) => {
      handled.push(event.seq);
      entered.resolve();
      await failed.promise;
    });
    const socket = await f.open();
    socket.deferClose = true;
    try {
      socket.emitServerEvent(restHistoryEvent(1));
      await entered.promise;
      socket.emitServerEvent(restHistoryEvent(2));
      socket.emitServerEvent(restHistoryEvent(3));
      expect(f.client.debugInfo().eventBacklogSize).toBe(3);
      failed.reject(new Error("synthetic handler failure"));
      await waitFor(() => f.client.debugInfo().eventDeliveryFailureCount === 1);
      expect(f.client.debugInfo().eventBacklogSize).toBe(0);
      expect(f.sockets).toHaveLength(1);
      expect(handled).toEqual([1]);
      expect(f.writes).toEqual([]);
    } finally {
      f.client.close();
      socket.emitClose();
      await f.client.closeAndWait();
    }
  });

  it("does not reconnect a superseded WebSocket recovery after its timed-out handler settles", async () => {
    const release = createDeferred<void>();
    const entered = createDeferred<void>();
    const f = await fixture({
      config: {
        replayMode: "websocket",
        eventDelivery: { handlerTimeoutMs: 5 },
        reconnect: { baseDelayMs: 0, maxDelayMs: 0 },
      },
    });
    f.client.onEvent(async () => {
      entered.resolve();
      await release.promise;
    });
    const socket = await f.open();
    try {
      socket.emitServerEvent(restHistoryEvent(1));
      await entered.promise;
      await waitFor(() => f.client.debugInfo().eventDeliveryFailureCount === 1);
      await f.client.reconnect();
      const generation = f.client.debugInfo().connectionGeneration;
      expect(f.sockets).toHaveLength(2);
      release.resolve();
      await new Promise<void>((resolve) => setImmediate(resolve));
      expect(f.sockets).toHaveLength(2);
      expect(f.client.debugInfo().connectionGeneration).toBe(generation);
    } finally {
      release.resolve();
      await f.client.closeAndWait();
    }
  });

  it("rejects the startup replay waiter when direct REST discovery returns 503", async () => {
    const f = await fixture({ page: () => Response.json({}, { status: 503 }) });
    const replay = f.client.waitForReplayComplete();
    replay.catch(() => undefined);
    try {
      await expect(f.client.open()).rejects.toMatchObject({
        code: "PERSISTENCE",
        details: { status: 503 },
      });
      await expect(replay).rejects.toMatchObject({ code: "PERSISTENCE", details: { status: 503 } });
      await f.client.waitForClose();
      expect(f.sockets).toEqual([]);
      expect(f.writes).toEqual([]);
    } finally {
      await f.client.closeAndWait();
    }
  });

  it("preserves WebSocket byte-overflow details before admitting an oversized frame", async () => {
    const f = await fixture({
      config: { replayMode: "websocket", eventDelivery: { maxQueueBytes: 64 } },
    });
    const socket = await f.open();
    const event = restHistoryEvent(1);
    try {
      socket.emitServerEvent(event);
      expect(f.errors.at(-1)).toMatchObject({
        reason: "delivery_byte_overflow",
        safeDetails: {
          maxQueueBytes: 64,
          observedQueueBytes: Buffer.byteLength(JSON.stringify({ event, op: "event" })),
        },
      });
      expect(f.client.debugInfo().lastHandledSeq).toBe(0);
      expect(f.writes).toEqual([]);
    } finally {
      await f.client.closeAndWait();
    }
  });

  it("preserves the WebSocket delivery-recovery handoff delay after a refused upgrade", async () => {
    vi.spyOn(Math, "random").mockReturnValue(1);
    vi.useFakeTimers();
    vi.setSystemTime(0);
    const attempts: number[] = [];
    const sockets: ParticipantFakeWebSocket[] = [];
    const client = await ParticipantRuntimeClient.create({
      ...baseConfig,
      reconnect: { baseDelayMs: 100, maxDelayMs: 800 },
      webSocketFactory: (url) => {
        attempts.push(Date.now());
        const socket = new ParticipantFakeWebSocket(
          url,
          attempts.length === 2 ? new Error("synthetic refused upgrade") : undefined,
        );
        sockets.push(socket);
        return socket as unknown as WebSocket;
      },
    });
    client.onError(() => undefined);
    client.onEvent(async () => {
      throw new Error("synthetic delivery failure");
    });
    try {
      await client.open();
      const socket = sockets[0];
      if (!socket) throw new Error("Missing delivery handoff socket");
      socket.emitServerEvent(restHistoryEvent(1));
      await flushMicrotasks();
      await vi.advanceTimersByTimeAsync(300);
      expect(attempts).toEqual([0, 100, 300]);
      expect(client.debugInfo().lastHandledSeq).toBe(0);
    } finally {
      client.close();
      await vi.advanceTimersByTimeAsync(800);
      await client.closeAndWait();
      vi.useRealTimers();
    }
  });

  it("preserves default WebSocket outage delays and restarts them after a successful open", async () => {
    vi.spyOn(Math, "random").mockReturnValue(1);
    vi.useFakeTimers();
    vi.setSystemTime(0);
    const attempts: number[] = [];
    const sockets: ParticipantFakeWebSocket[] = [];
    const client = await ParticipantRuntimeClient.create({
      ...baseConfig,
      reconnect: { baseDelayMs: 100, maxDelayMs: 800 },
      webSocketFactory: (url) => {
        attempts.push(Date.now());
        const refused = [2, 3, 5].includes(attempts.length);
        const socket = new ParticipantFakeWebSocket(
          url,
          refused ? new Error("synthetic refused upgrade") : undefined,
        );
        sockets.push(socket);
        if (attempts.length === 1)
          socket.once("open", () => queueMicrotask(() => emitFencedReplay(socket)));
        return socket as unknown as WebSocket;
      },
    });
    client.onError(() => undefined);
    const loop = client.runClaimableTasks({
      claimRefreshMs: 1_000,
      executor: createExecutor(),
      once: false,
      shouldClaimTask: () => true,
    });
    loop.catch(() => undefined);
    try {
      await client.open();
      await flushMicrotasks();
      sockets[0]?.emitClose();
      await vi.advanceTimersByTimeAsync(700);
      expect(attempts).toEqual([0, 100, 300, 700]);
      /** No replay marker or handled event occurs on the successful replacement. */
      sockets[3]?.emitClose();
      await vi.advanceTimersByTimeAsync(300);
      expect(attempts).toEqual([0, 100, 300, 700, 800, 1_000]);
      expect(client.debugInfo().lastHandledSeq).toBe(0);
    } finally {
      client.close();
      await vi.advanceTimersByTimeAsync(800);
      await loop;
      await client.closeAndWait();
      vi.useRealTimers();
    }
  });

  it("prepares no WebSocket generation when explicitly closed during reconnect backoff", async () => {
    const sockets: ParticipantFakeWebSocket[] = [];
    const f = await fixture({
      config: {
        replayMode: "websocket",
        reconnect: { baseDelayMs: 10, maxDelayMs: 10 },
        webSocketFactory: (url) => {
          if (sockets.length > 0) throw new Error("synthetic WebSocket refusal");
          const socket = new ParticipantFakeWebSocket(url);
          sockets.push(socket);
          return socket as unknown as WebSocket;
        },
      },
    });
    await f.client.open();
    vi.useFakeTimers();
    try {
      const reconnect = f.client.reconnect();
      await waitForMicrotasks(() => f.client.debugInfo().reconnectFailureCount === 1);
      const generation = f.client.debugInfo().connectionGeneration;
      await f.client.closeAndWait();
      await vi.advanceTimersByTimeAsync(10);
      await reconnect;
      expect(f.requests).toEqual([]);
      expect(sockets).toHaveLength(1);
      expect(f.client.debugInfo()).toMatchObject({
        stopped: true,
        pausedReason: null,
        connectionGeneration: generation,
        reconnectFailureCount: 1,
      });
    } finally {
      vi.useRealTimers();
    }
  });

  it("stops a transient reconnect backoff when explicitly closed", async () => {
    const f = await fixture({
      config: { reconnect: { baseDelayMs: 10, maxDelayMs: 10 } },
      page: () => {
        throw new TypeError("synthetic unavailable transport");
      },
    });
    vi.useFakeTimers();
    try {
      const reconnect = f.client.reconnect();
      await waitForMicrotasks(() => f.client.debugInfo().reconnectFailureCount === 1);
      const generation = f.client.debugInfo().connectionGeneration;
      await f.client.closeAndWait();
      await vi.advanceTimersByTimeAsync(10);
      await reconnect;
      expect(f.requests).toHaveLength(1);
      expect(f.sockets).toEqual([]);
      expect(f.client.debugInfo()).toMatchObject({
        stopped: true,
        pausedReason: null,
        connectionGeneration: generation,
        reconnectFailureCount: 1,
      });
    } finally {
      vi.useRealTimers();
    }
  });

  it("holds live events and the replay marker in wire order across a delayed REST page", async () => {
    const pageReady = createDeferred<void>();
    const release = createDeferred<void>();
    const f = await fixture({
      page: async (after, limit, _call, _signal, phase) => {
        if (phase === "history") {
          pageReady.resolve();
          await release.promise;
        }
        return restHistoryPage(after, limit, 1);
      },
    });
    const handled: number[] = [];
    f.client.onEvent((event) => {
      handled.push(event.seq);
    });
    const socket = await f.open();
    socket.emitServerEvent(restHistoryEvent(2));
    emitFencedReplay(socket);
    try {
      await pageReady.promise;
      socket.emitServerEvent(restHistoryEvent(3));
      expect(handled).toEqual([]);
      expect(f.writes).toEqual([]);
    } finally {
      release.resolve();
    }
    await f.client.waitForReplayComplete();
    await waitFor(() => f.client.debugInfo().lastHandledSeq === 3);
    expect(handled).toEqual([1, 2, 3]);
  });

  it.each([
    "count",
    "bytes",
  ])("rejects an active snapshot above the task %s budget before dispatch", async (bound) => {
    const tasks = Array.from({ length: bound === "count" ? 3 : 2 }, (_, index) => ({
      ...baseTask,
      taskId: `task_budget_${index}`,
    }));
    const f = await fixture({
      config: { eventDelivery: bound === "count" ? { maxQueueSize: 2 } : { maxQueueBytes: 800 } },
      tasks: () => ({ tasks }),
    });
    const executed: string[] = [];
    const loop = f.client.runClaimableTasks({
      claimRefreshMs: 1_000,
      executor: async ({ task }) => {
        executed.push(task.taskId);
        return { result: {} };
      },
      once: true,
      shouldClaimTask: () => true,
    });
    const rejected = expect(loop).rejects.toThrow();
    const socket = await f.open();
    replyToTaskCommands(socket, tasks);
    emitFencedReplay(socket);
    await rejected;
    expect(executed).toEqual([]);
    expect(socket.sent).toEqual([]);
    expect(f.client.debugInfo().pausedReason).toBe(
      bound === "count" ? "delivery_queue_overflow" : "delivery_byte_overflow",
    );
    expect(f.errors.at(-1)).toMatchObject({
      safeDetails:
        bound === "count"
          ? { maxQueueSize: 2, observedQueueSize: 3 }
          : {
              maxQueueBytes: 800,
              observedQueueBytes: tasks.reduce(
                (bytes, task) => bytes + Buffer.byteLength(JSON.stringify(task)),
                0,
              ),
            },
    });
    expect(f.writes).toEqual([]);
  });

  it.each([
    "completed",
    "cancelled",
  ])("runs REST claims serially and suppresses %s queued work through the existing claim flow", async (terminal) => {
    const tasks = [baseTask, otherTask, { ...baseTask, taskId: "task_third" }];
    const f = await fixture({ tasks: () => ({ tasks }) });
    const entered: string[] = [];
    const release = createDeferred<void>();
    const loop = f.client.runClaimableTasks({
      claimRefreshMs: 1_000,
      executor: async ({ task }) => {
        entered.push(task.taskId);
        if (task.taskId === baseTask.taskId) await release.promise;
        return { result: {} };
      },
      once: true,
      shouldClaimTask: () => true,
    });
    loop.catch(() => undefined);
    const socket = await f.open();
    replyToTaskCommands(socket, tasks);
    emitFencedReplay(socket);
    try {
      await waitFor(() => entered.length > 0);
      socket.emitServerEvent({
        ...createTaskReleasedEvent(
          {
            ...otherTask,
            ...(terminal === "completed"
              ? { completedAt: "2026-06-05T00:01:00Z" }
              : { cancelledAt: "2026-06-05T00:01:00Z" }),
          },
          1,
        ),
        type: terminal === "completed" ? "task.completed" : "control.cancel",
      });
      await waitFor(() => f.client.debugInfo().lastHandledSeq === 1);
      expect(entered).toEqual([baseTask.taskId]);
      expect(
        socket.sent
          .map((packet) => JSON.parse(packet) as { op: string; taskId?: string })
          .filter((command) => command.op === "task.claim")
          .map((command) => command.taskId),
      ).toEqual([baseTask.taskId]);
    } finally {
      release.resolve();
    }
    await loop;
    expect(entered).toEqual([baseTask.taskId, "task_third"]);
  });

  it.each([
    "count",
    "bytes",
  ])("bounds task candidates accumulated across history pages by %s", async (bound) => {
    const tasks = Array.from({ length: 3 }, (_, index) => ({
      ...baseTask,
      taskId: `task_retained_${index}`,
    }));
    const f = await fixture({
      config: { eventDelivery: bound === "count" ? { maxQueueSize: 2 } : { maxQueueBytes: 1_200 } },
      page: (after, limit) => {
        const page = restHistoryPage(after, limit, 3);
        return {
          ...page,
          events: page.events.map((event) => {
            const task = tasks[event.seq - 1];
            if (!task) throw new Error("Missing synthetic retained task");
            return createTaskCreatedEvent(task, event.seq);
          }),
        };
      },
    });
    const executed: string[] = [];
    const loop = f.client.runClaimableTasks({
      claimRefreshMs: 1_000,
      executor: async ({ task }) => {
        executed.push(task.taskId);
        return { result: {} };
      },
      once: true,
      shouldClaimTask: () => true,
    });
    const rejected = expect(loop).rejects.toThrow();
    const socket = await f.open();
    replyToTaskCommands(socket, tasks);
    emitFencedReplay(socket);
    await rejected;
    expect(executed).toEqual([]);
    expect(socket.sent).toEqual([]);
    expect(f.client.debugInfo()).toMatchObject({
      lastHandledSeq: 2,
      pausedReason: bound === "count" ? "delivery_queue_overflow" : "delivery_byte_overflow",
    });
    expect(f.errors.at(-1)).toMatchObject({
      safeDetails:
        bound === "count"
          ? { maxQueueSize: 2, observedQueueSize: 3 }
          : {
              maxQueueBytes: 1_200,
              observedQueueBytes: tasks.reduce(
                (bytes, task) => bytes + Buffer.byteLength(JSON.stringify(task)),
                0,
              ),
            },
    });
    expect(f.writes.every((seq) => seq <= 2)).toBe(true);
  });

  it.each([
    "count",
    "bytes",
  ])("bounds touched task IDs during an outstanding snapshot by %s", async (bound) => {
    const release = createDeferred<void>();
    let loading = false;
    const count = bound === "count" ? 3 : 4;
    const f = await fixture({
      config: {
        eventDelivery:
          bound === "count" ? { maxQueueSize: 2 } : { maxQueueSize: 10, maxQueueBytes: 1_400 },
      },
      tasks: async () => {
        loading = true;
        await release.promise;
        return { tasks: [] };
      },
    });
    const loop = f.client.runClaimableTasks({
      claimRefreshMs: 1_000,
      executor: createExecutor(),
      once: true,
      shouldClaimTask: () => true,
    });
    loop.catch(() => undefined);
    const socket = await f.open();
    emitFencedReplay(socket);
    try {
      await waitFor(() => loading);
      for (let index = 0; index < count; index += 1) {
        const task = {
          ...baseTask,
          taskId: `${bound === "bytes" ? "x".repeat(400) : "touched"}_${index}`,
          completedAt: "2026-06-05T00:01:00Z",
        };
        socket.emitServerEvent({
          ...createTaskReleasedEvent(task, index + 1),
          type: "task.completed",
        });
        if (index < count - 1)
          await waitFor(() => f.client.debugInfo().lastHandledSeq === index + 1);
      }
      await waitFor(() => f.client.debugInfo().pausedReason !== null);
      expect(f.client.debugInfo()).toMatchObject({
        lastHandledSeq: count - 1,
        pausedReason: bound === "count" ? "delivery_queue_overflow" : "delivery_byte_overflow",
      });
      expect(socket.sent).toEqual([]);
    } finally {
      release.resolve();
    }
    await expect(loop).rejects.toThrow();
  });

  it("keeps a running reservation charged when a replacement generation loads pending work", async () => {
    const third = { ...baseTask, taskId: "task_replacement_third" };
    let snapshots = 0;
    const f = await fixture({
      config: { eventDelivery: { maxQueueSize: 2 }, reconnect: { baseDelayMs: 0, maxDelayMs: 0 } },
      tasks: () => ({ tasks: ++snapshots === 1 ? [baseTask] : [otherTask, third] }),
    });
    const release = createDeferred<void>();
    const executed: string[] = [];
    const loop = f.client.runClaimableTasks({
      claimRefreshMs: 1_000,
      executor: async ({ task }) => {
        executed.push(task.taskId);
        await release.promise;
        return { result: {} };
      },
      once: false,
      shouldClaimTask: () => true,
    });
    loop.catch(() => undefined);
    const socket = await f.open();
    replyToTaskCommands(socket, [baseTask]);
    emitFencedReplay(socket);
    try {
      await waitFor(() => executed.length === 1);
      f.client.disconnect();
      await waitFor(() => f.sockets.length === 2);
      const replacement = f.sockets[1];
      if (!replacement) throw new Error("Missing replacement task-owner socket");
      replyToTaskCommands(replacement, [otherTask, third]);
      emitFencedReplay(replacement);
      await waitFor(() => f.client.debugInfo().pausedReason !== null);
      expect(executed).toEqual([baseTask.taskId]);
      expect(replacement.sent).toEqual([]);
      expect(f.client.debugInfo().pausedReason).toBe("delivery_queue_overflow");
    } finally {
      release.resolve();
    }
    await expect(loop).rejects.toThrow();
  });

  it("requeues a task that becomes claimable again while its current claim is running", async () => {
    const started = createDeferred<void>();
    const release = createDeferred<void>();
    const tasks: TaskRecord[] = [baseTask];
    const f = await fixture({
      config: { eventDelivery: { maxQueueSize: 1 } },
      tasks: () => ({ tasks: [baseTask] }),
    });
    const executed: string[] = [];
    const loop = f.client.runClaimableTasks({
      claimRefreshMs: 1_000,
      executor: async ({ task }) => {
        executed.push(task.objective);
        if (executed.length === 1) {
          started.resolve();
          await release.promise;
        }
        return { result: {} };
      },
      once: true,
      shouldClaimTask: () => true,
    });
    loop.catch(() => undefined);
    const socket = await f.open();
    replyToTaskCommands(socket, tasks);
    emitFencedReplay(socket);
    try {
      await started.promise;
      await waitFor(() => f.client.debugInfo().eventBacklogSize === 0);
      const updated = { ...baseTask, objective: "Run again" };
      tasks[0] = updated;
      socket.emitServerEvent(createTaskReleasedEvent(updated, 1));
      await waitFor(() => f.client.debugInfo().lastHandledSeq === 1);
      release.resolve();
      await loop;
      expect(executed).toEqual(["Summarize the session", "Run again"]);
      expect(
        socket.sent
          .map((packet) => JSON.parse(packet) as { readonly op: string })
          .filter(({ op }) => op === "task.claim"),
      ).toHaveLength(2);
      expect(f.client.debugInfo().pausedReason).toBeNull();
    } finally {
      release.resolve();
    }
  });

  it("releases snapshot tombstones before admitting later work at the full task-count budget", async () => {
    const snapshotStarted = createDeferred<void>();
    const releaseSnapshot = createDeferred<void>();
    const snapshotEnded = createDeferred<void>();
    const releaseBarrier = createDeferred<void>();
    const f = await fixture({
      config: { eventDelivery: { maxQueueSize: 1 } },
      tasks: async () => {
        snapshotStarted.resolve();
        await releaseSnapshot.promise;
        return { tasks: [baseTask] };
      },
    });
    const executed: string[] = [];
    const loop = f.client.runClaimableTasks({
      claimRefreshMs: 1_000,
      executor: async ({ task }) => {
        executed.push(task.taskId);
        return { result: {} };
      },
      once: true,
      replayBarrier: async () => {
        snapshotEnded.resolve();
        await releaseBarrier.promise;
      },
      shouldClaimTask: () => true,
    });
    loop.catch(() => undefined);
    const socket = await f.open();
    replyToTaskCommands(socket, [otherTask]);
    emitFencedReplay(socket);
    try {
      await snapshotStarted.promise;
      await waitFor(() => f.client.debugInfo().eventBacklogSize === 0);
      socket.emitServerEvent({
        ...createTaskReleasedEvent({ ...baseTask, completedAt: "2026-06-05T00:01:00Z" }, 1),
        type: "task.completed",
      });
      await waitFor(
        () =>
          f.client.debugInfo().lastHandledSeq === 1 && f.client.debugInfo().eventBacklogSize === 0,
      );
      releaseSnapshot.resolve();
      await snapshotEnded.promise;
      socket.emitServerEvent(createTaskCreatedEvent(otherTask, 2));
      await waitFor(
        () =>
          f.client.debugInfo().lastHandledSeq === 2 || f.client.debugInfo().pausedReason !== null,
      );
      releaseBarrier.resolve();
      await loop;
      expect(executed).toEqual([otherTask.taskId]);
      expect(f.client.debugInfo().pausedReason).toBeNull();
      expect(f.client.debugInfo().lastHandledSeq).toBe(2);
    } finally {
      releaseSnapshot.resolve();
      releaseBarrier.resolve();
    }
  });

  it.each([
    "duplicate",
    "terminal",
  ] as const)("releases replaced and terminal reservations without double-charging duplicate task IDs [%s]", async (kind) => {
    const first = { ...baseTask, objective: "x".repeat(500) };
    const replaced = { ...baseTask, objective: "short" };
    const second = { ...otherTask, objective: "y".repeat(500) };
    const third = { ...baseTask, taskId: "task_after_terminal", objective: "z".repeat(500) };
    const source =
      kind === "duplicate"
        ? [
            createTaskCreatedEvent(first, 1),
            createTaskCreatedEvent(second, 2),
            createTaskReleasedEvent(replaced, 3),
          ]
        : [
            createTaskCreatedEvent(first, 1),
            {
              ...createTaskReleasedEvent({ ...first, completedAt: "2026-06-05T00:01:00Z" }, 2),
              type: "task.completed",
            },
            createTaskCreatedEvent(second, 3),
            createTaskCreatedEvent(third, 4),
          ];
    const f = await fixture({
      config: { eventDelivery: { maxQueueSize: 2, maxQueueBytes: 2_000 } },
      page: (after, limit) => {
        const page = restHistoryPage(after, limit, source.length);
        return { ...page, events: source.filter((event) => event.seq > after).slice(0, limit) };
      },
    });
    const executed: string[] = [];
    const loop = f.client.runClaimableTasks({
      claimRefreshMs: 1_000,
      executor: async ({ task }) => {
        executed.push(task.objective);
        return { result: {} };
      },
      once: true,
      shouldClaimTask: () => true,
    });
    loop.catch(() => undefined);
    const socket = await f.open();
    replyToTaskCommands(socket, kind === "duplicate" ? [replaced, second] : [second, third]);
    emitFencedReplay(socket);
    await loop;
    if (kind === "duplicate") expect(executed).toEqual(["short", "y".repeat(500)]);
    else expect(executed).toEqual(["y".repeat(500), "z".repeat(500)]);
    expect(f.client.debugInfo().pausedReason).toBeNull();
  });

  it("recovers an active task whose request precedes the resumed cursor through the existing claim flow", async () => {
    const f = await fixture({ afterSeq: 50, head: 50, tasks: () => ({ tasks: [baseTask] }) });
    const claimed: string[] = [];
    vi.spyOn(f.client, "runTaskClaimFlow").mockImplementation(async ({ task }) => {
      claimed.push(task.taskId);
    });
    const loop = f.client.runClaimableTasks({
      claimRefreshMs: 1_000,
      executor: createExecutor(),
      once: true,
      shouldClaimTask: () => true,
    });
    loop.catch(() => undefined);
    const socket = await f.open();
    emitFencedReplay(socket);
    await loop;
    expect(claimed).toEqual([baseTask.taskId]);
    expect(f.writes).toEqual([]);
  });

  it("merges snapshot candidates without duplicates or overwriting live terminal state", async () => {
    const snapshotReady = createDeferred<void>();
    const release = createDeferred<void>();
    const f = await fixture({
      tasks: async () => {
        snapshotReady.resolve();
        await release.promise;
        return { tasks: [baseTask, otherTask] };
      },
    });
    const claimed: string[] = [];
    vi.spyOn(f.client, "runTaskClaimFlow").mockImplementation(async ({ task }) => {
      claimed.push(task.taskId);
    });
    const loop = f.client.runClaimableTasks({
      claimRefreshMs: 1_000,
      executor: createExecutor(),
      once: true,
      shouldClaimTask: () => true,
    });
    loop.catch(() => undefined);
    const socket = await f.open();
    emitFencedReplay(socket);
    try {
      await waitFor(() =>
        (fetch as ReturnType<typeof vi.fn>).mock.calls.some(([url]) =>
          (url as URL).pathname.endsWith("/tasks"),
        ),
      );
      await snapshotReady.promise;
      socket.emitServerEvent(createTaskCreatedEvent(baseTask, 1));
      socket.emitServerEvent({
        ...createTaskReleasedEvent({ ...otherTask, completedAt: "2026-06-05T00:01:00Z" }, 2),
        type: "task.completed",
      });
      socket.emitServerEvent({
        ...restHistoryEvent(3),
        payload: { task: { ...baseTask, completedAt: "2026-06-05T00:01:00Z" } },
      });
      await waitFor(() => f.client.debugInfo().lastHandledSeq === 3);
    } finally {
      release.resolve();
    }
    await loop;
    expect(claimed).toEqual([baseTask.taskId]);
  });

  it("rejects a malformed active snapshot instead of silently losing pending work", async () => {
    const f = await fixture({
      tasks: () => ({ tasks: [baseTask, { privatePayload: "must not escape" }] }),
    });
    const flow = vi.spyOn(f.client, "runTaskClaimFlow");
    const loop = f.client.runClaimableTasks({
      claimRefreshMs: 1_000,
      executor: createExecutor(),
      once: true,
      shouldClaimTask: () => true,
    });
    const rejected = expect(loop).rejects.toThrow();
    const socket = await f.open();
    emitFencedReplay(socket);
    await rejected;
    expect(flow).not.toHaveBeenCalled();
    expect(JSON.stringify(f.errors)).not.toContain("must not escape");
  });

  it.each([
    "non-progressing",
    "gap",
    "pagination",
    "malformed-event",
    "malformed-task",
  ])("fails closed for %s history pages without payload leakage", async (kind) => {
    const f = await fixture({
      page: (after, limit) => {
        const page = restHistoryPage(after, limit, 1);
        if (kind === "non-progressing")
          return {
            ...page,
            events: [],
            pagination: { ...page.pagination, returned: 0, nextAfterSeq: after, hasMore: true },
          };
        if (kind === "gap")
          return {
            ...page,
            events: [restHistoryEvent(2)],
            pagination: { ...page.pagination, nextAfterSeq: 2 },
          };
        if (kind === "pagination")
          return { ...page, pagination: { ...page.pagination, returned: 7 } };
        if (kind === "malformed-task")
          return {
            ...page,
            events: [
              {
                ...restHistoryEvent(1),
                type: "task.created",
                payload: { task: { privatePayload: "must not escape" } },
              },
            ],
          };
        return { ...page, events: [{ privatePayload: "must not escape" }] };
      },
    });
    const handled = vi.fn();
    f.client.onEvent(handled);
    await expect(f.client.open()).rejects.toThrow();
    expect(handled).not.toHaveBeenCalled();
    expect(f.writes).toEqual([]);
    expect(f.requests).toHaveLength(1);
    expect(JSON.stringify(f.errors)).not.toContain("must not escape");
  });

  it.each([
    "close",
    "reconnect",
  ])("aborts REST work on %s and fences late completions", async (action) => {
    const release = createDeferred<void>();
    let delayedSignal: AbortSignal | undefined;
    const f = await fixture({
      page: async (after, limit, _call, signal, phase) => {
        if (phase === "history" && delayedSignal === undefined) {
          delayedSignal = signal;
          await release.promise;
        }
        return restHistoryPage(
          after,
          limit,
          phase === "discovery" && delayedSignal !== undefined ? 0 : 1,
        );
      },
    });
    const handled: number[] = [];
    f.client.onEvent((event) => {
      handled.push(event.seq);
    });
    const socket = await f.open();
    const oldReplay = f.client.waitForReplayComplete();
    oldReplay.catch(() => undefined);
    emitFencedReplay(socket);
    try {
      await waitFor(() => delayedSignal !== undefined);
      if (action === "close") await f.client.closeAndWait();
      else {
        await f.client.reconnect();
        const replacement = f.sockets.at(-1);
        if (!replacement) throw new Error("Missing replacement socket");
        emitFencedReplay(replacement);
        await f.client.waitForReplayComplete();
      }
      expect(delayedSignal?.aborted).toBe(true);
    } finally {
      release.resolve();
    }
    await expect(oldReplay).rejects.toThrow();
    await flushPromises();
    expect(handled).toEqual([]);
    expect(f.writes).toEqual([]);
  });

  it.each([
    "close",
    "reconnect",
  ])("fences a stale history-head discovery after %s before any socket exists", async (action) => {
    const release = createDeferred<void>();
    let signal: AbortSignal | undefined;
    const f = await fixture({
      page: async (after, limit, call, requestSignal) => {
        if (call === 1) {
          signal = requestSignal;
          await release.promise;
        }
        return restHistoryPage(after, limit, call === 1 ? 1 : 0);
      },
    });
    f.client.onEvent(() => undefined);
    const opening = f.client.open();
    opening.catch(() => undefined);
    await waitFor(() => signal !== undefined);
    try {
      if (action === "close") {
        await f.client.closeAndWait();
      } else {
        await f.client.reconnect();
        expect(f.sockets).toHaveLength(1);
      }
      expect(signal?.aborted).toBe(true);
    } finally {
      release.resolve();
    }
    await expect(opening).rejects.toThrow();
    expect(f.writes).toEqual([]);
    if (action === "reconnect") {
      expect(f.client.debugInfo().pausedReason).toBeNull();
      const socket = f.sockets[0];
      if (!socket) throw new Error("Missing socket after discovery replacement");
      emitFencedReplay(socket);
      await f.client.waitForReplayComplete();
    } else {
      expect(f.sockets).toEqual([]);
    }
  });

  it("discards a late active snapshot from a replaced generation", async () => {
    const release = createDeferred<void>();
    let calls = 0;
    const f = await fixture({
      config: { reconnect: { baseDelayMs: 0, maxDelayMs: 0 } },
      tasks: async () => {
        calls += 1;
        if (calls === 1) {
          await release.promise;
          return { tasks: [baseTask] };
        }
        return { tasks: [] };
      },
    });
    const flow = vi.spyOn(f.client, "runTaskClaimFlow");
    const loop = f.client.runClaimableTasks({
      claimRefreshMs: 1_000,
      executor: createExecutor(),
      once: true,
      shouldClaimTask: () => true,
    });
    loop.catch(() => undefined);
    const socket = await f.open();
    emitFencedReplay(socket);
    try {
      await waitFor(() => calls === 1);
      f.client.disconnect();
      await waitFor(() => f.sockets.length === 2);
      const replacement = f.sockets[1];
      if (!replacement) throw new Error("Missing snapshot replacement socket");
      emitFencedReplay(replacement);
      await loop;
    } finally {
      release.resolve();
    }
    await flushPromises();
    expect(flow).not.toHaveBeenCalled();
    expect(calls).toBe(2);
  });

  it.each(["items", "bytes"])("includes a REST page in the combined %s budget", async (bound) => {
    const frameBytes = Buffer.byteLength(
      JSON.stringify({ event: restHistoryEvent(2), op: "event" }),
    );
    const f = await fixture({
      head: 1,
      config: {
        eventDelivery:
          bound === "items" ? { maxQueueSize: 2 } : { maxQueueBytes: 64 + frameBytes * 2 - 1 },
      },
    });
    const handled = vi.fn();
    f.client.onEvent(handled);
    const socket = await f.open();
    const rejected = expect(f.client.waitForReplayComplete()).rejects.toThrow();
    socket.emitServerEvent(restHistoryEvent(2));
    emitFencedReplay(socket);
    expect(f.client.debugInfo().pausedReason).toBeNull();
    await rejected;
    expect(f.client.debugInfo().pausedReason).toBe(
      bound === "items" ? "delivery_queue_overflow" : "delivery_byte_overflow",
    );
    expect(f.errors.at(-1)).toMatchObject({
      safeDetails:
        bound === "items"
          ? { maxQueueSize: 2, observedQueueSize: 3 }
          : { maxQueueBytes: 64 + frameBytes * 2 - 1, observedQueueBytes: 64 + frameBytes * 2 },
    });
    expect(handled).not.toHaveBeenCalled();
    expect(f.writes).toEqual([]);
  });

  it("waits for a late subscriber instead of mistaking a settled empty drain for handled history", async () => {
    const f = await fixture({ head: 1_005 });
    const socket = await f.open();
    const scanCalls = f.requests.length;
    emitFencedReplay(socket);
    await waitFor(() => f.client.debugInfo().lastReceivedSeq === 500);
    expect(f.client.debugInfo().lastHandledSeq).toBe(0);
    expect(f.requests).toHaveLength(scanCalls + 1);
    expect(f.writes).toEqual([]);
    f.client.onEvent(() => undefined);
    await f.client.waitForReplayComplete();
    expect(f.client.debugInfo().lastHandledSeq).toBe(1_005);
  });

  it("fails closed if the WebSocket cannot replay its gap after discovery", async () => {
    const f = await fixture({ head: 1 });
    const handler = vi.fn();
    f.client.onEvent(handler);
    const socket = await f.open();
    const rejected = expect(f.client.waitForReplayComplete()).rejects.toMatchObject({
      reason: "replay_window_exceeded",
    });
    socket.emit(
      "message",
      JSON.stringify({
        op: "error",
        error: "bounded replay window exceeded",
        reason: "replay_window_exceeded",
      }),
    );
    await rejected;
    expect(handler).not.toHaveBeenCalled();
    expect(f.writes).toEqual([]);
    expect(f.sockets).toHaveLength(1);
  });

  it.each([
    "reject",
    "timeout",
  ])("preserves handled-cursor recovery after a REST replay handler %s", async (failure) => {
    const f = await fixture({
      head: 1,
      config: {
        eventDelivery: { handlerTimeoutMs: 10 },
        reconnect: { baseDelayMs: 0, maxDelayMs: 0 },
      },
    });
    const loop = f.client.runClaimableTasks({
      claimRefreshMs: 1_000,
      executor: createExecutor(),
      once: false,
      shouldClaimTask: () => true,
    });
    loop.catch(() => undefined);
    const release = createDeferred<void>();
    const invocations: number[] = [];
    let inFlight = 0;
    let overlap = false;
    f.client.onEvent(async (event) => {
      invocations.push(event.seq);
      inFlight += 1;
      overlap ||= inFlight > 1;
      try {
        if (invocations.length === 1) {
          if (failure === "reject") throw new Error("synthetic handler failure");
          await release.promise;
        }
      } finally {
        inFlight -= 1;
      }
    });
    const socket = await f.open();
    emitFencedReplay(socket);
    try {
      await waitFor(() =>
        f.errors.some((error) => error.name === "ParticipantRuntimeEventDeliveryError"),
      );
      expect(f.writes).toEqual([]);
      expect(f.client.debugInfo().lastHandledSeq).toBe(0);
      if (failure === "timeout") expect(f.sockets).toHaveLength(1);
    } finally {
      release.resolve();
    }
    await waitFor(() => f.sockets.length === 2);
    const replacement = f.sockets[1];
    if (!replacement) throw new Error("Missing handler recovery socket");
    emitFencedReplay(replacement);
    await f.client.waitForReplayComplete();
    expect(invocations).toEqual([1, 1]);
    expect(overlap).toBe(false);
    expect(f.client.debugInfo()).toMatchObject({
      lastHandledSeq: 1,
      pausedReason: null,
      recoveryCount: 1,
    });
    f.client.close();
    await loop;
  });

  it("waits for actual socket closure when REST bootstrap pauses", async () => {
    const f = await fixture({ head: 1 });
    f.client.onEvent(() => undefined);
    const socket = await f.open();
    socket.deferClose = true;
    const replayRejected = expect(f.client.waitForReplayComplete()).rejects.toThrow();
    socket.emitReplayComplete();
    await replayRejected;
    let settled = false;
    const shutdown = f.client.closeAndWait().then(() => {
      settled = true;
    });
    try {
      await flushMicrotasks();
      expect(socket.readyState).toBe(WebSocket.CLOSING);
      expect(settled).toBe(false);
    } finally {
      socket.emitClose();
    }
    await shutdown;
  });

  it("keeps WebSocket replay as the default without REST discovery or snapshots", async () => {
    const f = await fixture({ config: { replayMode: undefined } });
    const socket = await f.open();
    expect(new URL(socket.url).searchParams.get("after")).toBe("0");
    const loop = f.client.runClaimableTasks({
      claimRefreshMs: 1_000,
      executor: createExecutor(),
      once: true,
      shouldClaimTask: () => true,
    });
    socket.emitReplayComplete();
    await loop;
    expect(fetch).not.toHaveBeenCalled();
  });

  it.each([
    "items",
    "bytes",
  ])("enforces unchanged combined %s bounds across held wire frames and history", async (bound) => {
    const f = await fixture({
      head: 1,
      config: {
        eventDelivery:
          bound === "items"
            ? { maxQueueSize: 2 }
            : { maxQueueBytes: 64 + Buffer.byteLength(JSON.stringify(restHistoryEvent(2))) + 10 },
      },
    });
    f.client.onEvent(() => undefined);
    const socket = await f.open();
    const replay = f.client.waitForReplayComplete();
    const rejected = expect(replay).rejects.toThrow();
    socket.emitServerEvent(restHistoryEvent(2));
    emitFencedReplay(socket);
    socket.emitServerEvent(restHistoryEvent(3));
    await rejected;
    expect(f.client.debugInfo().pausedReason).toBe(
      bound === "items" ? "delivery_queue_overflow" : "delivery_byte_overflow",
    );
    expect(f.writes).toEqual([]);
  });
});

describe("ParticipantRuntimeClient replay wait", () => {
  it("rejects replay waiters when the server emits an error before replay completion", async () => {
    const replay = createRejectableDeferred<void>();
    replay.promise.catch(() => undefined);
    const errors: Error[] = [];
    const client = Object.assign(Object.create(ParticipantRuntimeClient.prototype), {
      connectionGeneration: 0,
      emitError: (error: Error) => {
        errors.push(error);
      },
      observability: {
        debug: () => undefined,
      },
      pausedReason: null,
      pendingCommands: new Map(),
      rejectReplayComplete: replay.reject,
      replayComplete: replay.promise,
      replayCompleteSettled: false,
      resolveReplayComplete: replay.resolve,
    }) as PrivateReplayRuntimeClient;

    client.handleMessage(
      JSON.stringify({
        error: "WebSocket replay gap could not be repaired",
        op: "error",
        reason: "replay_gap_unrepaired",
      }),
    );

    await expect(client.waitForReplayComplete()).rejects.toMatchObject({
      reason: "replay_gap_unrepaired",
    });
    expect(errors).toHaveLength(1);
    expect(errors[0]).toMatchObject({ reason: "replay_gap_unrepaired" });
  });
});

describe("ParticipantRuntimeClient durable cursor", () => {
  it("captures the fenced participant epoch from replay completion", () => {
    const fixture = createCursorFixture();

    fixture.client.handleMessage(
      JSON.stringify({
        controlEpoch: 9,
        instanceId: baseConfig.instanceId,
        op: "replay.complete",
        participantId: baseConfig.participantId,
      }),
    );

    expect(fixture.client.currentControlEpoch).toBe(9);
  });

  it("keeps handled progress fixed when a synchronous event handler throws", async () => {
    const delivered: number[] = [];
    const fixture = createCursorFixture({ eventCount: 1, withHandler: false });
    fixture.client.onEvent((event) => {
      delivered.push(event.seq);
      if (event.seq === 1) {
        throw new Error("participant handler failed");
      }
    });

    emitCursorEvent(fixture.client, 1);
    emitCursorEvent(fixture.client, 2);
    await flushMicrotasks();

    expect(delivered).toEqual([1]);
    expect(fixture.writes).toEqual([]);
    expect(fixture.client.debugInfo()).toMatchObject({
      lastHandledSeq: 0,
      lastObservedSeq: 0,
      lastReceivedSeq: 2,
    });
  });

  it("keeps handled progress fixed when an asynchronous event handler rejects", async () => {
    const delivered: number[] = [];
    const fixture = createCursorFixture({ eventCount: 1, withHandler: false });
    fixture.client.onEvent(async (event) => {
      delivered.push(event.seq);
      await Promise.resolve();
      if (event.seq === 1) {
        throw new Error("async participant handler failed");
      }
    });

    emitCursorEvent(fixture.client, 1);
    emitCursorEvent(fixture.client, 2);
    await flushMicrotasks();

    expect(delivered).toEqual([1]);
    expect(fixture.writes).toEqual([]);
    expect(fixture.client.debugInfo()).toMatchObject({
      lastHandledSeq: 0,
      lastObservedSeq: 0,
      lastReceivedSeq: 2,
    });
  });

  it("does not advance an approval cursor before the awaited projection transaction commits", async () => {
    const transaction = createDeferred<void>();
    const projection = { approvalId: null as string | null, handledSeq: 0 };
    const fixture = createCursorFixture({ eventCount: 1, withHandler: false });
    fixture.client.onEvent(async (event) => {
      if (event.type !== "approval.recorded") {
        return;
      }
      await transaction.promise;
      projection.approvalId = event.eventId;
      projection.handledSeq = event.seq;
    });

    fixture.client.handleMessage(
      JSON.stringify({ event: createApprovalRecordedEvent(1), op: "event" }),
    );
    await flushMicrotasks();

    expect(projection).toEqual({ approvalId: null, handledSeq: 0 });
    expect(fixture.writes).toEqual([]);
    expect(fixture.client.debugInfo()).toMatchObject({
      lastHandledSeq: 0,
      lastObservedSeq: 0,
      lastReceivedSeq: 1,
    });

    transaction.resolve();
    await flushMicrotasks();

    expect(projection).toEqual({ approvalId: "evt_approval_recorded_1", handledSeq: 1 });
    expect(fixture.writes).toEqual([1]);
    expect(fixture.client.debugInfo()).toMatchObject({
      lastHandledSeq: 1,
      lastObservedSeq: 1,
      lastReceivedSeq: 1,
    });
  });

  it("settles participant replay after queued asynchronous delivery", async () => {
    const handler = createDeferred<void>();
    const delivered: number[] = [];
    const fixture = createCursorFixture({ withHandler: false });
    let replaySettled = false;
    const replay = fixture.client.waitForReplayComplete().then(() => {
      replaySettled = true;
    });

    emitCursorEvent(fixture.client, 1);
    fixture.client.handleMessage(JSON.stringify({ op: "replay.complete" }));
    await flushMicrotasks();
    expect(replaySettled).toBe(false);

    fixture.client.onEvent(async (event) => {
      delivered.push(event.seq);
      await handler.promise;
    });
    await flushMicrotasks();
    expect(replaySettled).toBe(false);

    handler.resolve();
    await replay;

    expect(delivered).toEqual([1]);
    expect(fixture.client.debugInfo()).toMatchObject({
      lastHandledSeq: 1,
      lastObservedSeq: 1,
      lastReceivedSeq: 1,
    });
  });

  it("adds an event to recent history only after handled delivery", async () => {
    const handler = createDeferred<void>();
    const fixture = createCursorFixture({ withHandler: false });
    fixture.client.onEvent(async () => {
      await handler.promise;
    });

    emitCursorEvent(fixture.client, 1);
    await flushMicrotasks();
    expect(fixture.client.recentEvents).toEqual([]);

    handler.resolve();
    await flushMicrotasks();

    expect(fixture.client.recentEvents.map((event) => event.seq)).toEqual([1]);
  });

  it("deduplicates replayed sequence and event identities in recent history", async () => {
    const fixture = createCursorFixture();

    emitCursorEvent(fixture.client, 1);
    await flushMicrotasks();
    fixture.client.replaceDelivery(0);
    emitCursorEvent(fixture.client, 1);
    await flushMicrotasks();

    expect(
      fixture.client.recentEvents.map((event) => ({ eventId: event.eventId, seq: event.seq })),
    ).toEqual([{ eventId: "evt_task_created_1", seq: 1 }]);
  });

  it("bounds recent handled history to 80 unique events", async () => {
    const fixture = createCursorFixture();

    for (let seq = 1; seq <= 81; seq += 1) {
      emitCursorEvent(fixture.client, seq);
    }
    await flushMicrotasks(500);

    expect(fixture.client.recentEvents).toHaveLength(80);
    expect(fixture.client.recentEvents[0]?.seq).toBe(2);
    expect(fixture.client.recentEvents.at(-1)?.seq).toBe(81);
  });

  it("applies participant handler changes after the active snapshot", async () => {
    const firstHandler = createDeferred<void>();
    const actions: string[] = [];
    const fixture = createCursorFixture({ withHandler: false });
    const unsubscribeFirst = fixture.client.onEvent(async (event) => {
      actions.push(`first:start:${event.seq}`);
      await firstHandler.promise;
      actions.push(`first:end:${event.seq}`);
    });

    emitCursorEvent(fixture.client, 1);
    await flushMicrotasks();
    unsubscribeFirst();
    fixture.client.onEvent((event) => {
      actions.push(`second:${event.seq}`);
    });
    emitCursorEvent(fixture.client, 2);
    firstHandler.resolve();
    await flushMicrotasks();

    expect(actions).toEqual(["first:start:1", "first:end:1", "second:2"]);
  });

  it("suppresses duplicate participant sequences without redelivery", async () => {
    const delivered: string[] = [];
    const fixture = createCursorFixture({ withHandler: false });
    fixture.client.onEvent((event) => {
      delivered.push(event.eventId);
    });

    emitCursorEvent(fixture.client, 1);
    await flushMicrotasks();
    fixture.client.handleMessage(
      JSON.stringify({
        event: { ...createTaskCreatedEvent(baseTask, 1), eventId: "evt_duplicate" },
        op: "event",
      }),
    );
    await flushMicrotasks();

    expect(delivered).toEqual(["evt_task_created_1"]);
    expect(fixture.client.debugInfo()).toMatchObject({
      lastHandledSeq: 1,
      lastReceivedSeq: 1,
    });
  });

  it("halts participant delivery before a sequence gap advances cursors", async () => {
    const delivered: number[] = [];
    const fixture = createCursorFixture({ withHandler: false });
    fixture.client.onEvent((event) => {
      delivered.push(event.seq);
    });

    emitCursorEvent(fixture.client, 2);
    emitCursorEvent(fixture.client, 1);
    await flushMicrotasks();

    expect(delivered).toEqual([]);
    expect(fixture.errors).toHaveLength(1);
    expect(fixture.errors[0]).toMatchObject({ reason: "non_contiguous_event" });
    expect(fixture.client.debugInfo()).toMatchObject({
      lastHandledSeq: 0,
      lastReceivedSeq: 0,
    });
  });

  it("halts participant delivery after an invalid event envelope", async () => {
    const delivered: number[] = [];
    const fixture = createCursorFixture({ withHandler: false });
    fixture.client.onEvent((event) => {
      delivered.push(event.seq);
    });

    fixture.client.handleMessage(
      JSON.stringify({
        event: { eventId: "evt_invalid", seq: 1 },
        op: "event",
      }),
    );
    emitCursorEvent(fixture.client, 1);
    await flushMicrotasks();

    expect(delivered).toEqual([]);
    expect(fixture.errors).toHaveLength(1);
    expect(fixture.errors[0]).toMatchObject({ reason: "invalid_server_envelope" });
    expect(fixture.client.debugInfo()).toMatchObject({
      lastHandledSeq: 0,
      lastReceivedSeq: 0,
    });
  });

  it("skips unknown-op server frames without pausing participant delivery", async () => {
    const delivered: number[] = [];
    const fixture = createCursorFixture({ withHandler: false });
    fixture.client.onEvent((event) => {
      delivered.push(event.seq);
    });

    fixture.client.handleMessage(JSON.stringify({ op: "presence.v2", payload: { future: true } }));
    emitCursorEvent(fixture.client, 1);
    await flushMicrotasks();

    expect(delivered).toEqual([1]);
    expect(fixture.errors).toEqual([]);
    expect(fixture.client.debugInfo()).toMatchObject({
      lastHandledSeq: 1,
      lastReceivedSeq: 1,
      pausedReason: null,
    });
  });

  it("exposes participant received, handled, queue, and active diagnostics", async () => {
    const handler = createDeferred<void>();
    const fixture = createCursorFixture({ withHandler: false });
    fixture.client.onEvent(async () => {
      await handler.promise;
    });

    emitCursorEvent(fixture.client, 1);
    await flushMicrotasks();

    expect(fixture.client.debugInfo()).toMatchObject({
      activeDeliverySeq: 1,
      eventBacklogSize: 1,
      lastHandledSeq: 0,
      lastObservedSeq: 0,
      lastReceivedSeq: 1,
    });

    handler.resolve();
    await flushMicrotasks();

    expect(fixture.client.debugInfo()).toMatchObject({
      activeDeliverySeq: null,
      eventBacklogSize: 0,
      lastHandledSeq: 1,
      lastObservedSeq: 1,
      lastReceivedSeq: 1,
    });
  });

  it("uses a non-null durable cursor even when it is below afterSeq", () => {
    expect(resolveResumeSeq(9_550, 10_928)).toBe(10_928);
    expect(resolveResumeSeq(9_550, 100)).toBe(100);
    expect(resolveResumeSeq(9_550, null)).toBe(9_550);
    expect(resolveResumeSeq(9_550, Number.NaN)).toBe(9_550);
  });

  it("resumes from the stored cursor when it is ahead of afterSeq", async () => {
    const fixture = createCursorFixture({ afterSeq: 9_550, stored: 10_928 });

    await expect(fixture.client.resolveInitialResumeSeq()).resolves.toBe(10_928);
  });

  it("resumes from a stored cursor below the afterSeq hint", async () => {
    const fixture = createCursorFixture({ afterSeq: 9_550, stored: 100 });

    await expect(fixture.client.resolveInitialResumeSeq()).resolves.toBe(100);
  });

  it("resumes from afterSeq when no cursor is stored yet", async () => {
    const fixture = createCursorFixture({ afterSeq: 9_550, stored: null });

    await expect(fixture.client.resolveInitialResumeSeq()).resolves.toBe(9_550);
  });

  it("advances and persists the cursor as handled events accumulate, throttled by event count", async () => {
    const fixture = createCursorFixture({ eventCount: 3 });

    emitCursorEvent(fixture.client, 1);
    emitCursorEvent(fixture.client, 2);
    emitCursorEvent(fixture.client, 3);
    await flushMicrotasks();

    expect(fixture.writes).toEqual([3]);

    emitCursorEvent(fixture.client, 4);
    emitCursorEvent(fixture.client, 5);
    await flushMicrotasks();

    expect(fixture.writes).toEqual([3]);
    expect(fixture.client.debugInfo().lastObservedSeq).toBe(5);
  });

  it("retains failed durable progress and retries it when later handling advances", async () => {
    let available = false;
    const writes: number[] = [];
    const fixture = createCursorFixture({
      cursorStore: {
        read: () => null,
        write: (seq) => {
          writes.push(seq);
          if (!available) {
            throw new Error("store unavailable: private connection detail");
          }
        },
      },
      eventCount: 1,
    });

    emitCursorEvent(fixture.client, 1);
    await flushMicrotasks();

    expect(fixture.client.debugInfo()).toMatchObject({
      cursorPersistFailureCount: 1,
      lastHandledSeq: 1,
      lastPersistedSeq: 0,
      pendingCursorSeq: 1,
    });
    expect(fixture.errors).toHaveLength(1);
    expect(fixture.errors[0]).toBeInstanceOf(ParticipantRuntimeCursorPersistError);
    expect(fixture.errors[0]?.message).not.toContain("private connection detail");

    available = true;
    emitCursorEvent(fixture.client, 2);
    await flushMicrotasks();

    expect(writes).toEqual([1, 2]);
    expect(fixture.client.debugInfo()).toMatchObject({
      lastHandledSeq: 2,
      lastPersistedSeq: 2,
      pendingCursorSeq: null,
    });
  });

  it.each([
    { configuredTimeoutMs: undefined, expectedTimeoutMs: 5_000 },
    { configuredTimeoutMs: 25, expectedTimeoutMs: 25 },
  ])("applies a $expectedTimeoutMs ms durable cursor write deadline", async ({
    configuredTimeoutMs,
    expectedTimeoutMs,
  }) => {
    vi.useFakeTimers();
    try {
      const sockets: ParticipantFakeWebSocket[] = [];
      const errors: Error[] = [];
      const client = await ParticipantRuntimeClient.connect({
        ...baseConfig,
        cursorPersist: {
          eventCount: 1,
          retryAttempts: 1,
          ...(configuredTimeoutMs === undefined ? {} : { writeTimeoutMs: configuredTimeoutMs }),
        },
        cursorStore: {
          read: () => null,
          write: () => new Promise<void>(() => undefined),
        },
        webSocketFactory: createParticipantWebSocketFactory(sockets),
      });
      client.onError((error) => errors.push(error));
      client.onEvent(() => undefined);
      sockets[0]?.emitServerEvent(createTaskCreatedEvent(baseTask, 1));
      await flushMicrotasks();

      await vi.advanceTimersByTimeAsync(expectedTimeoutMs - 1);
      expect(errors).toEqual([]);
      await vi.advanceTimersByTimeAsync(1);

      expect(errors).toHaveLength(1);
      expect(errors[0]).toMatchObject({
        attempts: 1,
        reason: "write_timeout",
        writeTimeoutMs: expectedTimeoutMs,
      });
      expect(client.debugInfo()).toMatchObject({
        cursorPersistFailureCount: 1,
        lastPersistedSeq: 0,
        pendingCursorSeq: 1,
      });
    } finally {
      vi.useRealTimers();
    }
  });

  it.each([
    {
      cursorPersist: { eventCount: 1 },
      expectedAttempts: 5,
      totalBackoffMs: 1_500,
    },
    {
      cursorPersist: {
        eventCount: 1,
        retryAttempts: 3,
        retryBaseDelayMs: 10,
        retryMaxDelayMs: 15,
      },
      expectedAttempts: 3,
      totalBackoffMs: 25,
    },
    {
      cursorPersist: { eventCount: 1, retryAttempts: 7 },
      expectedAttempts: 7,
      totalBackoffMs: 5_100,
    },
  ])("runs $expectedAttempts cursor attempts across $totalBackoffMs ms of bounded backoff", async ({
    cursorPersist,
    expectedAttempts,
    totalBackoffMs,
  }) => {
    vi.useFakeTimers();
    try {
      const sockets: ParticipantFakeWebSocket[] = [];
      const errors: Error[] = [];
      let writes = 0;
      const client = await ParticipantRuntimeClient.connect({
        ...baseConfig,
        cursorPersist,
        cursorStore: {
          read: () => null,
          write: () => {
            writes += 1;
            return Promise.reject(new Error("offline"));
          },
        },
        webSocketFactory: createParticipantWebSocketFactory(sockets),
      });
      client.onError((error) => errors.push(error));
      client.onEvent(() => undefined);
      sockets[0]?.emitServerEvent(createTaskCreatedEvent(baseTask, 1));
      await flushMicrotasks();
      expect(writes).toBe(1);

      await vi.advanceTimersByTimeAsync(totalBackoffMs - 1);
      expect(writes).toBe(expectedAttempts - 1);
      expect(errors).toEqual([]);
      await vi.advanceTimersByTimeAsync(1);

      expect(writes).toBe(expectedAttempts);
      expect(errors).toHaveLength(1);
      expect(errors[0]).toMatchObject({ attempts: expectedAttempts, reason: "write_failed" });
    } finally {
      vi.useRealTimers();
    }
  });

  it("does not advance the persisted cursor for events buffered in the backlog without a handler", () => {
    const fixture = createCursorFixture({ eventCount: 1, withHandler: false });

    emitCursorEvent(fixture.client, 1);
    emitCursorEvent(fixture.client, 2);

    expect(fixture.writes).toEqual([]);
    expect(fixture.client.debugInfo()).toMatchObject({
      lastObservedSeq: 0,
      lastReceivedSeq: 2,
    });
    expect(fixture.client.debugInfo().eventBacklogSize).toBe(2);

    fixture.client.close();

    expect(fixture.writes).toEqual([]);
    expect(fixture.errors).toEqual([]);
  });

  it("persists the backlog high-water mark only after a late handler drains it", async () => {
    const fixture = createCursorFixture({ eventCount: 1, withHandler: false });

    emitCursorEvent(fixture.client, 1);
    emitCursorEvent(fixture.client, 2);

    expect(fixture.writes).toEqual([]);

    fixture.client.onEvent(() => undefined);
    await flushMicrotasks();

    expect(fixture.writes).toEqual([1, 2]);
  });

  it("persists at most the last handled sequence for a handler that processes through seq N", async () => {
    const fixture = createCursorFixture({ afterSeq: 3, eventCount: 1 });

    emitCursorEvent(fixture.client, 4);
    await flushMicrotasks();

    expect(fixture.writes).toEqual([4]);

    emitCursorEvent(fixture.client, 5);
    await flushMicrotasks();

    expect(fixture.writes).toEqual([4, 5]);
  });

  it("flushes the latest handled cursor on graceful close", async () => {
    const fixture = createCursorFixture({ eventCount: 3 });

    emitCursorEvent(fixture.client, 1);
    emitCursorEvent(fixture.client, 2);
    emitCursorEvent(fixture.client, 3);
    emitCursorEvent(fixture.client, 4);
    await flushMicrotasks();
    fixture.client.close();

    expect(fixture.writes).toEqual([3, 4]);

    fixture.client.close();
    expect(fixture.writes).toEqual([3, 4]);
    expect(fixture.errors).toEqual([]);
  });

  it("waits for active delivery, final cursor acknowledgement, and socket closure", async () => {
    const sockets: ParticipantFakeWebSocket[] = [];
    const handler = createDeferred<void>();
    const handlerEntered = createDeferred<void>();
    const write = createDeferred<void>();
    const writes: number[] = [];
    const client = await ParticipantRuntimeClient.connect({
      ...baseConfig,
      cursorPersist: { eventCount: 1 },
      cursorStore: {
        read: () => null,
        write: async (seq) => {
          writes.push(seq);
          await write.promise;
        },
      },
      webSocketFactory: createParticipantWebSocketFactory(sockets),
    });
    client.onEvent(async () => {
      handlerEntered.resolve();
      await handler.promise;
    });
    const socket = sockets[0];
    if (!socket) {
      throw new Error("Expected participant socket");
    }
    socket.deferClose = true;
    socket.emitServerEvent(createTaskCreatedEvent(baseTask, 1));
    await handlerEntered.promise;

    let closed = false;
    const close = client.closeAndWait().then(() => {
      closed = true;
    });
    await flushMicrotasks();
    expect(closed).toBe(false);
    expect(writes).toEqual([]);

    handler.resolve();
    await waitForMicrotasks(() => writes.length === 1);
    expect(writes).toEqual([1]);
    expect(closed).toBe(false);

    write.resolve();
    await flushMicrotasks();
    expect(closed).toBe(false);
    socket.emitClose();
    await close;

    expect(client.debugInfo()).toMatchObject({ lastHandledSeq: 1, lastPersistedSeq: 1 });
  });

  it("starts a final bounded retry cycle for pending cursor progress", async () => {
    const sockets: ParticipantFakeWebSocket[] = [];
    const errors: Error[] = [];
    const writes: number[] = [];
    let available = false;
    const client = await ParticipantRuntimeClient.connect({
      ...baseConfig,
      cursorPersist: { eventCount: 1, retryAttempts: 1 },
      cursorStore: {
        read: () => null,
        write: (seq) => {
          writes.push(seq);
          if (!available) {
            throw new Error("offline");
          }
        },
      },
      webSocketFactory: createParticipantWebSocketFactory(sockets),
    });
    client.onError((error) => errors.push(error));
    client.onEvent(() => undefined);
    sockets[0]?.emitServerEvent(createTaskCreatedEvent(baseTask, 1));
    await waitForMicrotasks(() => errors.length === 1);
    expect(client.debugInfo()).toMatchObject({ lastPersistedSeq: 0, pendingCursorSeq: 1 });

    available = true;
    await client.closeAndWait();

    expect(writes).toEqual([1, 1]);
    expect(client.debugInfo()).toMatchObject({ lastPersistedSeq: 1, pendingCursorSeq: null });
  });

  it("rejects graceful shutdown when final cursor progress remains unacknowledged", async () => {
    const sockets: ParticipantFakeWebSocket[] = [];
    const errors: Error[] = [];
    const client = await ParticipantRuntimeClient.connect({
      ...baseConfig,
      cursorPersist: { eventCount: 1, retryAttempts: 1 },
      cursorStore: {
        read: () => null,
        write: () => Promise.reject(new Error("offline")),
      },
      webSocketFactory: createParticipantWebSocketFactory(sockets),
    });
    client.onError((error) => errors.push(error));
    client.onEvent(() => undefined);
    sockets[0]?.emitServerEvent(createTaskCreatedEvent(baseTask, 1));
    await waitForMicrotasks(() => errors.length === 1);

    await expect(client.closeAndWait()).rejects.toMatchObject({
      pendingPhases: ["cursor"],
      reason: "incomplete",
      timeoutMs: 30_000,
    });
    expect(client.debugInfo()).toMatchObject({ lastPersistedSeq: 0, pendingCursorSeq: 1 });
  });

  it.each([
    { configuredTimeoutMs: undefined, expectedTimeoutMs: 30_000 },
    { configuredTimeoutMs: 25, expectedTimeoutMs: 25 },
  ])("rejects incomplete graceful shutdown after $expectedTimeoutMs ms", async ({
    configuredTimeoutMs,
    expectedTimeoutMs,
  }) => {
    vi.useFakeTimers();
    try {
      const sockets: ParticipantFakeWebSocket[] = [];
      const client = await ParticipantRuntimeClient.connect({
        ...baseConfig,
        ...(configuredTimeoutMs === undefined ? {} : { shutdownTimeoutMs: configuredTimeoutMs }),
        webSocketFactory: createParticipantWebSocketFactory(sockets),
      });
      client.onEvent(() => new Promise<void>(() => undefined));
      const socket = sockets[0];
      if (!socket) {
        throw new Error("Expected participant socket");
      }
      socket.deferClose = true;
      socket.emitServerEvent(createTaskCreatedEvent(baseTask, 1));
      await flushMicrotasks();

      const close = client.closeAndWait();
      await vi.advanceTimersByTimeAsync(expectedTimeoutMs - 1);
      let settled = false;
      void close.catch(() => {
        settled = true;
      });
      await flushMicrotasks();
      expect(settled).toBe(false);
      await vi.advanceTimersByTimeAsync(1);

      await expect(close).rejects.toMatchObject({
        pendingPhases: expect.arrayContaining(["socket"]),
        reason: "timeout",
        timeoutMs: expectedTimeoutMs,
      });
      await expect(close).rejects.toBeInstanceOf(ParticipantRuntimeShutdownError);
    } finally {
      vi.useRealTimers();
    }
  });

  it("resumes a restart from the persisted cursor instead of the static afterSeq", async () => {
    const persisted: { seq: number | null } = { seq: null };
    const cursorStore: ParticipantRuntimeCursorStore = {
      read: () => persisted.seq,
      write: (seq) => {
        persisted.seq = seq;
      },
    };
    const firstRun = createCursorFixture({ afterSeq: 9_550, cursorStore, eventCount: 1 });
    emitCursorEvent(firstRun.client, 9_551);
    emitCursorEvent(firstRun.client, 9_552);
    await flushMicrotasks();
    firstRun.client.close();

    const restart = createCursorFixture({ afterSeq: 9_550, cursorStore });

    expect(persisted.seq).toBe(9_552);
    await expect(restart.client.resolveInitialResumeSeq()).resolves.toBe(9_552);
  });

  it("does not persist or fail when no cursor store is configured", async () => {
    const fixture = createCursorFixture({ afterSeq: 9_550, withStore: false });

    emitCursorEvent(fixture.client, 9_551);
    emitCursorEvent(fixture.client, 9_552);
    await flushMicrotasks();
    fixture.client.close();

    expect(fixture.writes).toEqual([]);
    expect(fixture.errors).toEqual([]);
    expect(fixture.client.debugInfo().lastObservedSeq).toBe(9_552);
    await expect(fixture.client.resolveInitialResumeSeq()).resolves.toBe(9_550);
  });
});
