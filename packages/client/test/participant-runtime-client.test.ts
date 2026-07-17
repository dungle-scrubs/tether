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
        mailboxScope: { accountId: "acct_opaque_1", provider: "fastmail" },
        scheduleWindow: {
          algorithmVersion: 1,
          endMs: 1_700_002_800_000,
          intervalMs: 3_600_000,
          startMs: 1_699_999_200_000,
        },
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
      client.close();
    }
  });
});

describe("ParticipantRuntimeClient command correlation", () => {
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

/** Creates a task executor that can trigger an optional side effect before completing. */
function createExecutor(beforeReturn: () => void = () => undefined): ParticipantTaskExecutor {
  return async () => {
    beforeReturn();
    return { result: { ok: true } };
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

/** Creates a manually controlled promise for async loop tests. */
function createDeferred<TValue>(): Deferred<TValue> {
  let resolve: (value: TValue | PromiseLike<TValue>) => void = () => undefined;
  const promise = new Promise<TValue>((promiseResolve) => {
    resolve = promiseResolve;
  });
  return { promise, resolve };
}

function createRejectableDeferred<TValue>(): RejectableDeferred<TValue> {
  let reject: (error: Error) => void = () => undefined;
  let resolve: (value: TValue | PromiseLike<TValue>) => void = () => undefined;
  const promise = new Promise<TValue>((promiseResolve, promiseReject) => {
    reject = promiseReject;
    resolve = promiseResolve;
  });
  return { promise, reject, resolve };
}

interface Deferred<TValue> {
  readonly promise: Promise<TValue>;
  readonly resolve: (value?: TValue | PromiseLike<TValue>) => void;
}

interface RejectableDeferred<TValue> extends Deferred<TValue> {
  readonly reject: (error: Error) => void;
}

/** In-memory participant WebSocket with controllable server frames and closure. */
class ParticipantFakeWebSocket extends EventEmitter {
  deferClose = false;
  readyState = WebSocket.CONNECTING;
  readonly sent: string[] = [];

  constructor(readonly url: string) {
    super();
    queueMicrotask(() => {
      this.readyState = WebSocket.OPEN;
      this.emit("open");
    });
  }

  close(): void {
    if (this.deferClose) {
      this.readyState = WebSocket.CLOSING;
      return;
    }
    this.emitClose();
  }

  emitClose(): void {
    this.readyState = WebSocket.CLOSED;
    this.emit("close");
  }

  emitReplayComplete(): void {
    this.emit("message", JSON.stringify({ op: "replay.complete" }));
  }

  emitServerEvent(event: SessionEvent): void {
    this.emit("message", JSON.stringify({ event, op: "event" }));
  }

  send(data: string): void {
    this.sent.push(data);
  }

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
