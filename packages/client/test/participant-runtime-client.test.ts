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
  type ParticipantRuntimeCursorStore,
  type ParticipantTaskExecutor,
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

type RunTaskClaimFlowInput = Parameters<ParticipantRuntimeClient["runTaskClaimFlow"]>[0];

type PrivateReconnectRuntimeClient = ParticipantRuntimeClient & {
  readonly reconnectFailureCount: number;
  readonly reconnectSuccessCount: number;
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
  handleMessage(data: string): void;
};

type PrivateCursorRuntimeClient = ParticipantRuntimeClient & {
  handleMessage(data: string): void;
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
  readonly client: ParticipantRuntimeClient;
  readonly closeTransport: () => void;
  readonly completeReplay: () => void;
  readonly emit: (event: SessionEvent) => void;
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
    runClaimableTasks: async () => {
      await options.onRunClaimableTasks?.();
    },
    waitForReplayComplete: async () => {
      await options.onWaitForReplayComplete?.();
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

  it("installs a structured stderr error handler by default", async () => {
    const stderrWrite = vi.spyOn(process.stderr, "write").mockImplementation(() => true);
    let runtime: RunParticipantRuntimeFixture;
    runtime = createRunParticipantRuntimeFixture({
      onRunClaimableTasks: () => runtime.emitError(new Error("stream failed")),
    });
    vi.spyOn(ParticipantRuntimeClient, "connect").mockResolvedValue(runtime.client);

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
    vi.spyOn(ParticipantRuntimeClient, "connect").mockResolvedValue(runtime.client);

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
    vi.spyOn(ParticipantRuntimeClient, "connect").mockResolvedValue(runtime.client);

    await runParticipantRuntimeForTest({ hooks: { onError: null } });

    expect(stderrWrite).not.toHaveBeenCalled();
  });

  it("waits for replay before running replay hooks and the claim loop", async () => {
    const actions: string[] = [];
    const runtime = createRunParticipantRuntimeFixture({
      onRunClaimableTasks: () => actions.push("claim-loop"),
      onWaitForReplayComplete: () => actions.push("replay"),
    });
    vi.spyOn(ParticipantRuntimeClient, "connect").mockResolvedValue(runtime.client);

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
    vi.spyOn(ParticipantRuntimeClient, "connect").mockResolvedValue(runtime.client);

    await runParticipantRuntimeForTest({
      hooks: {
        onReplayComplete: () => () => {
          actions.push("cleanup");
        },
      },
    });

    expect(actions).toEqual(["claim-loop", "cleanup"]);
  });
});

describe("ParticipantRuntimeClient.runTaskClaimFlow", () => {
  afterEach(() => {
    vi.useRealTimers();
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
  it("uses bounded reconnect backoff and reopens from the last observed event sequence", async () => {
    const openedAfterSeqs: number[] = [];
    const errors: Error[] = [];
    const client = Object.assign(Object.create(ParticipantRuntimeClient.prototype), {
      config: { ...baseConfig, reconnect: { baseDelayMs: 1, maxDelayMs: 2 } },
      errorHandlers: new Set<(error: Error) => void>([(error) => errors.push(error)]),
      lastObservedSeq: 42,
      open: async (afterSeq: number) => {
        openedAfterSeqs.push(afterSeq);
        if (openedAfterSeqs.length < 3) {
          throw new Error(`Reconnect attempt ${openedAfterSeqs.length} failed`);
        }
      },
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
    const errors: string[] = [];
    const client = Object.assign(Object.create(ParticipantRuntimeClient.prototype), {
      emitError: (error: Error) => {
        errors.push(error.message);
      },
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

    await expect(client.waitForReplayComplete()).rejects.toThrow(
      "WebSocket replay gap could not be repaired",
    );
    expect(errors).toEqual(["WebSocket replay gap could not be repaired"]);
  });
});

describe("ParticipantRuntimeClient durable cursor", () => {
  it("resolves the resume point as the higher of afterSeq floor and stored cursor", () => {
    expect(resolveResumeSeq(9_550, 10_928)).toBe(10_928);
    expect(resolveResumeSeq(9_550, 100)).toBe(9_550);
    expect(resolveResumeSeq(9_550, null)).toBe(9_550);
    expect(resolveResumeSeq(9_550, Number.NaN)).toBe(9_550);
  });

  it("resumes from the stored cursor when it is ahead of afterSeq", async () => {
    const fixture = createCursorFixture({ afterSeq: 9_550, stored: 10_928 });

    await expect(fixture.client.resolveInitialResumeSeq()).resolves.toBe(10_928);
  });

  it("never resumes below the afterSeq floor", async () => {
    const fixture = createCursorFixture({ afterSeq: 9_550, stored: 100 });

    await expect(fixture.client.resolveInitialResumeSeq()).resolves.toBe(9_550);
  });

  it("resumes from afterSeq when no cursor is stored yet", async () => {
    const fixture = createCursorFixture({ afterSeq: 9_550, stored: null });

    await expect(fixture.client.resolveInitialResumeSeq()).resolves.toBe(9_550);
  });

  it("advances and persists the cursor as handled events accumulate, throttled by event count", () => {
    const fixture = createCursorFixture({ eventCount: 3 });

    emitCursorEvent(fixture.client, 1);
    emitCursorEvent(fixture.client, 2);
    emitCursorEvent(fixture.client, 3);

    expect(fixture.writes).toEqual([3]);

    emitCursorEvent(fixture.client, 4);
    emitCursorEvent(fixture.client, 5);

    expect(fixture.writes).toEqual([3]);
    expect(fixture.client.debugInfo().lastObservedSeq).toBe(5);
  });

  it("does not advance the persisted cursor for events buffered in the backlog without a handler", () => {
    const fixture = createCursorFixture({ eventCount: 1, withHandler: false });

    emitCursorEvent(fixture.client, 7);
    emitCursorEvent(fixture.client, 8);

    expect(fixture.writes).toEqual([]);
    expect(fixture.client.debugInfo().lastObservedSeq).toBe(8);
    expect(fixture.client.debugInfo().eventBacklogSize).toBe(2);

    fixture.client.close();

    expect(fixture.writes).toEqual([]);
    expect(fixture.errors).toEqual([]);
  });

  it("persists the backlog high-water mark only after a late handler drains it", () => {
    const fixture = createCursorFixture({ eventCount: 1, withHandler: false });

    emitCursorEvent(fixture.client, 7);
    emitCursorEvent(fixture.client, 8);

    expect(fixture.writes).toEqual([]);

    fixture.client.onEvent(() => undefined);

    expect(fixture.writes).toEqual([8]);
  });

  it("persists at most the last handled sequence for a handler that processes through seq N", () => {
    const fixture = createCursorFixture({ eventCount: 1 });

    emitCursorEvent(fixture.client, 4);

    expect(fixture.writes).toEqual([4]);

    emitCursorEvent(fixture.client, 9);

    expect(fixture.writes).toEqual([4, 9]);
  });

  it("flushes the latest handled cursor on graceful close", () => {
    const fixture = createCursorFixture({ eventCount: 3 });

    emitCursorEvent(fixture.client, 1);
    emitCursorEvent(fixture.client, 2);
    emitCursorEvent(fixture.client, 3);
    emitCursorEvent(fixture.client, 4);
    fixture.client.close();

    expect(fixture.writes).toEqual([3, 4]);

    fixture.client.close();
    expect(fixture.writes).toEqual([3, 4]);
    expect(fixture.errors).toEqual([]);
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
    emitCursorEvent(firstRun.client, 9_800);
    emitCursorEvent(firstRun.client, 10_050);
    firstRun.client.close();

    const restart = createCursorFixture({ afterSeq: 9_550, cursorStore });

    expect(persisted.seq).toBe(10_050);
    await expect(restart.client.resolveInitialResumeSeq()).resolves.toBe(10_050);
  });

  it("does not persist or fail when no cursor store is configured", async () => {
    const fixture = createCursorFixture({ afterSeq: 9_550, withStore: false });

    emitCursorEvent(fixture.client, 9_600);
    emitCursorEvent(fixture.client, 9_601);
    fixture.client.close();

    expect(fixture.writes).toEqual([]);
    expect(fixture.errors).toEqual([]);
    expect(fixture.client.debugInfo().lastObservedSeq).toBe(9_601);
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
  } = {},
): RuntimeClientFixture {
  let appendCount = 0;
  let claimedTask: TaskRecord | null = baseTask;
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
    lastObservedSeq: Number.MAX_SAFE_INTEGER,
    recentEvents: [],
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
  let replay = createDeferred<void>();
  let close = createDeferred<void>();
  let reconnectAttempts = 0;
  const client = Object.assign(Object.create(ParticipantRuntimeClient.prototype), {
    close: () => {
      client.stopped = true;
      actions.push("close");
    },
    onEvent: (handler: (event: SessionEvent) => void) => {
      handlers.add(handler);
      return () => {
        handlers.delete(handler);
        actions.push("unsubscribe");
      };
    },
    reconnectWithBackoff: async () => {
      reconnectAttempts += 1;
      actions.push("reconnect");
      replay = createDeferred<void>();
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
  }) as unknown as ParticipantRuntimeClient & { stopped: boolean };

  return {
    actions,
    client,
    closeTransport: () => close.resolve(),
    completeReplay: () => replay.resolve(),
    emit: (event) => {
      for (const handler of handlers) {
        handler(event);
      }
    },
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
  const client = Object.assign(Object.create(ParticipantRuntimeClient.prototype), {
    config: { ...baseConfig, afterSeq, cursorStore },
    cursorPersistEventCount: options.eventCount ?? 50,
    cursorPersistIntervalMs: options.intervalMs ?? 1_000,
    cursorPersistTimer: null,
    errorHandlers: new Set<(error: Error) => void>([(error) => errors.push(error)]),
    eventBacklog: [],
    eventHandlers,
    eventsSinceCursorPersist: 0,
    lastHandledSeq: afterSeq,
    lastObservedSeq: afterSeq,
    lastPersistedSeq: afterSeq,
    observability: {
      debugInfo: () => ({}),
    },
    pendingCommands: new Map(),
    reconnectFailureCount: 0,
    reconnectSuccessCount: 0,
    recentEvents: [],
    socket: null,
    stopped: false,
  }) as PrivateCursorRuntimeClient;
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
