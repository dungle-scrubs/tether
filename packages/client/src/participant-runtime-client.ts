import { randomUUID } from "node:crypto";

import { Effect, Either } from "effect";
import WebSocket from "ws";

import {
  type BoundaryDebugInfo,
  ModuleObservability,
  readModuleObservabilityOptions,
} from "./observability.js";
import { resolveServiceAuthToken } from "./auth-token.js";
import {
  type AppendSessionEventInput,
  buildWsPublishMessage,
  buildWsTaskClaimMessage,
  buildWsTaskCompleteMessage,
  buildWsTaskFailMessage,
  buildWsTaskRefreshMessage,
  type CommandResultEnvelope,
  parseWebSocketServerEnvelope,
  type WebSocketCommandMessage,
  webSocketOperation,
} from "./protocol.js";
import {
  buildParticipantClaimableTaskLoop,
  type TaskCancellationContext,
} from "./participant-claimable-task-runner.js";
import { runParticipantTaskClaimFlow } from "./participant-task-claim-flow.js";
import { sleepUnrefEffect } from "./effect-timing.js";
import type { ParticipantRuntimeKind, SessionEvent, TaskRecord } from "./types.js";

const defaultReconnectBaseDelayMs = 100;
const defaultReconnectMaxDelayMs = 2_000;
const defaultCommandTimeoutMs = 15_000;
const defaultCursorPersistIntervalMs = 1_000;
const defaultCursorPersistEventCount = 50;

interface PendingCommand {
  readonly op: string;
  readonly reject: (error: Error) => void;
  readonly requestId: string;
  readonly resolve: (result: CommandResultEnvelope) => void;
  readonly taskId?: string;
  readonly timeout: ReturnType<typeof setTimeout>;
}

interface CommandContext {
  readonly op: string;
  readonly requestId: string;
  readonly taskId?: string;
}

/**
 * Error raised when participant runtime client configuration is invalid.
 */
export class ParticipantRuntimeClientConfigurationError extends Error {
  /** Machine-readable configuration field that failed validation. */
  readonly field: string;

  constructor(field: string, message: string) {
    super(message);
    this.name = "ParticipantRuntimeClientConfigurationError";
    this.field = field;
  }
}

/**
 * Error raised when the server returns a correlated command error envelope.
 */
export class ParticipantRuntimeCommandError extends Error {
  /** WebSocket command operation that rejected. */
  readonly op: string;
  /** Number of pending commands observed when the rejection was handled. */
  readonly pendingCommandCount: number;
  /** Correlation id attached to the command request. */
  readonly requestId: string;
  /** Task id carried by task-scoped commands when present. */
  readonly taskId?: string;

  constructor(input: {
    readonly message: string;
    readonly op: string;
    readonly pendingCommandCount: number;
    readonly requestId: string;
    readonly taskId?: string;
  }) {
    super(input.message);
    this.name = "ParticipantRuntimeCommandError";
    this.op = input.op;
    this.pendingCommandCount = input.pendingCommandCount;
    this.requestId = input.requestId;
    if (input.taskId !== undefined) {
      this.taskId = input.taskId;
    }
  }
}

/**
 * Error raised when a correlated WebSocket command does not receive a response
 * before the configured command timeout.
 */
export class ParticipantRuntimeCommandTimeoutError extends Error {
  /** WebSocket command operation that timed out. */
  readonly op: string;
  /** Number of pending commands observed when the timeout fired. */
  readonly pendingCommandCount: number;
  /** Correlation id attached to the command request. */
  readonly requestId: string;
  /** Task id carried by task-scoped commands when present. */
  readonly taskId?: string;
  /** Command response timeout in milliseconds. */
  readonly timeoutMs: number;

  constructor(input: {
    readonly op: string;
    readonly pendingCommandCount: number;
    readonly requestId: string;
    readonly taskId?: string;
    readonly timeoutMs: number;
  }) {
    super(`Timed out waiting for ${input.op} command response`);
    this.name = "ParticipantRuntimeCommandTimeoutError";
    this.op = input.op;
    this.pendingCommandCount = input.pendingCommandCount;
    this.requestId = input.requestId;
    if (input.taskId !== undefined) {
      this.taskId = input.taskId;
    }
    this.timeoutMs = input.timeoutMs;
  }
}

/**
 * Result returned by an adapter-specific task executor after task work finishes.
 */
export interface ParticipantTaskExecutorResult {
  /** Optional user-visible output event to publish before completion. */
  readonly output?: string | null;
  /** Structured completion payload stored on the durable task row. */
  readonly result: Record<string, unknown>;
}

/**
 * Runtime helpers and claimed task state passed to adapter-specific executors.
 */
export interface ParticipantTaskExecutorContext {
  /** Concrete runtime process id that owns the task claim. */
  readonly instanceId: string;
  /** Stable participant identity that owns the task claim. */
  readonly participantId: string;
  /** Bounded session events observed before this task executor started. */
  readonly recentEvents: readonly SessionEvent[];
  /** Publishes an additional user-visible output event for the active task. */
  readonly publishOutput: (output: string) => Promise<void>;
  /** Publishes an additional progress event for the active task. */
  readonly publishProgress: () => Promise<void>;
  /** Cancellation signal aborted when Tether cancels the active task or its claim is lost. */
  readonly signal: AbortSignal;
  /** Durable session that owns the task. */
  readonly sessionId: string;
  /** Claimed task record returned by Tether after this participant won the claim. */
  readonly task: TaskRecord;
}

/**
 * Executes a claimed task and returns the durable completion payload.
 */
export type ParticipantTaskExecutor = (
  context: ParticipantTaskExecutorContext,
) => Promise<ParticipantTaskExecutorResult>;

/**
 * Decides whether a participant runtime should attempt a claim for a task.
 */
export type ParticipantTaskSelector = (task: TaskRecord) => boolean;

/**
 * Durable resume cursor for a participant runtime. When supplied, the runtime
 * resumes from the persisted sequence instead of replaying a fixed window from
 * `afterSeq` on every process start. `read` runs once at startup; `write` runs
 * throttled as events are handled and once more on graceful close.
 */
export interface ParticipantRuntimeCursorStore {
  /** Returns the last persisted event sequence, or null when none is stored. */
  read(): number | null | Promise<number | null>;
  /** Persists the highest event sequence the runtime has confirmed as handled. */
  write(seq: number): void | Promise<void>;
}

/**
 * Throttle policy for durable cursor writes. Persists at most once per
 * `intervalMs`, or immediately once `eventCount` events have been observed
 * since the last write.
 */
export interface ParticipantRuntimeCursorPersistPolicy {
  /** Maximum observed events between writes before forcing a persist. */
  readonly eventCount?: number;
  /** Minimum delay between throttled cursor writes in milliseconds. */
  readonly intervalMs?: number;
}

/**
 * Connection settings for one external participant runtime process.
 */
export interface ParticipantRuntimeClientConfig {
  /** Last observed event sequence to resume after; acts as a resume floor. */
  readonly afterSeq: number;
  /** Bearer token sent to Tether; falls back to SERVICE_AUTH_TOKEN/TETHER_AUTH_TOKEN when omitted. */
  readonly authToken?: string | null;
  /** Public participant capabilities registered with the session. */
  readonly capabilities: Record<string, unknown>;
  /** Maximum time to wait for one correlated WebSocket command response. */
  readonly commandTimeoutMs?: number;
  /** Optional throttle policy for durable cursor writes. */
  readonly cursorPersist?: ParticipantRuntimeCursorPersistPolicy;
  /** Optional durable resume cursor. When omitted, resume behavior is unchanged. */
  readonly cursorStore?: ParticipantRuntimeCursorStore;
  /** Display name shown in participant presence. */
  readonly displayName: string;
  /** Concrete runtime process id that owns the WebSocket control lease. */
  readonly instanceId: string;
  /** Stable or generated participant identity controlled by this runtime. */
  readonly participantId: string;
  /** Optional reconnect backoff settings for long-running adapters. */
  readonly reconnect?: {
    /** Initial delay before reconnecting after an unexpected disconnect. */
    readonly baseDelayMs?: number;
    /** Maximum reconnect delay after repeated failures. */
    readonly maxDelayMs?: number;
  };
  /** Runtime implementation kind, such as `generic_agent` or `codex`. */
  readonly runtimeKind: ParticipantRuntimeKind;
  /** Base URL of the Tether HTTP service. */
  readonly serviceUrl: string;
  /** Session the participant runtime connects to. */
  readonly sessionId: string;
}

/**
 * Options for processing claimable tasks from the event stream.
 */
export interface ParticipantRuntimeTaskLoopOptions {
  /** Interval used to refresh active task claim leases. */
  readonly claimRefreshMs: number;
  /** Adapter-specific task executor. */
  readonly executor: ParticipantTaskExecutor;
  /** Whether to process replayed claimable work once and exit. */
  readonly once: boolean;
  /** Adapter-specific task selection rule. */
  readonly shouldClaimTask: ParticipantTaskSelector;
}

/** Cleanup callback returned by participant runtime lifecycle hooks. */
export type ParticipantRuntimeCleanup = () => void | Promise<void>;

/** Optional lifecycle hooks for the high-level participant runtime runner. */
export interface RunParticipantRuntimeHooks {
  /**
   * Handles stream and reconnect errors. Omit to use structured stderr
   * logging; pass null only when the embedding host installs an equivalent
   * visible error path.
   */
  readonly onError?: ((error: Error) => void) | null;
  /**
   * Runs after historical replay completes and before the task claim loop
   * starts. Use this to publish startup status or register observers that must
   * receive replay backlog. A returned cleanup callback runs after the claim
   * loop exits.
   */
  readonly onReplayComplete?: (
    client: ParticipantRuntimeClient,
  ) => undefined | ParticipantRuntimeCleanup | Promise<undefined | ParticipantRuntimeCleanup>;
}

/**
 * High-level adapter entry point for running one participant runtime.
 */
export interface RunParticipantRuntimeInput extends ParticipantRuntimeClientConfig {
  /** Interval used to refresh active task claim leases. */
  readonly claimRefreshMs: number;
  /** Adapter-specific task executor. */
  readonly executor: ParticipantTaskExecutor;
  /** Optional lifecycle and error hooks for adapters with replay-ready setup. */
  readonly hooks?: RunParticipantRuntimeHooks;
  /** Whether to process replayed claimable work once and exit. */
  readonly once?: boolean;
  /** Optional adapter-specific selector; defaults to work kind and participant ownership checks. */
  readonly shouldClaimTask?: ParticipantTaskSelector;
  /** Task kinds this runtime is willing to claim. */
  readonly workKinds: readonly string[];
}

/**
 * Runtime diagnostics for one participant WebSocket client.
 */
export interface ParticipantRuntimeClientDebugInfo extends BoundaryDebugInfo {
  /** Number of events received before a handler was attached. */
  readonly eventBacklogSize: number;
  /** Number of registered event handlers. */
  readonly eventHandlerCount: number;
  /** Highest event sequence observed by this client. */
  readonly lastObservedSeq: number;
  /** Number of failed reconnect attempts since startup. */
  readonly reconnectFailureCount: number;
  /** Number of successful reconnects since startup. */
  readonly reconnectSuccessCount: number;
  /** Whether the client has been intentionally closed. */
  readonly stopped: boolean;
  /** Number of in-flight command requests waiting for server responses. */
  readonly pendingCommandCount: number;
  /** Underlying WebSocket ready state. */
  readonly socketReadyState: number;
}

/**
 * Runs a participant runtime using the reusable client boundary. Adapters call
 * this when they only need to provide identity, capabilities, task selection,
 * and execution policy.
 */
export async function runParticipantRuntime(input: RunParticipantRuntimeInput): Promise<void> {
  const client = await ParticipantRuntimeClient.connect(input);
  const unsubscribeError = installRunParticipantRuntimeErrorHandler(client, input);
  let cleanup: ParticipantRuntimeCleanup | undefined;
  try {
    await client.waitForReplayComplete();
    const replayHookResult = await input.hooks?.onReplayComplete?.(client);
    cleanup = typeof replayHookResult === "function" ? replayHookResult : undefined;
    await client.runClaimableTasks({
      claimRefreshMs: input.claimRefreshMs,
      executor: input.executor,
      once: input.once ?? false,
      shouldClaimTask:
        input.shouldClaimTask ??
        ((task) => shouldClaimParticipantTask(task, input.workKinds, input.participantId)),
    });
  } finally {
    try {
      await cleanup?.();
    } finally {
      unsubscribeError();
    }
  }
}

/**
 * Checks whether a task is claimable by a participant with a work-kind allow
 * list.
 */
export function shouldClaimParticipantTask(
  task: TaskRecord,
  workKinds: readonly string[],
  participantId: string,
): boolean {
  if (!workKinds.includes(task.kind)) {
    return false;
  }
  if (task.completedAt || task.failedAt || task.cancelledAt) {
    return false;
  }
  return task.claimedBy === null || task.claimedBy === participantId;
}

/** Installs default or caller-supplied error handling for the high-level runner. */
function installRunParticipantRuntimeErrorHandler(
  client: ParticipantRuntimeClient,
  input: RunParticipantRuntimeInput,
): () => void {
  if (input.hooks?.onError === null) {
    return () => undefined;
  }
  const handler =
    input.hooks?.onError ?? ((error) => writeDefaultRuntimeError(error, client, input));
  return client.onError(handler);
}

/** Writes one structured participant runtime error payload to stderr. */
function writeDefaultRuntimeError(
  error: Error,
  client: ParticipantRuntimeClient,
  input: RunParticipantRuntimeInput,
): void {
  const debug = client.debugInfo();
  process.stderr.write(
    `${JSON.stringify({
      errorMessage: error.message,
      errorName: error.name,
      instanceId: input.instanceId,
      participantId: input.participantId,
      reconnectFailureCount: debug.reconnectFailureCount,
      runtimeKind: input.runtimeKind,
      sessionId: input.sessionId,
      type: "participant_runtime.error",
    })}\n`,
  );
}

/**
 * Reusable WebSocket runtime client for external participants. It owns replay
 * buffering, command correlation, task claiming, claim refresh, cancellation,
 * and shutdown hooks so adapter workers only provide task-selection and
 * execution policy.
 */
export class ParticipantRuntimeClient {
  private static readonly maxRecentEvents = 80;
  private readonly closeHandlers = new Set<() => void>();
  private closePromise: Promise<void> = Promise.resolve();
  private readonly eventBacklog: SessionEvent[] = [];
  private readonly eventHandlers = new Set<(event: SessionEvent) => void>();
  private readonly errorHandlers = new Set<(error: Error) => void>();
  private lastObservedSeq: number;
  private lastHandledSeq: number;
  private lastPersistedSeq: number;
  private eventsSinceCursorPersist = 0;
  private cursorPersistTimer: ReturnType<typeof setTimeout> | null = null;
  private readonly cursorPersistIntervalMs: number;
  private readonly cursorPersistEventCount: number;
  private readonly commandTimeoutMs: number;
  private readonly observability = new ModuleObservability(
    readModuleObservabilityOptions("ParticipantRuntimeClient"),
  );
  private readonly pendingCommands = new Map<string, PendingCommand>();
  private readonly recentEvents: SessionEvent[] = [];
  private reconnectFailureCount = 0;
  private reconnectSuccessCount = 0;
  private replayComplete: Promise<void> = Promise.resolve();
  private rejectReplayComplete: ((error: Error) => void) | null = null;
  private resolveReplayComplete: (() => void) | null = null;
  private replayCompleteSettled = true;
  private socket: WebSocket | null = null;
  private stopped = false;

  /**
   * Attaches protocol message handling to an already-created WebSocket.
   */
  private constructor(private readonly config: ParticipantRuntimeClientConfig) {
    this.lastObservedSeq = config.afterSeq;
    this.lastHandledSeq = config.afterSeq;
    this.lastPersistedSeq = config.afterSeq;
    this.commandTimeoutMs = resolveCommandTimeoutMs(config.commandTimeoutMs);
    this.cursorPersistIntervalMs =
      config.cursorPersist?.intervalMs ?? defaultCursorPersistIntervalMs;
    this.cursorPersistEventCount =
      config.cursorPersist?.eventCount ?? defaultCursorPersistEventCount;
  }

  /**
   * Opens the WebSocket stream and returns a command-capable participant client.
   * When a durable cursor is configured, the stream resumes from the higher of
   * `afterSeq` and the persisted cursor so restarts replay only actual downtime.
   */
  static async connect(config: ParticipantRuntimeClientConfig): Promise<ParticipantRuntimeClient> {
    const client = new ParticipantRuntimeClient(config);
    const resumeSeq = await client.resolveInitialResumeSeq();
    client.lastObservedSeq = resumeSeq;
    client.lastHandledSeq = resumeSeq;
    client.lastPersistedSeq = resumeSeq;
    await client.open(resumeSeq);
    return client;
  }

  /**
   * Publishes one participant-originated event over the WebSocket command
   * channel.
   */
  async appendEvent(input: AppendSessionEventInput): Promise<void> {
    await this.observability.traceBoundary(
      "appendEvent",
      {
        eventId: input.eventId,
        producerId: input.producerId,
        sessionId: input.sessionId,
        type: input.type,
      },
      async () => {
        await this.sendCommand((requestId) =>
          buildWsPublishMessage({
            eventId: input.eventId,
            payload: input.payload,
            producerId: input.producerId,
            requestId,
            type: input.type,
          }),
        );
      },
    );
  }

  /**
   * Attempts to claim a task and returns null when another participant won.
   */
  async claimTask(taskId: string): Promise<TaskRecord | null> {
    return this.observability.traceBoundary(
      "claimTask",
      { taskId },
      async () => {
        const result = await this.sendCommand((requestId) =>
          buildWsTaskClaimMessage({ requestId, taskId }),
        );
        return result.task ?? null;
      },
      (task) => ({ claimed: task !== null }),
    );
  }

  /**
   * Requests a graceful WebSocket close.
   */
  close(): void {
    this.stopped = true;
    this.persistCursor();
    this.socket?.close();
  }

  /**
   * Drops the current transport without stopping the client. Long-running task
   * loops reconnect from the last observed event sequence.
   */
  disconnect(): void {
    this.socket?.terminate();
  }

  /**
   * Returns client-side queue, cursor, and pending-command counters.
   */
  debugInfo(): ParticipantRuntimeClientDebugInfo {
    return {
      ...this.observability.debugInfo(),
      eventBacklogSize: this.eventBacklog.length,
      eventHandlerCount: this.eventHandlers.size,
      lastObservedSeq: this.lastObservedSeq,
      pendingCommandCount: this.pendingCommands.size,
      reconnectFailureCount: this.reconnectFailureCount,
      reconnectSuccessCount: this.reconnectSuccessCount,
      socketReadyState: this.socket?.readyState ?? WebSocket.CLOSED,
      stopped: this.stopped,
    };
  }

  /**
   * Completes the active task claim with a structured result payload.
   */
  async completeTask(taskId: string, result: Record<string, unknown>): Promise<void> {
    await this.observability.traceBoundary("completeTask", { taskId }, () =>
      this.sendCommand((requestId) => buildWsTaskCompleteMessage({ requestId, result, taskId })),
    );
  }

  /**
   * Fails the active task claim with a structured failure payload.
   */
  async failTask(taskId: string, failure: Record<string, unknown>): Promise<void> {
    await this.observability.traceBoundary("failTask", { taskId }, () =>
      this.sendCommand((requestId) => buildWsTaskFailMessage({ failure, requestId, taskId })),
    );
  }

  /**
   * Registers a close callback and returns an unsubscribe function.
   */
  onClose(handler: () => void): () => void {
    this.closeHandlers.add(handler);
    return () => {
      this.closeHandlers.delete(handler);
    };
  }

  /**
   * Registers an error callback and returns an unsubscribe function.
   */
  onError(handler: (error: Error) => void): () => void {
    this.errorHandlers.add(handler);
    return () => {
      this.errorHandlers.delete(handler);
    };
  }

  /**
   * Registers an event callback and drains any events received before the
   * handler was attached.
   */
  onEvent(handler: (event: SessionEvent) => void): () => void {
    this.eventHandlers.add(handler);
    let drainedSeq = this.lastHandledSeq;
    for (const event of this.eventBacklog.splice(0)) {
      handler(event);
      drainedSeq = Math.max(drainedSeq, event.seq);
    }
    this.markSeqHandled(drainedSeq);
    return () => {
      this.eventHandlers.delete(handler);
    };
  }

  /**
   * Reopens the participant stream from the highest observed event sequence.
   */
  async reconnect(): Promise<ParticipantRuntimeClient> {
    this.socket?.close();
    await this.waitForClose().catch(() => undefined);
    await this.reconnectWithBackoff(0);
    return this;
  }

  /**
   * Refreshes the active task claim lease and returns null if the claim is no
   * longer valid.
   */
  async refreshTaskClaim(taskId: string): Promise<TaskRecord | null> {
    return this.observability.traceBoundary(
      "refreshTaskClaim",
      { taskId },
      async () => {
        const result = await this.sendCommand((requestId) =>
          buildWsTaskRefreshMessage({ requestId, taskId }),
        );
        return result.task ?? null;
      },
      (task) => ({ refreshed: task !== null }),
    );
  }

  /**
   * Processes claimable tasks from replay and live events until either the
   * one-shot replay work completes or the socket closes.
   */
  async runClaimableTasks(options: ParticipantRuntimeTaskLoopOptions): Promise<void> {
    await Effect.runPromise(
      buildParticipantClaimableTaskLoop(
        {
          close: () => this.close(),
          isStopped: () => this.stopped,
          onEvent: (handler) => this.onEvent(handler),
          reconnectAfterClose: () => this.reconnectWithBackoff(),
          runTaskClaimFlow: (input) => this.runTaskClaimFlow(input),
          waitForClose: () => this.waitForClose(),
          waitForReplayComplete: () => this.waitForReplayComplete(),
        },
        options,
      ),
    );
  }

  /**
   * Runs the claim, progress, execution, output, and completion sequence for
   * one task while respecting cancellation.
   */
  async runTaskClaimFlow(input: {
    readonly cancellation: TaskCancellationContext;
    readonly claimRefreshMs: number;
    readonly executor: ParticipantTaskExecutor;
    readonly task: TaskRecord;
  }): Promise<void> {
    await this.observability.traceBoundary(
      "runTaskClaimFlow",
      {
        participantId: this.config.participantId,
        sessionId: this.config.sessionId,
        taskId: input.task.taskId,
      },
      () =>
        runParticipantTaskClaimFlow(
          this,
          {
            instanceId: this.config.instanceId,
            lastObservedSeq: this.lastObservedSeq,
            participantId: this.config.participantId,
            recentEvents: this.recentEvents,
            sessionId: this.config.sessionId,
          },
          this.observability,
          input,
        ),
    );
  }

  /**
   * Resolves when the current socket closes.
   */
  async waitForClose(): Promise<void> {
    await this.closePromise;
  }

  /**
   * Resolves once the server has finished replaying historical events.
   */
  async waitForReplayComplete(): Promise<void> {
    await this.replayComplete;
  }

  /**
   * Opens a WebSocket stream from the requested event cursor.
   */
  private async open(afterSeq: number): Promise<void> {
    this.replayCompleteSettled = false;
    const replayComplete = new Promise<void>((resolve, reject) => {
      this.rejectReplayComplete = reject;
      this.resolveReplayComplete = resolve;
    });
    replayComplete.catch(() => undefined);
    this.replayComplete = replayComplete;
    const socket = new WebSocket(
      buildParticipantRuntimeStreamUrl({
        ...this.config,
        afterSeq,
      }),
    );
    this.socket = socket;
    this.closePromise = new Promise((resolve) => {
      socket.once("close", () => {
        if (this.socket === socket) {
          this.socket = null;
        }
        this.settleReplayCompleteError(new Error("WebSocket closed before replay completed"));
        this.rejectPendingCommands(new Error("WebSocket closed"));
        for (const handler of this.closeHandlers) {
          handler();
        }
        resolve();
      });
    });
    socket.on("error", (error) => {
      this.settleReplayCompleteError(error);
      this.rejectPendingCommands(error);
      this.emitError(error);
    });
    socket.on("message", (data) => {
      this.handleMessage(data);
    });
    await new Promise<void>((resolve, reject) => {
      socket.once("open", resolve);
      socket.once("error", reject);
    });
  }

  /**
   * Reopens the WebSocket stream with bounded retry backoff.
   */
  private async reconnectWithBackoff(initialDelayMs: number | null = null): Promise<void> {
    await Effect.runPromise(this.buildReconnectWithBackoff(initialDelayMs));
  }

  /**
   * Builds the bounded reconnect loop as an Effect program.
   */
  private buildReconnectWithBackoff(
    initialDelayMs: number | null = null,
  ): Effect.Effect<void, never> {
    return Effect.gen(this, function* () {
      let attempt = 0;
      let delayMs = initialDelayMs ?? reconnectDelayMs(attempt, this.config);
      while (!this.stopped) {
        if (delayMs > 0) {
          yield* sleepUnrefEffect(delayMs);
        }
        const opened = yield* Effect.either(
          Effect.tryPromise({
            catch: (error) => error,
            try: () => this.open(this.lastObservedSeq),
          }),
        );
        if (Either.isRight(opened)) {
          this.reconnectSuccessCount += 1;
          return;
        }
        const error = opened.left;
        this.reconnectFailureCount += 1;
        this.emitError(error instanceof Error ? error : new Error("Reconnect failed"));
        attempt += 1;
        delayMs = reconnectDelayMs(attempt, this.config);
      }
    });
  }

  /**
   * Returns the currently open socket or throws a connection-level error.
   */
  private requireOpenSocket(): WebSocket {
    if (!this.socket || this.socket.readyState !== WebSocket.OPEN) {
      throw new Error("Participant runtime WebSocket is not open");
    }
    return this.socket;
  }

  /**
   * Parses one server envelope and routes it to event handlers, pending command
   * promises, or replay completion state.
   */
  private handleMessage(data: WebSocket.RawData): void {
    let parsed: unknown;
    try {
      parsed = JSON.parse(String(data)) as unknown;
      const envelope = parseWebSocketServerEnvelope(parsed);
      if (!envelope) {
        return;
      }
      if (envelope.op === webSocketOperation.event) {
        this.lastObservedSeq = Math.max(this.lastObservedSeq, envelope.event.seq);
        this.rememberEvent(envelope.event);
        if (this.eventHandlers.size === 0) {
          this.eventBacklog.push(envelope.event);
          return;
        }
        for (const handler of this.eventHandlers) {
          handler(envelope.event);
        }
        this.markSeqHandled(envelope.event.seq);
        return;
      }
      if (envelope.op === webSocketOperation.commandResult) {
        if (envelope.requestId) {
          const pending = this.settlePendingCommand(envelope.requestId);
          if (!pending) {
            return;
          }
          pending.resolve(envelope);
        }
        return;
      }
      if (envelope.op === webSocketOperation.error) {
        const error = new Error(envelope.error);
        if (envelope.requestId) {
          const pending = this.settlePendingCommand(envelope.requestId);
          if (!pending) {
            return;
          }
          const taskId = readStringField(envelope, "taskId") ?? pending.taskId;
          pending.reject(
            new ParticipantRuntimeCommandError({
              message: envelope.error,
              op: readStringField(envelope, "command") ?? pending.op,
              pendingCommandCount: this.pendingCommands.size,
              requestId: envelope.requestId,
              ...(taskId !== undefined ? { taskId } : {}),
            }),
          );
          return;
        }
        this.settleReplayCompleteError(error);
        this.emitError(error);
        return;
      }
      if (envelope.op === webSocketOperation.replayComplete) {
        this.settleReplayComplete();
      }
    } catch (error) {
      this.emitError(
        error instanceof Error ? error : new Error("Failed to handle WebSocket message"),
      );
    }
  }

  /**
   * Routes a connection-level or message-handling error to registered error
   * handlers instead of throwing out of a ws event listener, which would
   * surface as an unhandled exception.
   */
  private emitError(error: Error): void {
    for (const handler of this.errorHandlers) {
      handler(error);
    }
  }

  /** Resolves the replay wait once, preserving later reconnect state. */
  private settleReplayComplete(): void {
    if (this.replayCompleteSettled) {
      return;
    }
    this.replayCompleteSettled = true;
    this.resolveReplayComplete?.();
    this.resolveReplayComplete = null;
    this.rejectReplayComplete = null;
  }

  /** Rejects the replay wait once when the socket fails before replay.complete. */
  private settleReplayCompleteError(error: Error): void {
    if (this.replayCompleteSettled) {
      return;
    }
    this.replayCompleteSettled = true;
    this.rejectReplayComplete?.(error);
    this.resolveReplayComplete = null;
    this.rejectReplayComplete = null;
  }

  /**
   * Keeps a bounded in-memory window of replay/live events for executors that
   * need conversation context without reimplementing stream replay.
   */
  private rememberEvent(event: SessionEvent): void {
    this.recentEvents.push(event);
    if (this.recentEvents.length > ParticipantRuntimeClient.maxRecentEvents) {
      this.recentEvents.splice(
        0,
        this.recentEvents.length - ParticipantRuntimeClient.maxRecentEvents,
      );
    }
  }

  /**
   * Resolves the startup resume sequence from the durable cursor, never below
   * the configured `afterSeq` floor. Returns `afterSeq` when no cursor store is
   * configured, preserving the fixed-window resume behavior.
   */
  private async resolveInitialResumeSeq(): Promise<number> {
    const cursorStore = this.config.cursorStore;
    if (!cursorStore) {
      return this.config.afterSeq;
    }
    const stored = await cursorStore.read();
    return resolveResumeSeq(this.config.afterSeq, stored);
  }

  /**
   * Advances the handled cursor once an event's handlers have been invoked and
   * schedules a durable persist. Only sequences that have actually been
   * delivered to a handler reach the durable store, so a restart never resumes
   * past an event that was merely received but never handled. No-op when the
   * sequence has not advanced past the last handled value.
   */
  private markSeqHandled(seq: number): void {
    if (seq <= this.lastHandledSeq) {
      return;
    }
    this.lastHandledSeq = seq;
    this.scheduleCursorPersist();
  }

  /**
   * Records that the handled cursor advanced and persists it under the
   * configured throttle: immediately once enough events accumulate, otherwise
   * on a trailing timer. No-op when no cursor store is configured.
   */
  private scheduleCursorPersist(): void {
    if (!this.config.cursorStore) {
      return;
    }
    this.eventsSinceCursorPersist += 1;
    if (this.eventsSinceCursorPersist >= this.cursorPersistEventCount) {
      this.persistCursor();
      return;
    }
    if (this.cursorPersistTimer === null) {
      const timer = setTimeout(() => {
        this.cursorPersistTimer = null;
        this.persistCursor();
      }, this.cursorPersistIntervalMs);
      timer.unref?.();
      this.cursorPersistTimer = timer;
    }
  }

  /**
   * Writes the highest handled event sequence to the durable cursor store,
   * skipping when it has not advanced past the last persisted value. Only
   * sequences confirmed as handled are persisted, never a value ahead of the
   * events the runtime has actually processed.
   */
  private persistCursor(): void {
    if (this.cursorPersistTimer !== null) {
      clearTimeout(this.cursorPersistTimer);
      this.cursorPersistTimer = null;
    }
    this.eventsSinceCursorPersist = 0;
    const cursorStore = this.config.cursorStore;
    if (!cursorStore) {
      return;
    }
    const seq = this.lastHandledSeq;
    if (seq <= this.lastPersistedSeq) {
      return;
    }
    this.lastPersistedSeq = seq;
    try {
      const result = cursorStore.write(seq);
      if (result instanceof Promise) {
        result.catch((error) => {
          this.emitError(error instanceof Error ? error : new Error("Cursor store write failed"));
        });
      }
    } catch (error) {
      this.emitError(error instanceof Error ? error : new Error("Cursor store write failed"));
    }
  }

  /**
   * Rejects all pending command promises with a shared connection-level error.
   */
  private rejectPendingCommands(error: Error): void {
    const pendingCommands = [...this.pendingCommands.values()];
    this.pendingCommands.clear();
    for (const pending of pendingCommands) {
      clearTimeout(pending.timeout);
      pending.reject(error);
    }
  }

  /**
   * Sends a command with a generated request id and waits for its matching
   * command result or error envelope.
   */
  private async sendCommand(
    buildCommand: (requestId: string) => WebSocketCommandMessage,
  ): Promise<CommandResultEnvelope> {
    const result = await Effect.runPromise(Effect.either(this.buildCommandRequest(buildCommand)));
    if (Either.isLeft(result)) {
      throw result.left;
    }
    return result.right;
  }

  /**
   * Builds one correlated WebSocket command request and guarantees pending
   * command cleanup if the waiting Effect is interrupted.
   */
  private buildCommandRequest(
    buildCommand: (requestId: string) => WebSocketCommandMessage,
  ): Effect.Effect<CommandResultEnvelope, Error> {
    const requestId = `req_${randomUUID()}`;
    return Effect.async<CommandResultEnvelope, Error>((resume) => {
      const command = buildCommand(requestId);
      const commandContext = readCommandContext(command, requestId);
      const resolve = (result: CommandResultEnvelope): void => {
        resume(Effect.succeed(result));
      };
      const reject = (error: Error): void => {
        resume(Effect.fail(error));
      };
      const timeout = setTimeout(() => {
        const pending = this.settlePendingCommand(requestId);
        if (!pending) {
          return;
        }
        pending.reject(
          new ParticipantRuntimeCommandTimeoutError({
            op: pending.op,
            pendingCommandCount: this.pendingCommands.size,
            requestId: pending.requestId,
            ...(pending.taskId !== undefined ? { taskId: pending.taskId } : {}),
            timeoutMs: this.commandTimeoutMs,
          }),
        );
      }, this.commandTimeoutMs);
      timeout.unref?.();
      this.pendingCommands.set(requestId, {
        ...commandContext,
        reject,
        resolve,
        timeout,
      });
      try {
        this.requireOpenSocket().send(JSON.stringify(command));
      } catch (error) {
        this.settlePendingCommand(requestId);
        resume(Effect.fail(error instanceof Error ? error : new Error("Command send failed")));
      }
      return Effect.sync(() => {
        this.settlePendingCommand(requestId);
      });
    });
  }

  /** Removes one pending command and clears its timer before settlement. */
  private settlePendingCommand(requestId: string): PendingCommand | null {
    const pending = this.pendingCommands.get(requestId);
    if (!pending) {
      return null;
    }
    this.pendingCommands.delete(requestId);
    clearTimeout(pending.timeout);
    return pending;
  }
}

/**
 * Builds the WebSocket stream URL with participant identity and capabilities in
 * query parameters.
 */
export function buildParticipantRuntimeStreamUrl(config: ParticipantRuntimeClientConfig): string {
  const url = new URL(`/sessions/${config.sessionId}/stream`, config.serviceUrl);
  url.protocol = url.protocol === "https:" ? "wss:" : "ws:";
  url.searchParams.set("after", String(config.afterSeq));
  url.searchParams.set("capabilities", JSON.stringify(config.capabilities));
  url.searchParams.set("displayName", config.displayName);
  url.searchParams.set("instanceId", config.instanceId);
  url.searchParams.set("participantId", config.participantId);
  url.searchParams.set("runtimeKind", config.runtimeKind);
  const authToken = resolveServiceAuthToken(config.authToken);
  if (authToken) {
    url.searchParams.set("access_token", authToken);
  }
  return url.toString();
}

/**
 * Computes bounded exponential reconnect backoff from client configuration.
 */
function reconnectDelayMs(attempt: number, config: ParticipantRuntimeClientConfig): number {
  const baseDelayMs = config.reconnect?.baseDelayMs ?? defaultReconnectBaseDelayMs;
  const maxDelayMs = config.reconnect?.maxDelayMs ?? defaultReconnectMaxDelayMs;
  return Math.min(maxDelayMs, baseDelayMs * 2 ** attempt);
}

/**
 * Resolves the durable resume sequence as the higher of the configured
 * `afterSeq` floor and the persisted cursor, treating a missing or non-finite
 * stored value as zero. The resume point is never below `afterSeq`.
 */
export function resolveResumeSeq(afterSeq: number, storedSeq: number | null): number {
  const normalized = typeof storedSeq === "number" && Number.isFinite(storedSeq) ? storedSeq : 0;
  return Math.max(afterSeq, normalized);
}

/** Resolves and validates the finite command response timeout. */
export function resolveCommandTimeoutMs(commandTimeoutMs: number | undefined): number {
  const resolved = commandTimeoutMs ?? defaultCommandTimeoutMs;
  if (!Number.isFinite(resolved) || resolved <= 0) {
    throw new ParticipantRuntimeClientConfigurationError(
      "commandTimeoutMs",
      "Participant runtime commandTimeoutMs must be a finite positive number",
    );
  }
  return resolved;
}

/** Extracts bounded command metadata without retaining the full payload. */
function readCommandContext(command: WebSocketCommandMessage, requestId: string): CommandContext {
  const taskId = readStringField(command, "taskId");
  return {
    op: command.op,
    requestId,
    ...(taskId !== undefined ? { taskId } : {}),
  };
}

/** Reads one string field from a passthrough protocol record. */
function readStringField(record: Record<string, unknown>, field: string): string | undefined {
  const value = record[field];
  return typeof value === "string" ? value : undefined;
}
