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
import { ParticipantCursorWriter } from "./participant-cursor-writer.js";
import { sleepUnrefEffect } from "./effect-timing.js";
import { SerialEventDelivery, type SerialEventDeliveryOutcome } from "./serial-event-delivery.js";
import type { ParticipantRuntimeKind, SessionEvent, TaskRecord } from "./types.js";

export { ParticipantRuntimeCursorPersistError } from "./participant-cursor-writer.js";

const defaultReconnectBaseDelayMs = 100;
const defaultReconnectMaxDelayMs = 2_000;
const defaultCommandTimeoutMs = 15_000;
const defaultCursorPersistIntervalMs = 1_000;
const defaultCursorPersistEventCount = 50;
const defaultCursorPersistRetryAttempts = 5;
const defaultCursorPersistRetryBaseDelayMs = 100;
const defaultCursorPersistRetryMaxDelayMs = 2_000;
const defaultCursorPersistWriteTimeoutMs = 5_000;
const defaultParticipantHandlerTimeoutMs = 30_000;
const defaultParticipantMaxQueueSize = 2_000;
const defaultParticipantMaxRecoveryAttempts = 5;
const defaultParticipantShutdownTimeoutMs = 30_000;

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

interface PreparedParticipantTransport {
  readonly afterSeq: number;
  readonly generation: number;
  readonly resolveClose: () => void;
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

/** Recoverable participant event delivery failure with safe correlation fields. */
export class ParticipantRuntimeEventDeliveryError extends Error {
  readonly attempt: number;
  readonly eventId: string;
  readonly handlerIndex: number;
  readonly reason: "event_handler_failed" | "event_handler_timeout";
  readonly seq: number;
  readonly timeoutMs: number | null;

  constructor(input: {
    readonly attempt: number;
    readonly cause?: unknown;
    readonly eventId: string;
    readonly handlerIndex: number;
    readonly reason: "event_handler_failed" | "event_handler_timeout";
    readonly seq: number;
    readonly timeoutMs?: number;
  }) {
    super(
      input.reason === "event_handler_timeout"
        ? "Participant event handler timed out"
        : "Participant event handler failed",
      input.cause === undefined ? undefined : { cause: input.cause },
    );
    this.attempt = input.attempt;
    this.eventId = input.eventId;
    this.handlerIndex = input.handlerIndex;
    this.name = "ParticipantRuntimeEventDeliveryError";
    this.reason = input.reason;
    this.seq = input.seq;
    this.timeoutMs = input.timeoutMs ?? null;
  }
}

/** Terminal participant stream failure that requires explicit remediation. */
export class ParticipantRuntimeTerminalStreamError extends Error {
  readonly reason: ParticipantRuntimePausedReason;
  readonly safeDetails: Readonly<Record<string, number | string | null>>;

  constructor(input: {
    readonly cause?: unknown;
    readonly reason: ParticipantRuntimePausedReason;
    readonly safeDetails?: Readonly<Record<string, number | string | null>>;
  }) {
    super(
      "Participant event stream entered Paused State",
      input.cause === undefined ? undefined : { cause: input.cause },
    );
    this.name = "ParticipantRuntimeTerminalStreamError";
    this.reason = input.reason;
    this.safeDetails = input.safeDetails ?? {};
  }
}

/** Correlated command whose server-side outcome is unknown after recovery. */
export class ParticipantRuntimeCommandOutcomeUnknownError extends Error {
  readonly op: string;
  readonly requestId: string;
  readonly taskId?: string;

  constructor(input: {
    readonly cause?: unknown;
    readonly op: string;
    readonly requestId: string;
    readonly taskId?: string;
  }) {
    super(
      "Participant command outcome is unknown after transport recovery",
      input.cause === undefined ? undefined : { cause: input.cause },
    );
    this.name = "ParticipantRuntimeCommandOutcomeUnknownError";
    this.op = input.op;
    this.requestId = input.requestId;
    if (input.taskId !== undefined) {
      this.taskId = input.taskId;
    }
  }
}

/** Graceful shutdown that could not settle every required participant boundary. */
export class ParticipantRuntimeShutdownError extends Error {
  /** Boundaries still incomplete when shutdown rejected. */
  readonly pendingPhases: readonly ParticipantRuntimeShutdownPhase[];
  /** Whether shutdown exhausted its overall deadline or completed incompletely. */
  readonly reason: "incomplete" | "timeout";
  /** Overall graceful shutdown deadline. */
  readonly timeoutMs: number;

  constructor(input: {
    readonly pendingPhases: readonly ParticipantRuntimeShutdownPhase[];
    readonly reason: "incomplete" | "timeout";
    readonly timeoutMs: number;
  }) {
    super(
      input.reason === "timeout"
        ? "Participant graceful shutdown timed out"
        : "Participant graceful shutdown completed without durable settlement",
    );
    this.name = "ParticipantRuntimeShutdownError";
    this.pendingPhases = [...input.pendingPhases];
    this.reason = input.reason;
    this.timeoutMs = input.timeoutMs;
  }
}

/** Awaited participant boundaries that make up graceful shutdown. */
export type ParticipantRuntimeShutdownPhase = "cursor" | "delivery" | "socket";

/** Machine-readable reasons that put participant delivery into Paused State. */
export type ParticipantRuntimePausedReason =
  | "delivery_queue_overflow"
  | "delivery_retry_exhausted"
  | "invalid_server_envelope"
  | "non_contiguous_event"
  | "replay_gap_unrepaired"
  | "replay_window_exceeded";

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

/** Handles one participant event before its durable cursor may advance. */
export type ParticipantRuntimeEventHandler = (event: SessionEvent) => void | Promise<void>;

/** Factory for participant WebSocket connections, injectable by tests and hosts. */
export type ParticipantRuntimeWebSocketFactory = (url: string) => WebSocket;

/**
 * Durable resume cursor for a participant runtime. When supplied, the runtime
 * resumes from the persisted sequence instead of replaying a fixed window from
 * `afterSeq` on every process start. `read` runs once at startup; `write` runs
 * throttled as events are handled and once more on graceful close.
 */
export interface ParticipantRuntimeCursorStore {
  /** Returns the last persisted event sequence, or null when none is stored. */
  read(): number | null | Promise<number | null>;
  /**
   * Atomically retains the maximum handled sequence. A timed-out earlier call
   * may finish late, so implementations must never replace a higher stored
   * cursor with a lower `seq`.
   */
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
  /** Maximum durable write attempts in one bounded retry cycle. */
  readonly retryAttempts?: number;
  /** Initial delay between durable write attempts. */
  readonly retryBaseDelayMs?: number;
  /** Maximum delay between durable write attempts. */
  readonly retryMaxDelayMs?: number;
  /** Maximum duration of one durable write attempt. */
  readonly writeTimeoutMs?: number;
}

/** Finite participant event delivery and recovery limits. */
export interface ParticipantRuntimeEventDeliveryPolicy {
  /** Maximum time one handler may run before recovery begins. */
  readonly handlerTimeoutMs?: number;
  /** Maximum queued events and replay markers retained per connection. */
  readonly maxQueueSize?: number;
  /** Failed deliveries of one sequence allowed before Paused State. */
  readonly maxRecoveryAttempts?: number;
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
  /** Optional finite event delivery and recovery limits. */
  readonly eventDelivery?: ParticipantRuntimeEventDeliveryPolicy;
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
  /** Overall deadline for delivery, socket, and cursor shutdown settlement. */
  readonly shutdownTimeoutMs?: number;
  /** WebSocket implementation factory, injected by tests and non-Node hosts. */
  readonly webSocketFactory?: ParticipantRuntimeWebSocketFactory;
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
  /** Optional post-replay barrier that must settle before buffered task dispatch. */
  readonly replayBarrier?: () => Promise<void>;
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
   * Runs on the constructed client before its socket opens. Replay-consuming
   * observers must subscribe here. A returned cleanup runs during shutdown.
   */
  readonly onClientReady?: (
    client: ParticipantRuntimeClient,
  ) => undefined | ParticipantRuntimeCleanup | Promise<undefined | ParticipantRuntimeCleanup>;
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
  /** Event sequence currently awaiting handler completion. */
  readonly activeDeliverySeq: number | null;
  /** Number of events received before a handler was attached. */
  readonly eventBacklogSize: number;
  /** Monotonic transport generation assigned at socket open. */
  readonly connectionGeneration: number;
  /** Number of registered event handlers. */
  readonly eventHandlerCount: number;
  /** Number of handler failures and timeouts observed. */
  readonly eventDeliveryFailureCount: number;
  /** Number of failed durable cursor write attempts since startup. */
  readonly cursorPersistFailureCount: number;
  /** Highest event sequence whose non-empty handler snapshot completed. */
  readonly lastHandledSeq: number;
  /** Highest event sequence acknowledged by the durable cursor store. */
  readonly lastPersistedSeq: number;
  /** @deprecated Use `lastHandledSeq`; retained as a handled-cursor alias through 0.2.x. */
  readonly lastObservedSeq: number;
  /** Highest contiguous event sequence admitted from the current stream. */
  readonly lastReceivedSeq: number;
  /** Highest handled sequence waiting for durable acknowledgement. */
  readonly pendingCursorSeq: number | null;
  /** Terminal reason requiring explicit reconnect, or null while active. */
  readonly pausedReason: ParticipantRuntimePausedReason | null;
  /** Number of failed reconnect attempts since startup. */
  readonly reconnectFailureCount: number;
  /** Number of successful reconnects since startup. */
  readonly reconnectSuccessCount: number;
  /** Number of delivery-triggered recovery attempts. */
  readonly recoveryCount: number;
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
  const client = await ParticipantRuntimeClient.create(input);
  const unsubscribeError = installRunParticipantRuntimeErrorHandler(client, input);
  const cleanups: ParticipantRuntimeCleanup[] = [];
  let runError: unknown;
  try {
    const clientReadyResult = await input.hooks?.onClientReady?.(client);
    if (typeof clientReadyResult === "function") {
      cleanups.push(clientReadyResult);
    }
    const claimableTasks = client.runClaimableTasks({
      claimRefreshMs: input.claimRefreshMs,
      executor: input.executor,
      once: input.once ?? false,
      replayBarrier: async () => {
        const replayHookResult = await input.hooks?.onReplayComplete?.(client);
        if (typeof replayHookResult === "function") {
          cleanups.push(replayHookResult);
        }
      },
      shouldClaimTask:
        input.shouldClaimTask ??
        ((task) => shouldClaimParticipantTask(task, input.workKinds, input.participantId)),
    });
    claimableTasks.catch(() => undefined);
    await client.open();
    await claimableTasks;
  } catch (error) {
    runError = error;
  }
  const shutdownErrors: unknown[] = [];
  for (const cleanup of cleanups.reverse()) {
    try {
      await cleanup();
    } catch (error) {
      shutdownErrors.push(error);
    }
  }
  try {
    await client.closeAndWait();
  } catch (error) {
    shutdownErrors.push(error);
  }
  unsubscribeError();
  if (runError !== undefined) {
    if (shutdownErrors.length > 0) {
      throw new AggregateError([runError, ...shutdownErrors], "Participant runtime failed");
    }
    throw runError;
  }
  if (shutdownErrors.length === 1) {
    throw shutdownErrors[0];
  }
  if (shutdownErrors.length > 1) {
    throw new AggregateError(shutdownErrors, "Participant runtime shutdown failed");
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
  private connectionGeneration = 0;
  private delivery: SerialEventDelivery<SessionEvent>;
  private deliveryFailureCount = 0;
  private deliveryRecoveryAttempts = 0;
  private deliveryRecoverySeq: number | null = null;
  private readonly deliveryUnsubscribers = new Map<ParticipantRuntimeEventHandler, () => void>();
  private readonly errorBacklog: Error[] = [];
  private readonly eventHandlers = new Set<ParticipantRuntimeEventHandler>();
  private readonly errorHandlers = new Set<(error: Error) => void>();
  private lastHandledSeq: number;
  private cursorWriter: ParticipantCursorWriter;
  private eventsSinceCursorPersist = 0;
  private cursorPersistTimer: ReturnType<typeof setTimeout> | null = null;
  private readonly cursorPersistIntervalMs: number;
  private readonly cursorPersistEventCount: number;
  private readonly commandTimeoutMs: number;
  private readonly handlerTimeoutMs: number;
  private readonly maxDeliveryQueueSize: number;
  private readonly maxRecoveryAttempts: number;
  private readonly observability = new ModuleObservability(
    readModuleObservabilityOptions("ParticipantRuntimeClient"),
  );
  private readonly pendingCommands = new Map<string, PendingCommand>();
  private readonly recentEvents: SessionEvent[] = [];
  private reconnectPromise: Promise<void> | null = null;
  private reconnectFailureCount = 0;
  private reconnectSuccessCount = 0;
  private recoveryCount = 0;
  private replayComplete: Promise<void> = Promise.resolve();
  private rejectReplayComplete: ((error: Error) => void) | null = null;
  private resolveReplayComplete: (() => void) | null = null;
  private replayCompleteSettled = true;
  private pausedReason: ParticipantRuntimePausedReason | null = null;
  private preparedTransport: PreparedParticipantTransport | null = null;
  private socket: WebSocket | null = null;
  private stopped = false;
  private shutdownPromise: Promise<void> | null = null;
  private readonly shutdownTimeoutMs: number;
  private readonly webSocketFactory: ParticipantRuntimeWebSocketFactory;

  /**
   * Attaches protocol message handling to an already-created WebSocket.
   */
  private constructor(private readonly config: ParticipantRuntimeClientConfig) {
    this.delivery = this.createDelivery(config.afterSeq);
    this.lastHandledSeq = config.afterSeq;
    this.cursorWriter = this.createCursorWriter(config.afterSeq);
    this.commandTimeoutMs = resolveCommandTimeoutMs(config.commandTimeoutMs);
    this.handlerTimeoutMs = resolvePositiveIntegerConfig(
      "eventDelivery.handlerTimeoutMs",
      config.eventDelivery?.handlerTimeoutMs,
      defaultParticipantHandlerTimeoutMs,
    );
    this.maxDeliveryQueueSize = resolvePositiveIntegerConfig(
      "eventDelivery.maxQueueSize",
      config.eventDelivery?.maxQueueSize,
      defaultParticipantMaxQueueSize,
    );
    this.maxRecoveryAttempts = resolvePositiveIntegerConfig(
      "eventDelivery.maxRecoveryAttempts",
      config.eventDelivery?.maxRecoveryAttempts,
      defaultParticipantMaxRecoveryAttempts,
    );
    this.shutdownTimeoutMs = resolvePositiveIntegerConfig(
      "shutdownTimeoutMs",
      config.shutdownTimeoutMs,
      defaultParticipantShutdownTimeoutMs,
    );
    this.cursorPersistIntervalMs =
      config.cursorPersist?.intervalMs ?? defaultCursorPersistIntervalMs;
    this.cursorPersistEventCount =
      config.cursorPersist?.eventCount ?? defaultCursorPersistEventCount;
    this.webSocketFactory = config.webSocketFactory ?? ((url) => new WebSocket(url));
  }

  /** Constructs a participant client and prepares replay state without opening a socket. */
  static async create(config: ParticipantRuntimeClientConfig): Promise<ParticipantRuntimeClient> {
    const client = new ParticipantRuntimeClient(config);
    const resumeSeq = await client.resolveInitialResumeSeq();
    client.lastHandledSeq = resumeSeq;
    client.cursorWriter = client.createCursorWriter(resumeSeq);
    client.prepareTransport(resumeSeq);
    return client;
  }

  /** Constructs and opens a command-capable participant client. */
  static async connect(config: ParticipantRuntimeClientConfig): Promise<ParticipantRuntimeClient> {
    const client = await ParticipantRuntimeClient.create(config);
    await client.open();
    return client;
  }

  /** Opens the transport prepared during construction or recovery. */
  async open(): Promise<ParticipantRuntimeClient> {
    if (this.stopped) {
      throw new Error("Cannot open a stopped participant runtime client");
    }
    const prepared = this.preparedTransport ?? this.prepareTransport(this.lastHandledSeq);
    this.preparedTransport = null;
    try {
      await this.attachPreparedTransport(prepared);
    } catch (error) {
      const openError =
        error instanceof Error ? error : new Error("Participant socket open failed");
      this.settleReplayCompleteError(openError);
      if (!this.socket) {
        prepared.resolveClose();
      }
      throw openError;
    }
    return this;
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
    this.delivery.stop();
    this.persistCursor();
    if (this.socket) {
      this.socket.close();
    } else {
      this.preparedTransport?.resolveClose();
    }
  }

  /**
   * Stops transport activity and waits for active delivery, socket closure,
   * and final durable cursor acknowledgement under one overall deadline.
   */
  closeAndWait(): Promise<void> {
    if (this.shutdownPromise) {
      return this.shutdownPromise;
    }
    const pendingPhases = new Set<ParticipantRuntimeShutdownPhase>([
      "cursor",
      "delivery",
      "socket",
    ]);
    const delivery = this.delivery;
    this.close();
    const settlement = this.settleGracefulShutdown(delivery, pendingPhases);
    const shutdown = withShutdownTimeout(settlement, {
      pendingPhases,
      timeoutMs: this.shutdownTimeoutMs,
    });
    this.shutdownPromise = shutdown;
    return shutdown;
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
    const delivery = this.delivery.debugInfo();
    const cursor = this.cursorWriter.debugInfo();
    return {
      ...this.observability.debugInfo(),
      activeDeliverySeq: delivery.activeDeliverySeq,
      connectionGeneration: this.connectionGeneration,
      cursorPersistFailureCount: cursor.failureCount,
      eventBacklogSize: delivery.queueSize,
      eventHandlerCount: this.eventHandlers.size,
      eventDeliveryFailureCount: this.deliveryFailureCount,
      lastHandledSeq: this.lastHandledSeq,
      lastObservedSeq: this.lastHandledSeq,
      lastPersistedSeq: cursor.acknowledgedSeq,
      lastReceivedSeq: delivery.lastReceivedSeq,
      pausedReason: this.pausedReason,
      pendingCommandCount: this.pendingCommands.size,
      pendingCursorSeq: cursor.pendingSeq,
      reconnectFailureCount: this.reconnectFailureCount,
      reconnectSuccessCount: this.reconnectSuccessCount,
      recoveryCount: this.recoveryCount,
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
    for (const error of this.errorBacklog.splice(0)) {
      handler(error);
    }
    return () => {
      this.errorHandlers.delete(handler);
    };
  }

  /**
   * Registers an event callback and drains any events received before the
   * handler was attached.
   */
  onEvent(handler: ParticipantRuntimeEventHandler): () => void {
    this.eventHandlers.add(handler);
    this.deliveryUnsubscribers.set(handler, this.delivery.onEvent(handler));
    return () => {
      this.eventHandlers.delete(handler);
      this.deliveryUnsubscribers.get(handler)?.();
      this.deliveryUnsubscribers.delete(handler);
    };
  }

  /**
   * Reopens the participant stream from the highest observed event sequence.
   */
  async reconnect(): Promise<ParticipantRuntimeClient> {
    this.pausedReason = null;
    this.deliveryRecoveryAttempts = 0;
    this.deliveryRecoverySeq = null;
    this.socket?.close();
    await this.waitForClose().catch(() => undefined);
    await this.requestReconnect(0);
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
          connectionGeneration: () => this.connectionGeneration,
          isPaused: () => this.pausedReason !== null,
          isStopped: () => this.stopped,
          onEvent: (handler) => this.onEvent(handler),
          reconnectAfterClose: () => this.requestReconnect(),
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
            lastObservedSeq: this.lastHandledSeq,
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

  /** Prepares generation-fenced delivery and replay state before socket construction. */
  private prepareTransport(afterSeq: number): PreparedParticipantTransport {
    const generation = this.connectionGeneration + 1;
    this.connectionGeneration = generation;
    this.replaceDelivery(afterSeq, generation);
    this.replayCompleteSettled = false;
    const replayComplete = new Promise<void>((resolve, reject) => {
      this.rejectReplayComplete = reject;
      this.resolveReplayComplete = resolve;
    });
    replayComplete.catch(() => undefined);
    this.replayComplete = replayComplete;
    let resolveClose = (): void => undefined;
    this.closePromise = new Promise((resolve) => {
      resolveClose = resolve;
    });
    const prepared = { afterSeq, generation, resolveClose };
    this.preparedTransport = prepared;
    return prepared;
  }

  /** Attaches one prepared generation to a newly constructed WebSocket. */
  private async attachPreparedTransport(prepared: PreparedParticipantTransport): Promise<void> {
    const { afterSeq, generation } = prepared;
    const socket = this.webSocketFactory(
      buildParticipantRuntimeStreamUrl({
        ...this.config,
        afterSeq,
      }),
    );
    this.socket = socket;
    socket.once("close", () => {
      if (this.connectionGeneration === generation && this.socket === socket) {
        this.socket = null;
        this.settleReplayCompleteError(new Error("WebSocket closed before replay completed"));
        this.rejectPendingCommands(new Error("WebSocket closed"));
        for (const handler of this.closeHandlers) {
          handler();
        }
      }
      prepared.resolveClose();
    });
    socket.on("error", (error) => {
      if (this.connectionGeneration === generation) {
        this.settleReplayCompleteError(error);
        this.rejectPendingCommands(error);
        this.emitError(error);
      }
    });
    socket.on("message", (data) => {
      this.handleMessage(data, generation);
    });
    await new Promise<void>((resolve, reject) => {
      socket.once("open", resolve);
      socket.once("error", reject);
    });
  }

  /** Prepares and opens one recovery transport from the requested cursor. */
  private async openTransport(afterSeq: number): Promise<void> {
    this.prepareTransport(afterSeq);
    await this.open();
  }

  /**
   * Reopens the WebSocket stream with bounded retry backoff.
   */
  private async reconnectWithBackoff(initialDelayMs: number | null = null): Promise<void> {
    await Effect.runPromise(this.buildReconnectWithBackoff(initialDelayMs));
  }

  /** Coalesces every reconnect caller behind one transport-owner promise. */
  private requestReconnect(initialDelayMs: number | null = null): Promise<void> {
    if (this.reconnectPromise) {
      return this.reconnectPromise;
    }
    const reconnect = this.reconnectWithBackoff(initialDelayMs);
    this.reconnectPromise = reconnect;
    const clearOwner = (): void => {
      if (this.reconnectPromise === reconnect) {
        this.reconnectPromise = null;
      }
    };
    void reconnect.then(clearOwner, clearOwner);
    return reconnect;
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
      while (!this.stopped && this.pausedReason === null) {
        if (delayMs > 0) {
          yield* sleepUnrefEffect(delayMs);
        }
        const opened = yield* Effect.either(
          Effect.tryPromise({
            catch: (error) => error,
            try: () => this.openTransport(this.lastHandledSeq),
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
  private handleMessage(
    data: WebSocket.RawData,
    generation: number = this.connectionGeneration,
  ): void {
    if (generation !== this.connectionGeneration) {
      return;
    }
    let parsed: unknown;
    try {
      parsed = JSON.parse(String(data)) as unknown;
      const envelope = parseWebSocketServerEnvelope(parsed);
      if (!envelope) {
        this.delivery.rejectInvalidEnvelope();
        return;
      }
      if (envelope.op === webSocketOperation.event) {
        this.delivery.enqueueEvent(envelope.event);
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
        if (
          envelope.reason === "replay_window_exceeded" ||
          envelope.reason === "replay_gap_unrepaired"
        ) {
          this.enterPausedState(envelope.reason, error, {}, generation);
          return;
        }
        this.settleReplayCompleteError(error);
        this.emitError(error);
        return;
      }
      if (envelope.op === webSocketOperation.replayComplete) {
        this.delivery.enqueueReplayComplete();
      }
    } catch (error) {
      this.emitError(
        error instanceof Error ? error : new Error("Failed to handle WebSocket message"),
      );
    }
  }

  /** Creates the participant's shared serial delivery state machine. */
  private createDelivery(
    afterSeq: number,
    generation: number = this.connectionGeneration,
  ): SerialEventDelivery<SessionEvent> {
    return new SerialEventDelivery<SessionEvent>({
      handlerTimeoutMs: this.handlerTimeoutMs,
      initialSeq: afterSeq,
      maxQueueSize: this.maxDeliveryQueueSize,
      onOutcome: (outcome) => this.handleDeliveryOutcome(outcome, generation),
    });
  }

  /** Replaces connection-local delivery while preserving public subscriptions. */
  private replaceDelivery(afterSeq: number, generation: number = this.connectionGeneration): void {
    for (const unsubscribe of this.deliveryUnsubscribers.values()) {
      unsubscribe();
    }
    this.deliveryUnsubscribers.clear();
    this.delivery = this.createDelivery(afterSeq, generation);
    for (const handler of this.eventHandlers) {
      this.deliveryUnsubscribers.set(handler, this.delivery.onEvent(handler));
    }
  }

  /** Maps shared delivery outcomes to participant cursor and replay behavior. */
  private handleDeliveryOutcome(
    outcome: SerialEventDeliveryOutcome<SessionEvent>,
    generation: number,
  ): void {
    if (generation !== this.connectionGeneration) {
      return;
    }
    switch (outcome.kind) {
      case "delivery-queue-overflow":
        this.enterPausedState(
          "delivery_queue_overflow",
          undefined,
          {
            maxQueueSize: outcome.maxQueueSize,
            observedQueueSize: outcome.observedQueueSize,
          },
          generation,
        );
        return;
      case "duplicate-ignored":
        return;
      case "event-handled":
        this.recordHandledEvent(outcome.event);
        if (this.deliveryRecoverySeq === outcome.event.seq) {
          this.deliveryRecoveryAttempts = 0;
          this.deliveryRecoverySeq = null;
        }
        return;
      case "handler-failed":
        this.recoverFromDeliveryFailure(
          {
            cause: outcome.cause,
            event: outcome.event,
            handlerIndex: outcome.handlerIndex,
            reason: "event_handler_failed",
            timeoutMs: null,
          },
          generation,
        );
        return;
      case "handler-timeout":
        this.recoverFromDeliveryFailure(
          {
            event: outcome.event,
            handlerIndex: outcome.handlerIndex,
            reason: "event_handler_timeout",
            timeoutMs: outcome.timeoutMs,
          },
          generation,
        );
        return;
      case "invalid-server-envelope":
        this.enterPausedState("invalid_server_envelope", undefined, {}, generation);
        return;
      case "non-contiguous-event":
        this.enterPausedState(
          "non_contiguous_event",
          undefined,
          { expectedSeq: outcome.expectedSeq, observedSeq: outcome.observedSeq },
          generation,
        );
        return;
      case "replay-complete":
        this.settleReplayComplete();
        return;
    }
  }

  /** Applies bounded per-sequence recovery for one handler failure or timeout. */
  private recoverFromDeliveryFailure(
    input: {
      readonly cause?: unknown;
      readonly event: SessionEvent;
      readonly handlerIndex: number;
      readonly reason: "event_handler_failed" | "event_handler_timeout";
      readonly timeoutMs: number | null;
    },
    generation: number,
  ): void {
    if (this.stopped || this.pausedReason !== null || generation !== this.connectionGeneration) {
      return;
    }
    if (this.deliveryRecoverySeq !== input.event.seq) {
      this.deliveryRecoveryAttempts = 0;
      this.deliveryRecoverySeq = input.event.seq;
    }
    this.deliveryRecoveryAttempts += 1;
    this.deliveryFailureCount += 1;
    const error = new ParticipantRuntimeEventDeliveryError({
      attempt: this.deliveryRecoveryAttempts,
      ...(input.cause === undefined ? {} : { cause: input.cause }),
      eventId: input.event.eventId,
      handlerIndex: input.handlerIndex,
      reason: input.reason,
      seq: input.event.seq,
      ...(input.timeoutMs === null ? {} : { timeoutMs: input.timeoutMs }),
    });
    this.emitError(error);
    this.observability.debug("deliverEvent", "participant_event_delivery.failed", {
      attempt: error.attempt,
      eventId: error.eventId,
      generation,
      handlerIndex: error.handlerIndex,
      reason: error.reason,
      seq: error.seq,
    });
    if (this.deliveryRecoveryAttempts >= this.maxRecoveryAttempts) {
      this.enterPausedState(
        "delivery_retry_exhausted",
        error,
        { attempt: this.deliveryRecoveryAttempts, seq: input.event.seq },
        generation,
      );
      return;
    }
    this.recoveryCount += 1;
    const delayMs = reconnectDelayMs(this.deliveryRecoveryAttempts - 1, this.config);
    this.observability.debug("recoverDelivery", "participant_event_delivery.recovering", {
      attempt: this.deliveryRecoveryAttempts,
      delayMs,
      generation,
      handledSeq: this.lastHandledSeq,
      seq: input.event.seq,
    });
    this.settleReplayCompleteError(error);
    this.rejectPendingCommandsAsUnknown(error);
    const socket = this.socket;
    const closePromise = this.closePromise;
    this.socket = null;
    socket?.close();
    void closePromise.catch(() => undefined).then(() => this.requestReconnect(delayMs));
  }

  /** Enters terminal Paused State without opening another socket. */
  private enterPausedState(
    reason: ParticipantRuntimePausedReason,
    cause: unknown,
    safeDetails: Readonly<Record<string, number | string | null>>,
    generation: number,
  ): void {
    if (generation !== this.connectionGeneration || this.pausedReason !== null) {
      return;
    }
    this.pausedReason = reason;
    const error = new ParticipantRuntimeTerminalStreamError({
      ...(cause === undefined ? {} : { cause }),
      reason,
      safeDetails,
    });
    this.emitError(error);
    this.observability.debug("recoverDelivery", "participant_event_delivery.paused", {
      generation,
      reason,
      ...safeDetails,
    });
    this.settleReplayCompleteError(error);
    this.rejectPendingCommandsAsUnknown(error);
    const socket = this.socket;
    this.socket = null;
    socket?.close();
  }

  /**
   * Routes a connection-level or message-handling error to registered error
   * handlers instead of throwing out of a ws event listener, which would
   * surface as an unhandled exception.
   */
  private emitError(error: Error): void {
    if (this.errorHandlers.size === 0) {
      this.errorBacklog.push(error);
      return;
    }
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
    if (
      this.recentEvents.some(
        (recent) => recent.seq === event.seq && recent.eventId === event.eventId,
      )
    ) {
      return;
    }
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
  private recordHandledEvent(event: SessionEvent): void {
    if (event.seq <= this.lastHandledSeq) {
      return;
    }
    this.lastHandledSeq = event.seq;
    this.rememberEvent(event);
    this.cursorWriter.update(event.seq);
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
    this.cursorWriter.update(this.lastHandledSeq);
    void this.cursorWriter.flush();
  }

  /** Builds the sole serialized durable cursor writer for this runtime. */
  private createCursorWriter(acknowledgedSeq: number): ParticipantCursorWriter {
    return new ParticipantCursorWriter({
      acknowledgedSeq,
      onError: (error) => this.emitError(error),
      retryAttempts: resolvePositiveIntegerConfig(
        "cursorPersist.retryAttempts",
        this.config.cursorPersist?.retryAttempts,
        defaultCursorPersistRetryAttempts,
      ),
      retryBaseDelayMs: resolvePositiveIntegerConfig(
        "cursorPersist.retryBaseDelayMs",
        this.config.cursorPersist?.retryBaseDelayMs,
        defaultCursorPersistRetryBaseDelayMs,
      ),
      retryMaxDelayMs: resolvePositiveIntegerConfig(
        "cursorPersist.retryMaxDelayMs",
        this.config.cursorPersist?.retryMaxDelayMs,
        defaultCursorPersistRetryMaxDelayMs,
      ),
      ...(this.config.cursorStore === undefined
        ? {}
        : { store: { write: (seq: number) => this.config.cursorStore?.write(seq) } }),
      writeTimeoutMs: resolvePositiveIntegerConfig(
        "cursorPersist.writeTimeoutMs",
        this.config.cursorPersist?.writeTimeoutMs,
        defaultCursorPersistWriteTimeoutMs,
      ),
    });
  }

  /** Settles graceful shutdown phases while retaining incomplete phase detail. */
  private async settleGracefulShutdown(
    delivery: SerialEventDelivery<SessionEvent>,
    pendingPhases: Set<ParticipantRuntimeShutdownPhase>,
  ): Promise<void> {
    await delivery.waitForSettlement();
    pendingPhases.delete("delivery");
    this.cursorWriter.update(this.lastHandledSeq);
    const cursorOutcome = await this.cursorWriter.flush();
    if (cursorOutcome.status !== "pending") {
      pendingPhases.delete("cursor");
    }
    await this.closePromise;
    pendingPhases.delete("socket");
    if (pendingPhases.size > 0) {
      throw new ParticipantRuntimeShutdownError({
        pendingPhases: [...pendingPhases],
        reason: "incomplete",
        timeoutMs: this.shutdownTimeoutMs,
      });
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

  /** Rejects commands interrupted by recovery without implying safe retry. */
  private rejectPendingCommandsAsUnknown(cause: Error): void {
    const pendingCommands = [...this.pendingCommands.values()];
    this.pendingCommands.clear();
    for (const pending of pendingCommands) {
      clearTimeout(pending.timeout);
      pending.reject(
        new ParticipantRuntimeCommandOutcomeUnknownError({
          cause,
          op: pending.op,
          requestId: pending.requestId,
          ...(pending.taskId === undefined ? {} : { taskId: pending.taskId }),
        }),
      );
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

/** Applies one overall deadline to graceful participant shutdown settlement. */
async function withShutdownTimeout(
  settlement: Promise<void>,
  input: {
    readonly pendingPhases: ReadonlySet<ParticipantRuntimeShutdownPhase>;
    readonly timeoutMs: number;
  },
): Promise<void> {
  let timeoutHandle: ReturnType<typeof setTimeout> | null = null;
  const timeout = new Promise<never>((_resolve, reject) => {
    timeoutHandle = setTimeout(() => {
      reject(
        new ParticipantRuntimeShutdownError({
          pendingPhases: [...input.pendingPhases],
          reason: "timeout",
          timeoutMs: input.timeoutMs,
        }),
      );
    }, input.timeoutMs);
  });
  try {
    await Promise.race([settlement, timeout]);
  } finally {
    if (timeoutHandle !== null) {
      clearTimeout(timeoutHandle);
    }
  }
}

/**
 * Resolves the durable resume sequence from the store whenever it contains a
 * valid cursor. `afterSeq` is only the seed for an empty or absent store.
 */
export function resolveResumeSeq(afterSeq: number, storedSeq: number | null): number {
  return typeof storedSeq === "number" && Number.isFinite(storedSeq) && storedSeq >= 0
    ? storedSeq
    : afterSeq;
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

/** Resolves one optional finite positive integer participant policy value. */
function resolvePositiveIntegerConfig(
  field: string,
  value: number | undefined,
  defaultValue: number,
): number {
  const resolved = value ?? defaultValue;
  if (!Number.isInteger(resolved) || resolved <= 0) {
    throw new ParticipantRuntimeClientConfigurationError(
      field,
      `Participant runtime ${field} must be a finite positive integer`,
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
