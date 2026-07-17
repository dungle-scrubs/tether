import { describe, expect, expectTypeOf, it } from "vitest";

import * as Client from "../src/index.js";

type PromiseReturning<TArgs extends readonly unknown[], TValue> = (
  ...args: TArgs
) => Promise<TValue>;

interface ParticipantRuntimePromiseContract {
  readonly claimTask: PromiseReturning<[string], Client.TaskRecord | null>;
  readonly completeTask: PromiseReturning<[string, Record<string, unknown>, string], void>;
  readonly closeAndWait: PromiseReturning<[], void>;
  readonly connect: PromiseReturning<
    [Client.ParticipantRuntimeClientConfig],
    Client.ParticipantRuntimeClient
  >;
  readonly create: PromiseReturning<
    [Client.ParticipantRuntimeClientConfig],
    Client.ParticipantRuntimeClient
  >;
  readonly failTask: PromiseReturning<[string, Record<string, unknown>, string], void>;
  readonly reconnect: PromiseReturning<[], Client.ParticipantRuntimeClient>;
  readonly open: PromiseReturning<[], Client.ParticipantRuntimeClient>;
  readonly refreshTaskClaim: PromiseReturning<[string, string], Client.TaskRecord | null>;
  readonly runClaimableTasks: PromiseReturning<[Client.ParticipantRuntimeTaskLoopOptions], void>;
  readonly runParticipantRuntime: PromiseReturning<[Client.RunParticipantRuntimeInput], void>;
  readonly waitForClose: PromiseReturning<[], void>;
  readonly waitForReplayComplete: PromiseReturning<[], void>;
}

interface SessionEventStreamPromiseContract {
  readonly connect: PromiseReturning<
    [Client.SessionEventStreamClientConfig],
    Client.SessionEventStreamClient
  >;
  readonly reconnect: PromiseReturning<[], Client.SessionEventStreamClient>;
  readonly waitForClose: PromiseReturning<[], void>;
  readonly waitForReplayComplete: PromiseReturning<[], void>;
}

const participantRuntimePromiseContract: ParticipantRuntimePromiseContract = {
  claimTask: Client.ParticipantRuntimeClient.prototype.claimTask,
  closeAndWait: Client.ParticipantRuntimeClient.prototype.closeAndWait,
  completeTask: Client.ParticipantRuntimeClient.prototype.completeTask,
  connect: Client.ParticipantRuntimeClient.connect,
  create: Client.ParticipantRuntimeClient.create,
  failTask: Client.ParticipantRuntimeClient.prototype.failTask,
  open: Client.ParticipantRuntimeClient.prototype.open,
  reconnect: Client.ParticipantRuntimeClient.prototype.reconnect,
  refreshTaskClaim: Client.ParticipantRuntimeClient.prototype.refreshTaskClaim,
  runClaimableTasks: Client.ParticipantRuntimeClient.prototype.runClaimableTasks,
  runParticipantRuntime: Client.runParticipantRuntime,
  waitForClose: Client.ParticipantRuntimeClient.prototype.waitForClose,
  waitForReplayComplete: Client.ParticipantRuntimeClient.prototype.waitForReplayComplete,
};

const sessionEventStreamPromiseContract: SessionEventStreamPromiseContract = {
  connect: Client.SessionEventStreamClient.connect,
  reconnect: Client.SessionEventStreamClient.prototype.reconnect,
  waitForClose: Client.SessionEventStreamClient.prototype.waitForClose,
  waitForReplayComplete: Client.SessionEventStreamClient.prototype.waitForReplayComplete,
};

describe("@dungle-scrubs/tether-client public API", () => {
  it("keeps participant runtime APIs Promise-based", () => {
    expect(Object.keys(participantRuntimePromiseContract).sort()).toEqual([
      "claimTask",
      "closeAndWait",
      "completeTask",
      "connect",
      "create",
      "failTask",
      "open",
      "reconnect",
      "refreshTaskClaim",
      "runClaimableTasks",
      "runParticipantRuntime",
      "waitForClose",
      "waitForReplayComplete",
    ]);
  });

  it("types participant event handlers as synchronous or asynchronous", () => {
    type ParticipantEventHandler = Client.ParticipantRuntimeEventHandler;

    expectTypeOf<ReturnType<ParticipantEventHandler>>().toEqualTypeOf<void | Promise<void>>();
  });

  it("types subscriber-first lifecycle hooks and finite runtime policies", () => {
    const hooks = {
      onClientReady: (client: Client.ParticipantRuntimeClient) => client.onEvent(() => undefined),
      onReplayComplete: async (_client: Client.ParticipantRuntimeClient) => {
        await Promise.resolve();
        return () => undefined;
      },
    } satisfies Client.RunParticipantRuntimeHooks;
    const config = {
      afterSeq: 0,
      capabilities: {},
      cursorPersist: {
        retryAttempts: 5,
        retryBaseDelayMs: 100,
        retryMaxDelayMs: 2_000,
        writeTimeoutMs: 5_000,
      },
      displayName: "Runtime",
      eventDelivery: {
        handlerTimeoutMs: 30_000,
        maxQueueSize: 2_000,
        maxRecoveryAttempts: 5,
      },
      instanceId: "inst_1",
      participantId: "part_1",
      runtimeKind: "generic_agent",
      serviceUrl: "http://127.0.0.1:4123",
      sessionId: "sess_1",
      shutdownTimeoutMs: 30_000,
    } satisfies Client.ParticipantRuntimeClientConfig;

    expectTypeOf(hooks).toMatchTypeOf<Client.RunParticipantRuntimeHooks>();
    expectTypeOf(config).toMatchTypeOf<Client.ParticipantRuntimeClientConfig>();
  });

  it("exports typed participant recovery errors and additive diagnostics", () => {
    const recoveryErrors: readonly (new (...args: never[]) => Error)[] = [
      Client.ParticipantRuntimeCommandOutcomeUnknownError,
      Client.ParticipantRuntimeCursorPersistError,
      Client.ParticipantRuntimeEventDeliveryError,
      Client.ParticipantRuntimeShutdownError,
      Client.ParticipantRuntimeTerminalStreamError,
    ];
    type ParticipantDiagnostics = Pick<
      Client.ParticipantRuntimeClientDebugInfo,
      | "activeDeliverySeq"
      | "connectionGeneration"
      | "cursorPersistFailureCount"
      | "eventDeliveryFailureCount"
      | "lastHandledSeq"
      | "lastObservedSeq"
      | "lastPersistedSeq"
      | "lastReceivedSeq"
      | "pausedReason"
      | "pendingCursorSeq"
      | "recoveryCount"
    >;

    expect(recoveryErrors).toHaveLength(5);
    expectTypeOf<ParticipantDiagnostics["lastObservedSeq"]>().toEqualTypeOf<number>();
    expectTypeOf<ParticipantDiagnostics["lastHandledSeq"]>().toEqualTypeOf<number>();
  });

  it("does not export Effect as a top-level adapter API", () => {
    expect(Object.keys(Client)).not.toContain("Effect");
  });

  it("re-exports protocol-owned diagnostic record types", () => {
    const summary = {
      controlLeases: {
        active: 0,
        expired: 0,
        released: 0,
        superseded: 0,
        total: 0,
      },
      participants: {
        activeControl: 0,
        leaseOnly: 0,
        registered: 0,
        total: 0,
        withoutActiveControl: 0,
      },
      sessionId: "sess_1",
      tasks: {
        activeClaims: 0,
        cancelled: 0,
        claimable: 0,
        claimActive: 0,
        claimCleared: 0,
        claimExpired: 0,
        completed: 0,
        expiredClaims: 0,
        failed: 0,
        terminal: 0,
        total: 0,
        unclaimed: 0,
      },
    } satisfies Client.SessionDebugSummary;

    expect(summary.sessionId).toBe("sess_1");
  });

  it("keeps observer stream APIs Promise-based without task command methods", () => {
    expect(Object.keys(sessionEventStreamPromiseContract).sort()).toEqual([
      "connect",
      "reconnect",
      "waitForClose",
      "waitForReplayComplete",
    ]);
    expect(Object.getOwnPropertyNames(Client.SessionEventStreamClient.prototype)).not.toEqual(
      expect.arrayContaining(["appendEvent", "claimTask", "completeTask", "refreshTaskClaim"]),
    );
    expect(new Client.SessionEventStreamError({ message: "Replay failed" })).toMatchObject({
      reason: null,
      safeDetails: {},
    });
  });
});
