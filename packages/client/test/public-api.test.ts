import { describe, expect, it } from "vitest";

import * as Client from "../src/index.js";

type PromiseReturning<TArgs extends readonly unknown[], TValue> = (
  ...args: TArgs
) => Promise<TValue>;

interface ParticipantRuntimePromiseContract {
  readonly claimTask: PromiseReturning<[string], Client.TaskRecord | null>;
  readonly completeTask: PromiseReturning<[string, Record<string, unknown>], void>;
  readonly connect: PromiseReturning<
    [Client.ParticipantRuntimeClientConfig],
    Client.ParticipantRuntimeClient
  >;
  readonly failTask: PromiseReturning<[string, Record<string, unknown>], void>;
  readonly reconnect: PromiseReturning<[], Client.ParticipantRuntimeClient>;
  readonly refreshTaskClaim: PromiseReturning<[string], Client.TaskRecord | null>;
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
  completeTask: Client.ParticipantRuntimeClient.prototype.completeTask,
  connect: Client.ParticipantRuntimeClient.connect,
  failTask: Client.ParticipantRuntimeClient.prototype.failTask,
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
      "completeTask",
      "connect",
      "failTask",
      "reconnect",
      "refreshTaskClaim",
      "runClaimableTasks",
      "runParticipantRuntime",
      "waitForClose",
      "waitForReplayComplete",
    ]);
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
  });
});
