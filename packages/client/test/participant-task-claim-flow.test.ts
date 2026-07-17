import { describe, expect, it } from "vitest";

import {
  invalidClaimDeadlineReason,
  type ParticipantTaskClaimFlowClient,
  type ParticipantTaskClaimFlowContext,
  type ParticipantTaskClaimFlowInput,
  type ParticipantTaskClaimFlowLogger,
  runParticipantTaskClaimFlow,
} from "../src/participant-task-claim-flow.js";
import type { TaskRecord } from "../src/types.js";

interface FlowHarness {
  readonly completedTaskIds: string[];
  readonly debugMessages: string[];
  readonly executorCalls: number;
  run(): Promise<void>;
}

describe("participant task claim flow claim deadline validation", () => {
  it("declines a claim whose deadline lacks an RFC 3339 offset before the executor runs", async () => {
    const harness = createHarness("2099-07-17T00:00:00");

    await harness.run();

    expect(harness.executorCalls).toBe(0);
    expect(harness.completedTaskIds).toEqual([]);
    expect(harness.debugMessages).toContain(invalidClaimDeadlineReason);
  });

  it("declines a claim with a null deadline before the executor runs", async () => {
    const harness = createHarness(null);

    await harness.run();

    expect(harness.executorCalls).toBe(0);
    expect(harness.debugMessages).toContain(invalidClaimDeadlineReason);
  });

  it("runs the executor when the deadline is a valid offset timestamp", async () => {
    const harness = createHarness("2099-07-17T00:00:00.000Z");

    await harness.run();

    expect(harness.executorCalls).toBe(1);
    expect(harness.completedTaskIds).toEqual(["task_1"]);
    expect(harness.debugMessages).not.toContain(invalidClaimDeadlineReason);
  });
});

/** Builds a claim-flow harness whose claim mints the supplied deadline. */
function createHarness(claimExpiresAt: string | null): FlowHarness {
  const debugMessages: string[] = [];
  const completedTaskIds: string[] = [];
  let executorCalls = 0;
  const claimedTask: TaskRecord = { ...createTaskFixture(), claimExpiresAt, claimId: "claim_1" };

  const client: ParticipantTaskClaimFlowClient = {
    appendEvent: async () => undefined,
    claimTask: async () => claimedTask,
    completeTask: async (taskId) => {
      completedTaskIds.push(taskId);
    },
    failTask: async () => undefined,
    refreshTaskClaim: async () => claimedTask,
  };
  const context: ParticipantTaskClaimFlowContext = {
    instanceId: "instance_1",
    lastObservedSeq: 0,
    participantId: "participant_1",
    recentEvents: [],
    sessionId: "sess_1",
  };
  const logger: ParticipantTaskClaimFlowLogger = {
    debug: (_boundary, message) => {
      debugMessages.push(message);
    },
  };
  const input: ParticipantTaskClaimFlowInput = {
    cancellation: {
      abortActive: () => undefined,
      isCancelled: () => false,
      signal: new AbortController().signal,
    },
    claimRefreshMs: 3_600_000,
    executor: async () => {
      executorCalls += 1;
      return { output: null, result: {} };
    },
    task: createTaskFixture(),
  };

  return {
    completedTaskIds,
    debugMessages,
    get executorCalls() {
      return executorCalls;
    },
    run: () => runParticipantTaskClaimFlow(client, context, logger, input),
  };
}

/** Builds a minimal claimable task fixture. */
function createTaskFixture(): TaskRecord {
  return {
    cancelledAt: null,
    claimExpiredAt: null,
    claimExpiredBy: null,
    claimExpiresAt: null,
    claimId: null,
    claimedAt: null,
    claimedBy: null,
    completedAt: null,
    createdAt: "2026-01-01T00:00:00.000Z",
    failedAt: null,
    failure: null,
    input: null,
    kind: "generic_request",
    objective: "handle request",
    releasedAt: null,
    releasedBy: null,
    result: null,
    sessionId: "sess_1",
    taskId: "task_1",
  };
}
