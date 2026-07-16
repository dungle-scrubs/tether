import { describe, expect, it } from "vitest";
import type { z } from "zod";
import { ZodError } from "zod";

import type { CandidateScheduleIdentity, TaskRecord } from "../src/index.js";
import {
  candidateScheduleIdentitySchema,
  formatTaskFailurePayload,
  formatTaskResultPayload,
  scheduledTaskIdentitySchema,
  taskContractAdvertisementSchema,
  taskContractSummarySchema,
  taskRecordSchema,
} from "../src/index.js";

type IsExact<TLeft, TRight> = [TLeft] extends [TRight]
  ? [TRight] extends [TLeft]
    ? true
    : false
  : false;

const taskRecordTypeParity: IsExact<TaskRecord, z.output<typeof taskRecordSchema>> = true;

describe("taskRecordSchema", () => {
  it("exactly matches the static TaskRecord type in both directions", () => {
    expect(taskRecordTypeParity).toBe(true);
  });

  it("validates the shared durable task record shape", () => {
    const task = createTaskFixture();
    const parsed = taskRecordSchema.parse(task);

    expect(parsed).toEqual(task);
    expect(parsed).not.toHaveProperty("schedule");
  });

  it("preserves structured task input when present", () => {
    const task = createTaskFixture({
      input: {
        candidate: {
          mediaType: "movie",
          title: "Greenland 2: Migration",
          tmdbId: 840464,
        },
        searchOnAdd: true,
      },
    });

    expect(taskRecordSchema.parse(task).input).toEqual(task.input);
  });

  it("preserves a valid scheduled task identity", () => {
    const task: TaskRecord = {
      ...createTaskFixture(),
      schedule: createScheduleFixture(),
    };

    expect(taskRecordSchema.parse(task)).toEqual(task);
  });

  it("preserves a validated schedule through a JSON round trip", () => {
    const task = taskRecordSchema.parse({
      ...createTaskFixture(),
      schedule: createScheduleFixture(),
    });
    const roundTripped = JSON.parse(JSON.stringify(task)) as unknown;

    expect(taskRecordSchema.parse(roundTripped).schedule).toEqual(task.schedule);
  });

  it("preserves null schedule identity for manual tasks", () => {
    const task: TaskRecord = { ...createTaskFixture(), schedule: null };

    expect(taskRecordSchema.parse(task)).toEqual(task);
  });

  it("strips unknown Mailbox Scope properties", () => {
    const schedule = createScheduleFixture();
    const parsed = taskRecordSchema.parse({
      ...createTaskFixture(),
      schedule: {
        ...schedule,
        mailboxScope: { ...schedule.mailboxScope, ignored: true },
      },
    });

    expect(parsed.schedule?.mailboxScope).toEqual(schedule.mailboxScope);
  });

  it("strips unknown Schedule Window properties", () => {
    const schedule = createScheduleFixture();
    const parsed = taskRecordSchema.parse({
      ...createTaskFixture(),
      schedule: {
        ...schedule,
        scheduleWindow: { ...schedule.scheduleWindow, ignored: true },
      },
    });

    expect(parsed.schedule?.scheduleWindow).toEqual(schedule.scheduleWindow);
  });

  it("rejects an empty Mailbox Scope account identifier", () => {
    const schedule = createScheduleFixture();

    expect(
      candidateScheduleIdentitySchema.safeParse({
        ...schedule,
        mailboxScope: { ...schedule.mailboxScope, accountId: "" },
      }).success,
    ).toBe(false);
  });

  it("rejects an empty Mailbox Scope provider", () => {
    const schedule = createScheduleFixture();

    expect(
      candidateScheduleIdentitySchema.safeParse({
        ...schedule,
        mailboxScope: { ...schedule.mailboxScope, provider: "" },
      }).success,
    ).toBe(false);
  });

  it("rejects a present schedule missing either Mailbox Scope field", () => {
    const schedule = createScheduleFixture();
    const incompleteMailboxScopes = [
      { accountId: schedule.mailboxScope.accountId },
      { provider: schedule.mailboxScope.provider },
    ];

    for (const mailboxScope of incompleteMailboxScopes) {
      expect(() =>
        taskRecordSchema.parse({
          ...createTaskFixture(),
          schedule: { ...schedule, mailboxScope },
        }),
      ).toThrowError(ZodError);
    }
  });

  it("rejects a present schedule missing any Schedule Window field", () => {
    const schedule = createScheduleFixture();
    const { algorithmVersion, endMs, intervalMs, startMs } = schedule.scheduleWindow;
    const incompleteScheduleWindows = [
      { endMs, intervalMs, startMs },
      { algorithmVersion, intervalMs, startMs },
      { algorithmVersion, endMs, startMs },
      { algorithmVersion, endMs, intervalMs },
    ];

    for (const scheduleWindow of incompleteScheduleWindows) {
      expect(() =>
        taskRecordSchema.parse({
          ...createTaskFixture(),
          schedule: { ...schedule, scheduleWindow },
        }),
      ).toThrowError(ZodError);
    }
  });

  it("keeps scheduled task creation flat and aligned with schedule leaf constraints", () => {
    const identity = {
      mailboxAccountId: "acct_opaque_1",
      mailboxProvider: "fastmail",
      scheduleAlgorithmVersion: 1,
      scheduleIntervalMs: 3_600_000,
      scheduleWindowStart: 1_699_999_200_000,
    };

    expect(scheduledTaskIdentitySchema.parse(identity)).toEqual(identity);
    expect(scheduledTaskIdentitySchema.parse({ ...identity, endMs: 1_700_002_800_000 })).toEqual(
      identity,
    );

    const invalidIdentities = [
      { ...identity, mailboxAccountId: "" },
      { ...identity, mailboxProvider: "" },
      { ...identity, scheduleAlgorithmVersion: 0 },
      { ...identity, scheduleAlgorithmVersion: Number.MAX_SAFE_INTEGER + 1 },
      { ...identity, scheduleIntervalMs: 0 },
      { ...identity, scheduleIntervalMs: Number.MAX_SAFE_INTEGER + 1 },
      { ...identity, scheduleWindowStart: -1 },
      { ...identity, scheduleWindowStart: Number.MAX_SAFE_INTEGER + 1 },
    ];

    for (const invalidIdentity of invalidIdentities) {
      expect(scheduledTaskIdentitySchema.safeParse(invalidIdentity).success).toBe(false);
    }
  });

  it("rejects a scheduled creation window whose start-plus-interval sum is unsafe", () => {
    expect(
      scheduledTaskIdentitySchema.safeParse({
        mailboxAccountId: "acct_opaque_1",
        mailboxProvider: "fastmail",
        scheduleAlgorithmVersion: 1,
        scheduleIntervalMs: 2,
        scheduleWindowStart: Number.MAX_SAFE_INTEGER - 1,
      }).success,
    ).toBe(false);
  });
});

describe("task payload display formatting", () => {
  it("formats string output payloads", () => {
    expect(formatTaskResultPayload({ output: "Inbox organized." })).toEqual({
      label: "Result",
      lines: ["Inbox organized."],
    });
  });

  it("formats summary and action payloads", () => {
    expect(
      formatTaskResultPayload({
        actions: ["Grouped newsletters", "Flagged replies"],
        summary: "Inbox organized.",
      }),
    ).toEqual({
      label: "Result",
      lines: ["Inbox organized.", "- Grouped newsletters", "- Flagged replies"],
    });
  });

  it("formats error payloads", () => {
    expect(formatTaskFailurePayload({ error: "Provider failed." })).toEqual({
      label: "Failure",
      lines: ["Provider failed."],
    });
  });

  it("falls back to stable JSON for unknown payloads", () => {
    expect(formatTaskResultPayload({ count: 2 })).toEqual({
      label: "Result",
      lines: ['{"count":2}'],
    });
  });

  it("formats empty payloads as none", () => {
    expect(formatTaskResultPayload(null)).toEqual({
      label: "Result",
      lines: ["none"],
    });
  });
});

describe("task contract schemas", () => {
  it("validates the common Tether contract envelope and domain advertisement shape", () => {
    const summary = {
      approval: "required_for_mutation",
      description: "Prepare a provider dry run before approval.",
      inputJsonSchema: { type: "object" },
      inputSchemaRef: "task-contract:generic_request:v1:input",
      participantRuntimeKind: "generic_agent",
      readOnlyByDefault: true,
      resultJsonSchema: { type: "object" },
      resultSchemaRef: "task-contract:generic_request:v1:result",
      taskKind: "generic_request",
      title: "Generic request",
      version: "1",
    };

    expect(taskContractSummarySchema.parse(summary)).toEqual(summary);
    expect(
      taskContractAdvertisementSchema.parse({
        common: summary,
        domain: {
          inputJsonSchema: { type: "object" },
          resultJsonSchema: { type: "object" },
        },
      }),
    ).toMatchObject({
      common: { participantRuntimeKind: "generic_agent", taskKind: "generic_request" },
      domain: { inputJsonSchema: { type: "object" } },
    });
  });
});

/** Builds a complete durable schedule identity fixture. */
function createScheduleFixture(): CandidateScheduleIdentity {
  return {
    mailboxScope: {
      accountId: "acct_opaque_1",
      provider: "fastmail",
    },
    scheduleWindow: {
      algorithmVersion: 1,
      endMs: 1_700_002_800_000,
      intervalMs: 3_600_000,
      startMs: 1_699_999_200_000,
    },
  };
}

/**
 * Builds a complete durable task record fixture.
 */
function createTaskFixture(
  input: { readonly input?: Record<string, unknown> | null } = {},
): TaskRecord {
  return {
    cancelledAt: null,
    claimExpiredAt: null,
    claimExpiredBy: null,
    claimExpiresAt: null,
    claimedAt: null,
    claimedBy: null,
    completedAt: null,
    createdAt: "2026-01-01T00:00:00.000Z",
    failedAt: null,
    failure: null,
    input: input.input ?? null,
    kind: "generic_request",
    objective: "handle request",
    releasedAt: null,
    releasedBy: null,
    result: null,
    sessionId: "sess_1",
    taskId: "task_1",
  };
}
