import { describe, expect, it } from "vitest";

import {
  formatTaskFailurePayload,
  formatTaskResultPayload,
  taskContractAdvertisementSchema,
  taskContractSummarySchema,
  taskRecordSchema,
  type TaskRecord,
} from "../src/index.js";

describe("taskRecordSchema", () => {
  it("validates the shared durable task record shape", () => {
    const task = createTaskFixture();

    expect(taskRecordSchema.parse(task)).toEqual(task);
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
