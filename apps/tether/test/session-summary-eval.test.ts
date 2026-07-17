import type { SessionSummaryContent } from "@dungle-scrubs/tether-protocol";
import { describe, expect, it } from "vitest";

import {
  scoreSessionSummaryCase,
  type SessionSummaryEvalCase,
} from "../evals/session-summary/scorer.js";

describe("Session Summary evaluation", () => {
  it("rejects a schema-valid summary that invents an approval identity", () => {
    const evalCase: SessionSummaryEvalCase = {
      allowedIdentities: {
        approval: ["approval_real"],
        decision: [],
        participant: [],
        task: ["task_email"],
      },
      eventIds: ["evt_approval_requested"],
      forbiddenClaims: [],
      id: "invented-approval",
      maxOutputBytes: 4_096,
      projection: {
        activity: "settled",
        archived: false,
        deleted: false,
        title: "Send launch email",
      },
      requiredFacts: [],
      sensitiveValues: [],
    };
    const candidate: SessionSummaryContent = {
      facts: [
        {
          category: "approval",
          sourceEventIds: ["evt_approval_requested"],
          statement: "Approval approval_invented was granted.",
          subjectIds: ["approval_invented", "task_email"],
        },
      ],
      headline: "Launch email approved",
      narrative: "The launch email is ready to send.",
      openQuestions: [],
    };

    const result = scoreSessionSummaryCase(evalCase, candidate);

    expect(result.passed).toBe(false);
    expect(result.gates.identity).toEqual({
      failures: ["invented approval identity: approval_invented"],
      passed: false,
    });
  });

  it("rejects output that does not satisfy the structured summary schema", () => {
    const result = scoreSessionSummaryCase(
      {
        allowedIdentities: { approval: [], decision: [], participant: [], task: [] },
        eventIds: [],
        forbiddenClaims: [],
        id: "invalid-schema",
        maxOutputBytes: 4_096,
        projection: {
          activity: "idle",
          archived: false,
          deleted: false,
          title: "Empty session",
        },
        requiredFacts: [],
        sensitiveValues: [],
      },
      { headline: "Missing required fields" },
    );

    expect(result.passed).toBe(false);
    expect(result.gates.schema.passed).toBe(false);
    expect(result.gates.schema.failures).toEqual(["structured summary schema validation failed"]);
  });

  it("rejects structured output above the case byte limit", () => {
    const result = scoreSessionSummaryCase(
      {
        allowedIdentities: { approval: [], decision: [], participant: [], task: [] },
        eventIds: [],
        forbiddenClaims: [],
        id: "bounded-output",
        maxOutputBytes: 100,
        projection: {
          activity: "idle",
          archived: false,
          deleted: false,
          title: "Bounded output",
        },
        requiredFacts: [],
        sensitiveValues: [],
      },
      {
        facts: [],
        headline: "A valid but oversized summary",
        narrative: "x".repeat(200),
        openQuestions: [],
      },
    );

    expect(result.passed).toBe(false);
    expect(result.gates.outputSize).toEqual({
      failures: ["structured summary output exceeds 100 bytes"],
      passed: false,
    });
  });

  it("rejects a candidate that omits a designated critical fact tuple", () => {
    const result = scoreSessionSummaryCase(
      {
        allowedIdentities: {
          approval: ["approval_send"],
          decision: [],
          participant: [],
          task: ["task_send"],
        },
        eventIds: ["evt_approval_granted"],
        forbiddenClaims: [],
        id: "critical-approval",
        maxOutputBytes: 4_096,
        projection: {
          activity: "settled",
          archived: false,
          deleted: false,
          title: "Send launch email",
        },
        requiredFacts: [
          {
            category: "approval",
            sourceEventIds: ["evt_approval_granted"],
            statements: ["Approval approval_send was granted for task_send."],
            subjectIds: ["approval_send", "task_send"],
          },
        ],
        sensitiveValues: [],
      },
      {
        facts: [],
        headline: "Launch email",
        narrative: "The launch email task exists.",
        openQuestions: [],
      },
    );

    expect(result.passed).toBe(false);
    expect(result.gates.criticalFacts).toEqual({
      failures: ["missing critical fact tuple: approval/evt_approval_granted"],
      passed: false,
    });
  });

  it("rejects source event identities outside the covered range", () => {
    const result = scoreSessionSummaryCase(
      {
        allowedIdentities: { approval: [], decision: [], participant: [], task: [] },
        eventIds: ["evt_covered"],
        forbiddenClaims: [],
        id: "invented-event",
        maxOutputBytes: 4_096,
        projection: {
          activity: "idle",
          archived: false,
          deleted: false,
          title: "Covered range",
        },
        requiredFacts: [],
        sensitiveValues: [],
      },
      {
        facts: [
          {
            category: "other",
            sourceEventIds: ["evt_outside_range"],
            statement: "An event happened.",
            subjectIds: [],
          },
        ],
        headline: "Event",
        narrative: "An event happened.",
        openQuestions: [],
      },
    );

    expect(result.passed).toBe(false);
    expect(result.gates.identity.failures).toEqual(["invented event identity: evt_outside_range"]);
  });

  it("rejects designated sensitive canaries copied into structured output", () => {
    const result = scoreSessionSummaryCase(
      {
        allowedIdentities: { approval: [], decision: [], participant: [], task: [] },
        eventIds: [],
        forbiddenClaims: [],
        id: "sensitive-canary",
        maxOutputBytes: 4_096,
        projection: {
          activity: "idle",
          archived: false,
          deleted: false,
          title: "Sensitive input",
        },
        requiredFacts: [],
        sensitiveValues: ["EVAL_SECRET_CANARY_7F3A"],
      },
      {
        facts: [],
        headline: "Sensitive input",
        narrative: "The supplied credential was EVAL_SECRET_CANARY_7F3A.",
        openQuestions: [],
      },
    );

    expect(result.passed).toBe(false);
    expect(result.gates.sensitivity).toEqual({
      failures: ["designated sensitive value leaked: sensitive_0"],
      passed: false,
    });
  });

  it("rejects an exact unsafe handled-claim canary", () => {
    const result = scoreSessionSummaryCase(
      {
        allowedIdentities: { approval: [], decision: [], participant: [], task: [] },
        eventIds: [],
        forbiddenClaims: ["All exact events were handled."],
        id: "unsafe-handled-claim",
        maxOutputBytes: 4_096,
        projection: {
          activity: "idle",
          archived: false,
          deleted: false,
          title: "Unsafe claim",
        },
        requiredFacts: [],
        sensitiveValues: [],
      },
      {
        facts: [],
        headline: "Unsafe claim",
        narrative: "All exact events were handled.",
        openQuestions: [],
      },
    );

    expect(result.passed).toBe(false);
    expect(result.gates.forbiddenClaims).toEqual({
      failures: ["forbidden exact claim emitted: forbidden_0"],
      passed: false,
    });
  });

  it("rejects an exact contradiction of authoritative projection state", () => {
    const result = scoreSessionSummaryCase(
      {
        allowedIdentities: { approval: [], decision: [], participant: [], task: [] },
        eventIds: [],
        forbiddenClaims: [],
        id: "projection-contradiction",
        maxOutputBytes: 4_096,
        projection: {
          activity: "idle",
          archived: false,
          deleted: false,
          title: "Active session",
        },
        projectionContradictions: ["Session is archived."],
        requiredFacts: [],
        sensitiveValues: [],
      },
      {
        facts: [],
        headline: "Active session",
        narrative: "Session is archived.",
        openQuestions: [],
      },
    );

    expect(result.passed).toBe(false);
    expect(result.gates.projection).toEqual({
      failures: ["authoritative projection contradicted: projection_0"],
      passed: false,
    });
  });

  it("rejects real identities used as the wrong durable subject kind", () => {
    const result = scoreSessionSummaryCase(
      {
        allowedIdentities: {
          approval: [],
          decision: ["decision_real"],
          participant: ["participant_real"],
          task: ["task_real"],
        },
        eventIds: ["evt_task", "evt_participant", "evt_decision"],
        forbiddenClaims: [],
        id: "wrong-identity-kind",
        maxOutputBytes: 4_096,
        projection: {
          activity: "idle",
          archived: false,
          deleted: false,
          title: "Identity kinds",
        },
        requiredFacts: [],
        sensitiveValues: [],
      },
      {
        facts: [
          {
            category: "task",
            sourceEventIds: ["evt_task"],
            statement: "A task exists.",
            subjectIds: ["participant_real"],
          },
          {
            category: "participant",
            sourceEventIds: ["evt_participant"],
            statement: "A participant exists.",
            subjectIds: ["task_real"],
          },
          {
            category: "decision",
            sourceEventIds: ["evt_decision"],
            statement: "A decision exists.",
            subjectIds: ["task_real"],
          },
        ],
        headline: "Identity kinds",
        narrative: "Durable identities are present.",
        openQuestions: [],
      },
    );

    expect(result.gates.identity.failures).toEqual([
      "invented task identity: participant_real",
      "invented participant identity: task_real",
      "invented decision identity: task_real",
    ]);
  });
});
