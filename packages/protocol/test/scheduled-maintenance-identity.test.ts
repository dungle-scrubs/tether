import { describe, expect, it } from "vitest";

import {
  classifyScheduledSupersession,
  computeScheduleWindow,
  deriveScheduledTaskId,
  scheduledSupersessionResultSchema,
  scheduleWindowAlgorithmVersion,
  scheduleWindowKey,
  type MailboxScope,
  type ScheduledMaintenanceIdentity,
  type ScheduledSupersessionCandidate,
} from "../src/index.js";

const mailboxScope: MailboxScope = { accountId: "acct_opaque_1", provider: "fastmail" };

describe("computeScheduleWindow", () => {
  it("buckets a timestamp into the half-open UTC interval", () => {
    const window = computeScheduleWindow(1_700_000_123_456, 3_600_000);
    expect(window).toEqual({
      algorithmVersion: scheduleWindowAlgorithmVersion,
      endMs: 1_700_002_800_000,
      intervalMs: 3_600_000,
      startMs: 1_699_999_200_000,
    });
    // Half-open: start is inclusive, end is exclusive.
    expect(window.startMs % 3_600_000).toBe(0);
    expect(window.endMs - window.startMs).toBe(3_600_000);
  });

  it("resolves a rolled-back clock inside the same window to the same bucket", () => {
    const first = computeScheduleWindow(1_700_000_123_456, 3_600_000);
    const rolledBack = computeScheduleWindow(1_700_000_000_001, 3_600_000);
    expect(rolledBack.startMs).toBe(first.startMs);
  });

  it("rejects a non-positive or fractional interval", () => {
    expect(() => computeScheduleWindow(1_700_000_000_000, 0)).toThrow();
    expect(() => computeScheduleWindow(1_700_000_000_000, -60_000)).toThrow();
    expect(() => computeScheduleWindow(1_700_000_000_000, 1_000.5)).toThrow();
  });
});

describe("deriveScheduledTaskId", () => {
  const identity: ScheduledMaintenanceIdentity = {
    kind: "email_organization",
    mailboxScope,
    scheduleWindow: computeScheduleWindow(1_700_000_123_456, 3_600_000),
    sessionId: "sess_mailbox_1",
  };

  it("is stable and deterministic for one identity", () => {
    expect(deriveScheduledTaskId(identity)).toBe(deriveScheduledTaskId(identity));
    expect(deriveScheduledTaskId(identity)).toMatch(/^task_sched_[0-9a-f]+$/u);
  });

  it("changes when the mailbox scope, kind, interval, or window changes", () => {
    const base = deriveScheduledTaskId(identity);
    expect(
      deriveScheduledTaskId({
        ...identity,
        mailboxScope: { accountId: "acct_opaque_2", provider: "fastmail" },
      }),
    ).not.toBe(base);
    expect(
      deriveScheduledTaskId({
        ...identity,
        mailboxScope: { accountId: "acct_opaque_1", provider: "gmail" },
      }),
    ).not.toBe(base);
    expect(deriveScheduledTaskId({ ...identity, kind: "email_triage" })).not.toBe(base);
    expect(
      deriveScheduledTaskId({
        ...identity,
        scheduleWindow: computeScheduleWindow(1_700_000_123_456, 1_800_000),
      }),
    ).not.toBe(base);
    expect(
      deriveScheduledTaskId({
        ...identity,
        scheduleWindow: computeScheduleWindow(1_700_100_000_000, 3_600_000),
      }),
    ).not.toBe(base);
  });

  it("changes when the schedule algorithm version changes", () => {
    const base = deriveScheduledTaskId(identity);
    const nextAlgorithm: ScheduledMaintenanceIdentity = {
      ...identity,
      scheduleWindow: { ...identity.scheduleWindow, algorithmVersion: 2 },
    };
    expect(deriveScheduledTaskId(nextAlgorithm)).not.toBe(base);
  });
});

describe("scheduleWindowKey", () => {
  it("encodes algorithm version, interval, and start", () => {
    const window = computeScheduleWindow(1_700_000_123_456, 3_600_000);
    expect(scheduleWindowKey(window)).toBe("v1:3600000:1699999200000");
  });
});

describe("classifyScheduledSupersession", () => {
  const currentWindow = computeScheduleWindow(1_700_003_600_000, 3_600_000);
  const olderWindow = computeScheduleWindow(1_700_000_000_000, 3_600_000);
  const target: ScheduledMaintenanceIdentity = {
    kind: "email_organization",
    mailboxScope,
    scheduleWindow: currentWindow,
    sessionId: "sess_mailbox_1",
  };

  const olderPendingCandidate: ScheduledSupersessionCandidate = {
    cancelledAt: null,
    claimedBy: null,
    completedAt: null,
    failedAt: null,
    kind: "email_organization",
    schedule: { mailboxScope, scheduleWindow: olderWindow },
    sessionId: "sess_mailbox_1",
  };

  it("supersedes an older, matching, unclaimed, nonterminal run", () => {
    expect(classifyScheduledSupersession(olderPendingCandidate, target)).toEqual({
      decision: "supersede",
    });
  });

  it("refuses a manual task without schedule identity", () => {
    expect(
      classifyScheduledSupersession({ ...olderPendingCandidate, schedule: null }, target),
    ).toEqual({ decision: "refuse", reason: "manual" });
  });

  it("refuses a task claimed by a worker", () => {
    expect(
      classifyScheduledSupersession({ ...olderPendingCandidate, claimedBy: "part_worker" }, target),
    ).toEqual({ decision: "refuse", reason: "claimed" });
  });

  it("refuses a terminal task", () => {
    expect(
      classifyScheduledSupersession(
        { ...olderPendingCandidate, completedAt: "2026-07-12T00:00:00.000Z" },
        target,
      ),
    ).toEqual({ decision: "refuse", reason: "terminal" });
    expect(
      classifyScheduledSupersession(
        { ...olderPendingCandidate, cancelledAt: "2026-07-12T00:00:00.000Z" },
        target,
      ),
    ).toEqual({ decision: "refuse", reason: "terminal" });
  });

  it("refuses a task whose schedule identity does not match", () => {
    expect(
      classifyScheduledSupersession(
        {
          ...olderPendingCandidate,
          schedule: {
            mailboxScope: { accountId: "acct_other", provider: "fastmail" },
            scheduleWindow: olderWindow,
          },
        },
        target,
      ),
    ).toEqual({ decision: "refuse", reason: "schedule_identity_mismatch" });
  });

  it("refuses the current or newer window run so it is never cancelled", () => {
    expect(
      classifyScheduledSupersession(
        { ...olderPendingCandidate, schedule: { mailboxScope, scheduleWindow: currentWindow } },
        target,
      ),
    ).toEqual({ decision: "refuse", reason: "current_window" });
  });

  it("treats a run returned to pending after claim expiry as eligible again", () => {
    // After expiry the sweeper clears claimedBy, so the same run is eligible.
    const expiredThenPending: ScheduledSupersessionCandidate = {
      ...olderPendingCandidate,
      claimedBy: null,
    };
    expect(classifyScheduledSupersession(expiredThenPending, target)).toEqual({
      decision: "supersede",
    });
  });
});

describe("scheduledSupersessionResultSchema", () => {
  it("validates a typed supersession result with refusals", () => {
    const parsed = scheduledSupersessionResultSchema.parse({
      refusals: [{ reason: "claimed", taskId: "task_sched_abc" }],
      supersededTaskIds: ["task_sched_def"],
    });
    expect(parsed.refusals[0]?.reason).toBe("claimed");
    expect(parsed.supersededTaskIds).toEqual(["task_sched_def"]);
  });

  it("rejects an unknown refusal reason", () => {
    expect(() =>
      scheduledSupersessionResultSchema.parse({
        refusals: [{ reason: "nope", taskId: "task_sched_abc" }],
        supersededTaskIds: [],
      }),
    ).toThrow();
  });
});
