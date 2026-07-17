import { describe, expect, it } from "vitest";

import { deriveTaskSnapshotStatus } from "../src/db.js";
import type { TaskRecord } from "../src/types.js";

const baseTask: TaskRecord = {
  cancelledAt: null,
  claimExpiredAt: null,
  claimExpiredBy: null,
  claimExpiresAt: null,
  claimId: null,
  claimedAt: null,
  claimedBy: null,
  completedAt: null,
  createdAt: "2026-05-22T00:00:00.000Z",
  failedAt: null,
  failure: null,
  input: null,
  kind: "software_dev",
  objective: "Inspect task state",
  releasedAt: null,
  releasedBy: null,
  result: null,
  sessionId: "sess_task_snapshot",
  taskId: "task_snapshot",
};

describe("deriveTaskSnapshotStatus", () => {
  it("derives task lifecycle states from timestamps", () => {
    const observedAt = new Date("2026-05-22T00:01:00.000Z");

    expect(deriveTaskSnapshotStatus(baseTask, observedAt)).toBe("unclaimed");
    expect(
      deriveTaskSnapshotStatus(
        {
          ...baseTask,
          claimExpiresAt: "2026-05-22T00:02:00.000Z",
          claimedAt: "2026-05-22T00:00:30.000Z",
          claimedBy: "part_active",
        },
        observedAt,
      ),
    ).toBe("claim_active");
    expect(
      deriveTaskSnapshotStatus(
        {
          ...baseTask,
          claimExpiredAt: "2026-05-22T00:00:59.000Z",
        },
        observedAt,
      ),
    ).toBe("claim_expired");
    expect(
      deriveTaskSnapshotStatus(
        {
          ...baseTask,
          claimExpiresAt: "2026-05-22T00:00:59.000Z",
          claimedAt: "2026-05-22T00:00:30.000Z",
          claimedBy: "part_expired",
        },
        observedAt,
      ),
    ).toBe("claim_expired");
    expect(
      deriveTaskSnapshotStatus(
        {
          ...baseTask,
          releasedAt: "2026-05-22T00:00:45.000Z",
        },
        observedAt,
      ),
    ).toBe("claim_cleared");
    expect(
      deriveTaskSnapshotStatus(
        { ...baseTask, completedAt: "2026-05-22T00:00:45.000Z" },
        observedAt,
      ),
    ).toBe("completed");
    expect(
      deriveTaskSnapshotStatus({ ...baseTask, failedAt: "2026-05-22T00:00:45.000Z" }, observedAt),
    ).toBe("failed");
    expect(
      deriveTaskSnapshotStatus(
        { ...baseTask, cancelledAt: "2026-05-22T00:00:45.000Z" },
        observedAt,
      ),
    ).toBe("cancelled");
  });
});
