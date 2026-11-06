import { describe, expect, it } from "vitest";

import { checkRecoveryBudget } from "../src/participant-rest-replay.js";

describe("ParticipantRestReplay admission", () => {
  it("reports count overflow before bytes when admission exceeds both budgets", () => {
    expect(() => checkRecoveryBudget({ maxQueueSize: 2, maxQueueBytes: 10 }, 3, 11)).toThrowError(
      expect.objectContaining({
        reason: "delivery_queue_overflow",
        safeDetails: { maxQueueSize: 2, observedQueueSize: 3 },
      }),
    );
  });
});
