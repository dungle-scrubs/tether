import type { BrowserSessionSnapshot } from "@dungle-scrubs/tether-protocol";
import { describe, expect, it } from "vitest";

import {
  browserOperatorHttpRoutes,
  fitBrowserSnapshotWithinByteBudget,
} from "../src/http-browser-operator-route-handlers.js";
import { matchHttpRoute } from "../src/http-route-spec.js";

describe("browser operator HTTP routes", () => {
  it.each([
    [browserOperatorHttpRoutes.self, "GET", "/operator/browser-session"],
    [browserOperatorHttpRoutes.selfRevoke, "POST", "/operator/browser-session/revoke"],
    [browserOperatorHttpRoutes.snapshot, "GET", "/operator/sessions/sess_email/snapshot"],
    [
      browserOperatorHttpRoutes.approval,
      "POST",
      "/operator/sessions/sess_email/tasks/task_review/approval",
    ],
    [browserOperatorHttpRoutes.command, "POST", "/operator/sessions/sess_email/commands"],
    [browserOperatorHttpRoutes.websocketTicket, "POST", "/operator/websocket-ticket"],
  ])("matches only the dedicated non-participant route", (route, method, pathname) => {
    expect(matchHttpRoute(route, method, pathname)).not.toBeNull();
    expect(matchHttpRoute(route, method, pathname.replace("/operator", "/sessions"))).toBeNull();
  });

  it("truncates a complete snapshot to the configured serialized byte budget", () => {
    const snapshot: BrowserSessionSnapshot = {
      cursor: 0,
      events: [],
      participants: [],
      sessionId: "sess_email",
      tasks: [
        {
          cancelledAt: null,
          claimExpiredAt: null,
          claimExpiredBy: null,
          claimExpiresAt: null,
          claimId: null,
          claimedAt: null,
          claimedBy: null,
          completedAt: null,
          createdAt: "2026-08-01T00:00:00.000Z",
          failedAt: null,
          failure: null,
          input: null,
          kind: "operator.scan",
          objective: "x".repeat(2_000),
          releasedAt: null,
          releasedBy: null,
          result: null,
          sessionId: "sess_email",
          taskId: "task_large",
        },
      ],
      truncated: { events: false, participants: false, tasks: false },
    };

    const bounded = fitBrowserSnapshotWithinByteBudget(snapshot, 600);

    expect(Buffer.byteLength(JSON.stringify(bounded))).toBeLessThanOrEqual(600);
    expect(bounded.tasks).toEqual([]);
    expect(bounded.truncated.tasks).toBe(true);
  });
});
