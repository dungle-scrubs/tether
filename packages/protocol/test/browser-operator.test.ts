import { describe, expect, it } from "vitest";

import {
  browserOperatorCommandResponseSchema,
  browserOperatorSessionSchema,
  browserSessionSnapshotSchema,
  browserWebSocketTicketResponseSchema,
  operatorCommandPermission,
  operatorCommandRequestSchema,
  operatorTaskApprovalRequestSchema,
} from "../src/browser-operator.js";

describe("browser operator protocol", () => {
  it("accepts only bounded provider-neutral operator commands", () => {
    expect(
      operatorCommandRequestSchema.parse({
        command: "scan",
        scopeKey: "account-primary:inbox",
      }),
    ).toEqual({ command: "scan", scopeKey: "account-primary:inbox" });
    expect(
      operatorCommandRequestSchema.safeParse({
        command: "arbitrary-event",
        scopeKey: "account-primary:inbox",
      }).success,
    ).toBe(false);
    expect(operatorCommandPermission("scan")).toBe("scan.request");
    expect(operatorCommandPermission("backlog-preview")).toBe("backlog-preview.request");
    expect(operatorCommandPermission("authority-revoke")).toBe("authority.revoke");
  });

  it("requires the complete immutable target tuple on browser decisions", () => {
    expect(
      operatorTaskApprovalRequestSchema.safeParse({
        decision: "approved",
        reason: {},
      }).success,
    ).toBe(false);
  });

  it("validates the bounded non-participant bootstrap and snapshot responses", () => {
    const scope = {
      actions: [],
      commands: [],
      permissions: ["session.read"],
      scopeKeys: ["scope"],
      sessionIds: ["sess_email"],
      targetKinds: [],
    };
    expect(
      browserOperatorSessionSchema.safeParse({
        expiresAt: "2026-08-02T00:00:00.000Z",
        grantJti: "grant_browser",
        scope,
        status: "active",
        subject: "operator@example.test",
      }).success,
    ).toBe(false);
    expect(
      browserOperatorSessionSchema.safeParse({
        expiresAt: "2026-08-02T00:00:00.000Z",
        grantJti: "grant_browser",
        scope,
        sessionIds: ["sess_email"],
        status: "active",
        subject: "operator@example.test",
      }).success,
    ).toBe(true);
    expect(
      browserOperatorSessionSchema.safeParse({
        expiresAt: "2026-08-02T00:00:00.000Z",
        grantJti: "grant_browser",
        scope,
        sessionIds: ["sess_other"],
        status: "active",
        subject: "operator@example.test",
      }).success,
    ).toBe(false);
    expect(
      browserSessionSnapshotSchema.safeParse({
        cursor: 0,
        events: [],
        participants: [],
        sessionId: "sess_email",
        tasks: [],
        truncated: { events: false, participants: false, tasks: false },
      }).success,
    ).toBe(true);
  });

  it("rejects malformed command and WebSocket ticket responses", () => {
    expect(
      browserOperatorCommandResponseSchema.safeParse({ status: "queued", task: {} }).success,
    ).toBe(false);
    expect(
      browserWebSocketTicketResponseSchema.safeParse({
        expiresAt: "2026-08-01T00:00:30.000Z",
        ticket: "short",
      }).success,
    ).toBe(false);
  });
});
