import type { OperatorGrantScope } from "@dungle-scrubs/tether-protocol";
import { describe, expect, it } from "vitest";

import { authorizeOperator } from "../src/auth/operator-authority.js";

const scope: OperatorGrantScope = {
  actions: ["approve"],
  commands: ["scan"],
  permissions: ["approval.submit", "session.read"],
  scopeKeys: ["account-primary:inbox"],
  sessionIds: ["sess_email"],
  targetKinds: ["message"],
};

describe("operator authority", () => {
  it("allows only an exact resource-action tuple inside the durable grant scope", () => {
    expect(
      authorizeOperator(scope, {
        action: "approve",
        permission: "approval.submit",
        scopeKey: "account-primary:inbox",
        sessionId: "sess_email",
        targetKind: "message",
      }),
    ).toBeNull();
  });

  it.each([
    [{ permission: "scan.request" as const }, "operator_permission_denied"],
    [{ permission: "session.read" as const, sessionId: "sess_other" }, "operator_session_denied"],
    [
      { permission: "approval.submit" as const, scopeKey: "account-other:inbox" },
      "operator_scope_key_denied",
    ],
    [
      { permission: "approval.submit" as const, targetKind: "thread" },
      "operator_target_kind_denied",
    ],
    [{ action: "reject", permission: "approval.submit" as const }, "operator_action_denied"],
    [{ command: "archive", permission: "session.read" as const }, "operator_command_denied"],
  ])("denies each scope dimension with %s", (request, expected) => {
    expect(authorizeOperator(scope, request)).toBe(expected);
  });
});
