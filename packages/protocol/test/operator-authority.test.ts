import { describe, expect, it } from "vitest";

import { operatorGrantScopeSchema } from "../src/index.js";

describe("operator grant scope", () => {
  it("accepts only bounded provider-neutral resource and action scopes", () => {
    const scope = {
      actions: ["action_opaque_archive"],
      commands: ["command_opaque_scan"],
      permissions: [
        "session.read",
        "approval.submit",
        "scan.request",
        "browser-session.read",
        "websocket.connect",
      ],
      scopeKeys: ["scope_opaque_primary"],
      sessionIds: ["sess_email"],
      targetKinds: ["target_opaque_handling"],
    };

    expect(operatorGrantScopeSchema.parse(scope)).toEqual(scope);
    expect(
      operatorGrantScopeSchema.safeParse({
        ...scope,
        mailboxIds: ["mailbox_private"],
        provider: "fastmail",
      }).success,
    ).toBe(false);
  });

  it("rejects duplicate, empty, and oversized scope values", () => {
    const base = {
      actions: ["action_opaque_archive"],
      commands: ["command_opaque_scan"],
      permissions: ["session.read"],
      scopeKeys: ["scope_opaque_primary"],
      sessionIds: ["sess_email"],
      targetKinds: ["target_opaque_handling"],
    };

    expect(operatorGrantScopeSchema.safeParse({ ...base, actions: ["same", "same"] }).success).toBe(
      false,
    );
    expect(operatorGrantScopeSchema.safeParse({ ...base, sessionIds: [] }).success).toBe(false);
    expect(
      operatorGrantScopeSchema.safeParse({ ...base, scopeKeys: ["x".repeat(513)] }).success,
    ).toBe(false);
    expect(
      operatorGrantScopeSchema.safeParse({
        ...base,
        actions: Array.from({ length: 32 }, (_, index) => `${index}_${"é".repeat(250)}`),
      }).success,
    ).toBe(false);
  });
});
