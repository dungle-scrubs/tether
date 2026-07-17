import { describe, expect, it } from "vitest";

import { authorize } from "../src/auth/authorize.js";
import { type AuthContext, AuthError } from "../src/auth/token.js";

function context(role: AuthContext["role"], sessionScope = "*"): AuthContext {
  return {
    expiresAt: "2099-01-01T00:00:00.000Z",
    grantJti: null,
    issuer: null,
    kid: "default",
    participantId: `${role}_1`,
    role,
    sessionScope,
  };
}

describe("authorize scheduled-supersede", () => {
  it("rejects a scheduler/participant token with the typed role error", () => {
    expect(
      authorize({
        action: "scheduled-supersede",
        context: context("participant"),
        sessionId: "sess_1",
      }),
    ).toBe(AuthError.RoleDenied);
  });

  it("rejects an observer token with the typed role error", () => {
    expect(
      authorize({
        action: "scheduled-supersede",
        context: context("observer"),
        sessionId: "sess_1",
      }),
    ).toBe(AuthError.RoleDenied);
  });

  it("accepts a service-scoped admin/operator token", () => {
    expect(
      authorize({
        action: "scheduled-supersede",
        context: context("admin"),
        sessionId: "sess_1",
      }),
    ).toBeNull();
  });

  it("rejects an admin token scoped to a different session", () => {
    expect(
      authorize({
        action: "scheduled-supersede",
        context: context("admin", "sess_other"),
        sessionId: "sess_1",
      }),
    ).toBe(AuthError.ScopeDenied);
  });

  it("accepts an admin token scoped to the target session", () => {
    expect(
      authorize({
        action: "scheduled-supersede",
        context: context("admin", "sess_1"),
        sessionId: "sess_1",
      }),
    ).toBeNull();
  });

  it("keeps generic task-mutate available to the scheduler/participant role", () => {
    expect(
      authorize({
        action: "task-mutate",
        context: context("participant"),
        sessionId: "sess_1",
      }),
    ).toBeNull();
  });

  it("allows supersession when auth is disabled", () => {
    expect(
      authorize({ action: "scheduled-supersede", context: null, sessionId: "sess_1" }),
    ).toBeNull();
  });
});
