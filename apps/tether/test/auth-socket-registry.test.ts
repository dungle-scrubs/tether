import { describe, expect, it, vi } from "vitest";

import { AuthSocketRegistry, type AuthSocketStreamKind } from "../src/auth/socket-registry.js";
import type { AuthContext } from "../src/auth/token.js";

describe("authentication socket registry", () => {
  it("registers every stream kind without exposing grant identities in debug state", () => {
    const registry = new AuthSocketRegistry({
      now: () => Date.parse("2026-07-17T01:00:00.000Z"),
    });
    const streams = ["participant", "observer", "host", "viewer"] as const;
    for (const streamKind of streams) {
      registry.register({
        context: createContext(`grant_${streamKind}`),
        socket: new FakeSocket(),
        streamKind,
      });
    }

    expect(registry.debugInfo()).toEqual({
      closeCount: 0,
      grantCount: 4,
      maxSocketCount: 2_000,
      socketCount: 4,
      socketsByStream: {
        host: 1,
        observer: 1,
        participant: 1,
        viewer: 1,
      },
      timerCount: 4,
    });
    expect(JSON.stringify(registry.debugInfo())).not.toContain("grant_");
  });

  it("closes at the parent expiry with no late grace and removes the entry", () => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date("2026-07-17T01:00:00.000Z"));
    const registry = new AuthSocketRegistry();
    const socket = new FakeSocket();
    registry.register({
      context: createContext("grant_expiry", "2026-07-17T01:00:05.000Z"),
      socket,
      streamKind: "participant",
    });

    vi.advanceTimersByTime(4_999);
    expect(socket.close).not.toHaveBeenCalled();
    vi.advanceTimersByTime(1);

    expect(socket.close).toHaveBeenCalledWith(1008, "auth_grant_expired");
    expect(registry.debugInfo()).toMatchObject({
      closeCount: 1,
      grantCount: 0,
      socketCount: 0,
      timerCount: 0,
    });
    vi.useRealTimers();
  });

  it("closes only sockets bound to one revoked parent grant", () => {
    const registry = new AuthSocketRegistry({
      now: () => Date.parse("2026-07-17T01:00:00.000Z"),
    });
    const first = new FakeSocket();
    const second = new FakeSocket();
    const unrelated = new FakeSocket();
    registry.register({
      context: createContext("grant_revoked"),
      socket: first,
      streamKind: "participant",
    });
    registry.register({
      context: createContext("grant_revoked"),
      socket: second,
      streamKind: "observer",
    });
    registry.register({
      context: createContext("grant_other"),
      socket: unrelated,
      streamKind: "viewer",
    });

    expect(registry.closeGrant("grant_revoked", "auth_grant_revoked")).toBe(2);

    expect(first.close).toHaveBeenCalledWith(1008, "auth_grant_revoked");
    expect(second.close).toHaveBeenCalledWith(1008, "auth_grant_revoked");
    expect(unrelated.close).not.toHaveBeenCalled();
    expect(registry.grantJtis()).toEqual(["grant_other"]);
  });

  it("unregisters closed sockets and clears their expiry timer idempotently", () => {
    const registry = new AuthSocketRegistry({
      now: () => Date.parse("2026-07-17T01:00:00.000Z"),
    });
    const socket = new FakeSocket();
    const unregister = registry.register({
      context: createContext("grant_cleanup"),
      socket,
      streamKind: "host",
    });

    unregister();
    unregister();

    expect(registry.debugInfo()).toMatchObject({
      grantCount: 0,
      socketCount: 0,
      timerCount: 0,
    });
    expect(socket.close).not.toHaveBeenCalled();
  });

  it("rejects sockets beyond the configured registry bound", () => {
    const registry = new AuthSocketRegistry({
      maxSocketCount: 1,
      now: () => Date.parse("2026-07-17T01:00:00.000Z"),
    });
    registry.register({
      context: createContext("grant_first"),
      socket: new FakeSocket(),
      streamKind: "participant",
    });
    const rejected = new FakeSocket();

    registry.register({
      context: createContext("grant_second"),
      socket: rejected,
      streamKind: "observer",
    });

    expect(rejected.close).toHaveBeenCalledWith(1013, "auth_socket_capacity");
    expect(registry.debugInfo()).toMatchObject({ maxSocketCount: 1, socketCount: 1 });
  });

  it("chunks compatibility deadlines beyond the Node timer limit", () => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date("2026-07-17T01:00:00.000Z"));
    const registry = new AuthSocketRegistry();
    const socket = new FakeSocket();
    registry.register({
      context: createContext("grant_compatibility", "2099-12-31T23:59:59.000Z"),
      socket,
      streamKind: "observer",
    });

    vi.advanceTimersByTime(86_400_000);

    expect(socket.close).not.toHaveBeenCalled();
    expect(registry.debugInfo()).toMatchObject({ socketCount: 1, timerCount: 1 });
    vi.useRealTimers();
  });
});

class FakeSocket {
  readonly close = vi.fn<(code: number, reason: string) => void>();
}

function createContext(grantJti: string, expiresAt = "2026-07-18T01:00:00.000Z"): AuthContext {
  return {
    expiresAt,
    grantJti,
    issuer: "https://auth.socket.test",
    kid: "current",
    participantId: "part_socket",
    role: "participant",
    sessionScope: "*",
  };
}

const streamKindTypeCheck: readonly AuthSocketStreamKind[] = [
  "participant",
  "observer",
  "host",
  "viewer",
];
void streamKindTypeCheck;
