import { describe, expect, it } from "vitest";
import type { WebSocket } from "ws";

import { ControlSocketRegistry, controlSocketKey } from "../src/websocket-participant-gateway.js";

/** Minimal WebSocket double tracking close calls and open state. */
class FakeSocket {
  readonly OPEN = 1;
  readyState = 1;
  closeCode: number | null = null;
  closeReason: string | null = null;
  readonly sent: string[] = [];

  send(data: string): void {
    this.sent.push(data);
  }

  close(code: number, reason: string): void {
    this.readyState = 3;
    this.closeCode = code;
    this.closeReason = reason;
  }
}

function asSocket(fake: FakeSocket): WebSocket {
  return fake as unknown as WebSocket;
}

const key = controlSocketKey("sess_1", "part_1", "inst_a");

describe("ControlSocketRegistry proactive supersession close", () => {
  it("closes the prior-epoch socket immediately when a newer epoch registers the same key", () => {
    const registry = new ControlSocketRegistry();
    const first = new FakeSocket();
    const second = new FakeSocket();

    registry.register(key, asSocket(first), 7);
    // A same-instance reconnect advanced the epoch to 8 and committed it; the
    // prior epoch-7 socket must be fenced immediately, not on its next command.
    registry.register(key, asSocket(second), 8);

    expect(first.readyState).toBe(3);
    expect(first.closeCode).toBe(1008);
    expect(first.closeReason).toBe("control epoch superseded");
    expect(second.readyState).toBe(1);
  });

  it("does not close a socket for a different runtime identity", () => {
    const registry = new ControlSocketRegistry();
    const first = new FakeSocket();
    const other = new FakeSocket();

    registry.register(key, asSocket(first), 7);
    registry.register(controlSocketKey("sess_1", "part_1", "inst_b"), asSocket(other), 1);

    expect(first.readyState).toBe(1);
    expect(other.readyState).toBe(1);
  });

  it("never closes the incoming socket when re-registering the same or a stale epoch", () => {
    const registry = new ControlSocketRegistry();
    const socket = new FakeSocket();

    registry.register(key, asSocket(socket), 5);
    // Re-registering the same socket, or an older epoch, must not close it.
    registry.register(key, asSocket(socket), 5);

    expect(socket.readyState).toBe(1);
  });

  it("rejects a late strictly-lower-epoch registration without replacing the current owner", () => {
    const registry = new ControlSocketRegistry();
    const high = new FakeSocket();
    const low = new FakeSocket();

    // The newer epoch-8 acquisition registered first; a late epoch-7 registration
    // (its own reconnect already superseded) then arrives out of order.
    registry.register(key, asSocket(high), 8);
    registry.register(key, asSocket(low), 7);

    // The stale lower-epoch socket is rejected and closed; the higher-epoch owner
    // is untouched and still registered.
    expect(low.readyState).toBe(3);
    expect(low.closeCode).toBe(1008);
    expect(low.closeReason).toBe("control epoch superseded");
    expect(high.readyState).toBe(1);

    // A subsequent newer epoch closes the retained epoch-8 owner, proving the
    // registry never adopted the stale epoch-7 socket.
    const higher = new FakeSocket();
    registry.register(key, asSocket(higher), 9);
    expect(high.readyState).toBe(3);
    expect(higher.readyState).toBe(1);
  });

  it("removes a key only when the closing socket is still its current registrant", () => {
    const registry = new ControlSocketRegistry();
    const first = new FakeSocket();
    const second = new FakeSocket();

    registry.register(key, asSocket(first), 7);
    registry.register(key, asSocket(second), 8);

    // The superseded first socket closing must not evict the current second one.
    registry.remove(key, asSocket(first));
    const third = new FakeSocket();
    registry.register(key, asSocket(third), 9);
    expect(second.readyState).toBe(3);
    expect(third.readyState).toBe(1);
  });
});
