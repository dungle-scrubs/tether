import { describe, expect, it } from "vitest";

import { type AppServerCloseResources, closeAppServerResources } from "../src/http.js";

interface FakeWsClient {
  readonly closes: { readonly code: number; readonly reason: string }[];
}

function fakeWsClient(): FakeWsClient & { close: (code: number, reason: string) => void } {
  const closes: { readonly code: number; readonly reason: string }[] = [];
  return {
    close: (code: number, reason: string) => {
      closes.push({ code, reason });
    },
    closes,
  };
}

/**
 * Builds recording shutdown resources whose listener closes complete
 * asynchronously, mirroring the real servers finishing their drains.
 */
function recordingResources(events: string[]): {
  readonly resources: AppServerCloseResources;
  readonly wsClients: readonly ReturnType<typeof fakeWsClient>[];
} {
  const wsClients = [fakeWsClient(), fakeWsClient()];
  return {
    resources: {
      auth: {
        close: () => {
          events.push("auth.close");
        },
      },
      authRevocation: {
        stop: async () => {
          events.push("authRevocation.stop");
        },
      },
      eventFanout: {
        stop: async () => {
          events.push("eventFanout.stop");
        },
      },
      server: {
        close: (callback) => {
          events.push("server.close.initiated");
          setImmediate(() => {
            events.push("server.close.completed");
            callback();
          });
        },
        closeIdleConnections: () => {
          events.push("server.closeIdleConnections");
        },
      },
      taskClaimSweeper: {
        stop: async () => {
          events.push("taskClaimSweeper.stop");
        },
      },
      wsServer: {
        clients: wsClients,
        close: (callback) => {
          events.push("wsServer.close.initiated");
          setImmediate(() => {
            events.push("wsServer.close.completed");
            callback?.();
          });
        },
      },
    },
    wsClients,
  };
}

describe("app server shutdown ordering", () => {
  it("stops intake and drains sockets before stopping fanout and the claim sweeper", async () => {
    const events: string[] = [];
    const { resources, wsClients } = recordingResources(events);

    await closeAppServerResources(resources);

    // Intake stops first: both listener closes are initiated before any
    // background delivery module stops.
    const intakeInitiated = [
      events.indexOf("server.close.initiated"),
      events.indexOf("wsServer.close.initiated"),
    ];
    const drainCompleted = [
      events.indexOf("server.close.completed"),
      events.indexOf("wsServer.close.completed"),
    ];
    const backgroundStops = [
      events.indexOf("taskClaimSweeper.stop"),
      events.indexOf("eventFanout.stop"),
      events.indexOf("authRevocation.stop"),
      events.indexOf("auth.close"),
    ];
    for (const index of [...intakeInitiated, ...drainCompleted, ...backgroundStops]) {
      expect(index).toBeGreaterThanOrEqual(0);
    }
    // Fanout and the sweeper stop strictly after both server drains complete,
    // so accepted subscribers keep receiving committed events for the whole
    // drain window instead of silently missing them.
    for (const stopIndex of backgroundStops) {
      for (const drainIndex of drainCompleted) {
        expect(stopIndex).toBeGreaterThan(drainIndex);
      }
    }
    // The sweeper stops before the fanout, and auth teardown is last.
    expect(events.indexOf("taskClaimSweeper.stop")).toBeLessThan(
      events.indexOf("eventFanout.stop"),
    );
    expect(events.indexOf("auth.close")).toBe(events.length - 1);

    // Every connected socket received a going-away close while delivery was
    // still running, so clients reconnect and resume from durable cursors.
    for (const client of wsClients) {
      expect(client.closes).toEqual([{ code: 1001, reason: "server shutting down" }]);
    }
    const socketCloseAfterIntake = events.indexOf("server.closeIdleConnections");
    for (const initiatedIndex of intakeInitiated) {
      expect(socketCloseAfterIntake).toBeGreaterThan(initiatedIndex);
    }
  });

  it("propagates a listener close failure after initiating both closes", async () => {
    const events: string[] = [];
    const { resources } = recordingResources(events);
    const failing: AppServerCloseResources = {
      ...resources,
      server: {
        close: (callback) => {
          events.push("server.close.initiated");
          setImmediate(() => callback(new Error("listener close failed")));
        },
        closeIdleConnections: () => {
          events.push("server.closeIdleConnections");
        },
      },
    };

    await expect(closeAppServerResources(failing)).rejects.toThrow("listener close failed");
    // Background modules must not have been stopped out of order on the
    // failure path either.
    expect(events).not.toContain("eventFanout.stop");
    expect(events).not.toContain("taskClaimSweeper.stop");
  });
});
