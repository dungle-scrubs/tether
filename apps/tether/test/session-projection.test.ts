import { describe, expect, it } from "vitest";

import {
  foldSessionProjection,
  reduceSessionProjection,
  SESSION_PROJECTION_REDUCER_VERSION,
} from "../src/session-projection.js";
import type { SessionEvent } from "../src/types.js";

describe("SessionProjectionReducer", () => {
  it("keeps the inventory title current when a title event follows 10,000 earlier events", () => {
    const projection = foldSessionProjection(
      createLongSessionEvents("session.title", { title: "The current title" }),
    );

    expect(projection.title).toBe("The current title");
  });

  it("keeps archive state current when an archive event follows 10,000 earlier events", () => {
    const projection = foldSessionProjection(
      createLongSessionEvents("session.archived", { archived: true }),
    );

    expect(projection.archivedAt).toBe(new Date(10_001).toISOString());
  });

  it("keeps deletion state current when a delete event follows 10,000 earlier events", () => {
    const projection = foldSessionProjection(
      createLongSessionEvents("session.deleted", { deleted: true }),
    );

    expect(projection.deletedAt).toBe(new Date(10_001).toISOString());
  });

  it("keeps activity current when a run starts after 10,000 earlier events", () => {
    const projection = foldSessionProjection(
      createLongSessionEvents("assistant.started", { runId: "run_current" }),
    );

    expect(projection.activity).toBe("running");
    expect(projection.activeRunId).toBe("run_current");
    expect(projection.activityChangedAt).toBe(new Date(10_001).toISOString());
  });

  it("keeps durable host metadata current when a host event follows 10,000 earlier events", () => {
    const hostMetadata = {
      branch: "feat/event-log-scalability",
      cwd: "/workspace/tether",
      workspace: "/workspace",
    };
    const projection = foldSessionProjection(createLongSessionEvents("host.online", hostMetadata));

    expect(projection.hostMetadata).toEqual(hostMetadata);
    expect(projection.hostMetadataSourceSeq).toBe(10_001);
  });

  it("keeps fork lineage current when a fork event follows 10,000 earlier events", () => {
    const projection = foldSessionProjection(
      createLongSessionEvents("session.forkedFrom", {
        forkSeq: 9_999,
        parentSessionId: "sess_parent",
      }),
    );

    expect(projection.forkedFrom).toEqual({
      forkSeq: 9_999,
      parentSessionId: "sess_parent",
    });
  });

  it("records the active reducer version and safe coverage sequence", () => {
    const projection = foldSessionProjection(
      createLongSessionEvents("client.observed", { outcome: "complete" }),
    );

    expect(projection.reducerVersion).toBe(SESSION_PROJECTION_REDUCER_VERSION);
    expect(projection.coversSeqTo).toBe(10_001);
    expect(Number.isSafeInteger(projection.coversSeqTo)).toBe(true);
  });

  it("records the exact source sequence for the resolved title", () => {
    const projection = foldSessionProjection([
      createEvent(1, "session.title", { title: "Projection contract" }),
    ]);

    expect(projection.title).toBe("Projection contract");
    expect(projection.titleSourceSeq).toBe(1);
  });

  it("records validated tangent lineage with the source event timestamp", () => {
    const projection = foldSessionProjection([
      createEvent(1, "session.tangentOf", {
        label: "Reducer design",
        parentSessionId: "sess_parent",
        quote: "Keep the reducer pure",
        sourceMessageId: "msg_source",
      }),
    ]);

    expect(projection.tangentOf).toEqual({
      createdAt: new Date(1).toISOString(),
      label: "Reducer design",
      parentSessionId: "sess_parent",
      quote: "Keep the reducer pure",
      sourceMessageId: "msg_source",
    });
  });

  it("records cumulative event count, last event time, and coverage", () => {
    const projection = foldSessionProjection(
      createLongSessionEvents("client.observed", { outcome: "complete" }),
    );

    expect(projection.eventCount).toBe(10_001);
    expect(projection.lastEventAt).toBe(new Date(10_001).toISOString());
    expect(projection.coversSeqTo).toBe(10_001);
  });

  it("uses the first valid user message as fallback without replacing an explicit title", () => {
    const fallback = foldSessionProjection([
      createEvent(1, "user.message", {}),
      createEvent(2, "user.message", { text: "  First\nmessage  " }),
      createEvent(3, "user.message", { text: "Later message" }),
    ]);
    const explicitFirst = foldSessionProjection([
      createEvent(1, "session.title", { title: "Explicit title" }),
      createEvent(2, "user.message", { text: "User fallback" }),
    ]);

    expect(fallback.title).toBe("First message");
    expect(fallback.titleSourceSeq).toBe(2);
    expect(explicitFirst.title).toBe("Explicit title");
    expect(explicitFirst.titleSourceSeq).toBe(1);
  });

  it("replaces a fallback title with a later valid explicit title", () => {
    const projection = foldSessionProjection([
      createEvent(1, "user.message", { text: "Fallback title" }),
      createEvent(2, "session.title", { title: "Explicit replacement" }),
    ]);

    expect(projection.title).toBe("Explicit replacement");
    expect(projection.titleSourceSeq).toBe(2);
  });

  it("clears archive state when a valid unarchive event follows an archive", () => {
    const projection = foldSessionProjection([
      createEvent(1, "session.archived", { archived: true }),
      createEvent(2, "session.archived", { archived: false }),
    ]);

    expect(projection.archivedAt).toBeNull();
    expect(projection.coversSeqTo).toBe(2);
  });

  it("clears deletion state when a valid restore event follows a delete", () => {
    const projection = foldSessionProjection([
      createEvent(1, "session.deleted", { deleted: true }),
      createEvent(2, "session.deleted", { deleted: false }),
    ]);

    expect(projection.deletedAt).toBeNull();
    expect(projection.coversSeqTo).toBe(2);
  });

  it("replaces durable host metadata with the latest valid host event", () => {
    const projection = foldSessionProjection([
      createEvent(1, "host.online", { branch: "main" }),
      createEvent(2, "host.online", { branch: "feat/projections" }),
    ]);

    expect(projection.hostMetadata).toEqual({ branch: "feat/projections" });
    expect(projection.hostMetadataSourceSeq).toBe(2);
  });

  it("settles only the matching active run and clears activity history on /clear", () => {
    const started = foldSessionProjection([
      createEvent(1, "assistant.started", { runId: "run_active" }),
    ]);
    const mismatched = reduceSessionProjection(
      started,
      createEvent(2, "assistant.completed", { runId: "run_other" }),
    );
    const completed = reduceSessionProjection(
      mismatched,
      createEvent(3, "assistant.completed", { runId: "run_active" }),
    );
    const restarted = reduceSessionProjection(
      completed,
      createEvent(4, "assistant.started", { runId: "run_next" }),
    );
    const cleared = reduceSessionProjection(
      restarted,
      createEvent(5, "user.command", { command: "/clear" }),
    );

    expect(mismatched.activity).toBe("running");
    expect(mismatched.activeRunId).toBe("run_active");
    expect(completed.activity).toBe("settled");
    expect(completed.activeRunId).toBeNull();
    expect(completed.activityChangedAt).toBe(new Date(3).toISOString());
    expect(cleared.activity).toBe("idle");
    expect(cleared.activeRunId).toBeNull();
    expect(cleared.activityChangedAt).toBe(new Date(5).toISOString());
  });

  it("advances cumulative metadata without changing semantics for an unknown client event", () => {
    const projection = foldSessionProjection([
      createEvent(1, "session.title", { title: "Durable title" }),
      createEvent(2, "session.archived", { archived: true }),
      createEvent(3, "assistant.started", { runId: "run_active" }),
      createEvent(4, "host.online", { branch: "main" }),
      createEvent(5, "session.forkedFrom", {
        forkSeq: 4,
        parentSessionId: "sess_parent",
      }),
    ]);
    const next = reduceSessionProjection(
      projection,
      createEvent(6, "client.futureEvent", { ignored: true }),
    );

    expect(next).toEqual({
      ...projection,
      coversSeqTo: 6,
      eventCount: 6,
      lastEventAt: new Date(6).toISOString(),
    });
  });

  it("treats malformed projection payloads as semantic no-ops while metadata advances", () => {
    const projection = foldSessionProjection([
      createEvent(1, "session.title", { title: "Uncorrupted title" }),
    ]);
    const next = reduceSessionProjection(
      projection,
      createEvent(2, "assistant.started", { runId: "   " }),
    );

    expect(next).toEqual({
      ...projection,
      coversSeqTo: 2,
      eventCount: 2,
      lastEventAt: new Date(2).toISOString(),
    });
  });

  it("rejects an event that would regress projection coverage", () => {
    const projection = foldSessionProjection([createEvent(1, "client.observed", {})]);

    expect(() =>
      reduceSessionProjection(projection, createEvent(1, "client.replayed", {})),
    ).toThrow(/coverage regression/u);
  });

  it("rejects unsafe event sequence values before changing projection state", () => {
    const projection = foldSessionProjection([]);
    const unsafeEvents = [
      createEvent(0, "client.observed", {}),
      createEvent(-1, "client.observed", {}),
      createEvent(1.5, "client.observed", {}),
      { ...createEvent(1, "client.observed", {}), seq: Number.MAX_SAFE_INTEGER + 1 },
    ];

    for (const event of unsafeEvents) {
      expect(() => reduceSessionProjection(projection, event)).toThrow(/unsafe sequence/u);
    }
  });

  it("rejects a prior projection from another reducer version", () => {
    const projection = {
      ...foldSessionProjection([]),
      reducerVersion: SESSION_PROJECTION_REDUCER_VERSION + 1,
    };

    expect(() =>
      reduceSessionProjection(projection, createEvent(1, "client.observed", {})),
    ).toThrow(/version mismatch/u);
  });
});

function createLongSessionEvents(
  finalType: string,
  finalPayload: Record<string, unknown>,
): SessionEvent[] {
  const events = Array.from({ length: 10_000 }, (_, index) =>
    createEvent(index + 1, "client.observed", {}),
  );
  events.push(createEvent(10_001, finalType, finalPayload));
  return events;
}

function createEvent(seq: number, type: string, payload: Record<string, unknown>): SessionEvent {
  return {
    createdAt: new Date(seq).toISOString(),
    eventId: `evt_${seq}`,
    payload,
    producerId: "projection-test",
    seq,
    sessionId: "sess_projection",
    type,
  };
}
