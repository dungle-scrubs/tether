import { Effect } from "effect";

import { sessionEventByteLength } from "./resource-limits.js";
import type { SessionServiceEffect } from "./session-service.js";
import type { SessionEvent } from "./types.js";

/** Bounded REST event-list page and whether more events remain past it. */
export interface EventListPage {
  readonly events: SessionEvent[];
  readonly hasMore: boolean;
}

/**
 * Fetches one REST event-list page bounded by both the row limit and a
 * cumulative byte budget. Events are read in bounded pages so a session of
 * near-max events cannot materialize unbounded memory before the response is
 * serialized, mirroring the WebSocket replay-window byte budget. Truncation at
 * the byte boundary is surfaced through the existing pagination contract:
 * fewer events plus hasMore, with the caller's next-cursor advancing past the
 * last returned event. At least one event is always returned when any remain so
 * a single oversized event cannot stall pagination.
 */
export function listEventPageWithinByteBudget(input: {
  readonly afterSeq: number;
  readonly limit: number;
  readonly maxBytes: number;
  readonly maxEventBytes: number;
  readonly service: SessionServiceEffect;
  readonly sessionId: string;
}): Effect.Effect<EventListPage, unknown> {
  return Effect.gen(function* () {
    // One page never holds more than the byte budget plus a single event, so
    // peak materialization stays bounded regardless of how many events exist.
    const fetchPageSize = Math.max(1, Math.floor(input.maxBytes / input.maxEventBytes));
    const events: SessionEvent[] = [];
    let byteLength = 0;
    let cursor = input.afterSeq;
    for (;;) {
      // Fetch one row beyond the requested page size so a full page still
      // detects that more events remain, matching the prior limit + 1 lookahead.
      const pageLimit = Math.min(fetchPageSize, input.limit + 1 - events.length);
      const page = yield* input.service.listEvents(input.sessionId, cursor, { limit: pageLimit });
      for (const event of page) {
        if (events.length >= input.limit) {
          // Lookahead row: more events exist past the requested page size.
          return { events, hasMore: true };
        }
        const nextByteLength = byteLength + sessionEventByteLength(event);
        if (events.length > 0 && nextByteLength > input.maxBytes) {
          // Stop at the byte boundary; remaining events are reachable through
          // the next-cursor on a follow-up request.
          return { events, hasMore: true };
        }
        byteLength = nextByteLength;
        events.push(event);
      }
      if (page.length < pageLimit) return { events, hasMore: false };
      cursor = events.at(-1)?.seq ?? cursor;
    }
  });
}
