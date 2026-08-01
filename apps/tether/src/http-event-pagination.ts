import { Effect } from "effect";

import { sessionEventByteLength } from "./resource-limits.js";
import type { SessionServiceEffect } from "./session-service.js";
import type { SessionEvent } from "./types.js";

/** Bounded REST event-list page and whether more events remain past it. */
export interface EventListPage {
  readonly events: SessionEvent[];
  readonly hasMore: boolean;
}

/** Fetches one event page bounded by row count and serialized bytes. */
export function listEventPageWithinByteBudget(input: {
  readonly afterSeq: number;
  readonly limit: number;
  readonly maxBytes: number;
  readonly maxEventBytes: number;
  readonly service: SessionServiceEffect;
  readonly sessionId: string;
}): Effect.Effect<EventListPage, unknown> {
  return Effect.gen(function* () {
    const fetchPageSize = Math.max(1, Math.floor(input.maxBytes / input.maxEventBytes));
    const events: SessionEvent[] = [];
    let byteLength = 0;
    let cursor = input.afterSeq;
    for (;;) {
      const pageLimit = Math.min(fetchPageSize, input.limit + 1 - events.length);
      const page = yield* input.service.listEvents(input.sessionId, cursor, { limit: pageLimit });
      for (const event of page) {
        if (events.length >= input.limit) return { events, hasMore: true };
        const nextByteLength = byteLength + sessionEventByteLength(event);
        if (events.length > 0 && nextByteLength > input.maxBytes) {
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
