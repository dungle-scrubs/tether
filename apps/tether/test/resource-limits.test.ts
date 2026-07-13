import { describe, expect, it } from "vitest";

import { createWebSocketMessageRateLimiter, parseEventListLimit } from "../src/resource-limits.js";

describe("resource limit helpers", () => {
  it("resets WebSocket message rate windows using an injectable clock", () => {
    let now = 0;
    const limiter = createWebSocketMessageRateLimiter({
      limit: 2,
      now: () => now,
      windowMs: 100,
    });

    expect(limiter.check()).toMatchObject({ allowed: true, observed: 1 });
    expect(limiter.check()).toMatchObject({ allowed: true, observed: 2 });
    expect(limiter.check()).toMatchObject({ allowed: false, observed: 3 });

    now = 100;

    expect(limiter.check()).toMatchObject({ allowed: true, observed: 1 });
  });

  it("defaults and clamps REST event-list limits", () => {
    const limits = { eventListDefaultLimit: 5, eventListMaxLimit: 10 };

    expect(parseEventListLimit(null, limits)).toBe(5);
    expect(parseEventListLimit("bad", limits)).toBe(5);
    expect(parseEventListLimit("0", limits)).toBe(5);
    expect(parseEventListLimit("7", limits)).toBe(7);
    expect(parseEventListLimit("99", limits)).toBe(10);
  });
});
