import { describe, expect, it } from "vitest";

import { parseAfterSeq } from "../src/event-builders.js";

describe("parseAfterSeq", () => {
  it("accepts only positive safe integer cursors", () => {
    expect(parseAfterSeq(undefined)).toBe(0);
    expect(parseAfterSeq("42")).toBe(42);
    expect(parseAfterSeq(Number.MAX_SAFE_INTEGER.toString())).toBe(Number.MAX_SAFE_INTEGER);
    expect(parseAfterSeq((Number.MAX_SAFE_INTEGER + 1).toString())).toBe(0);
    expect(parseAfterSeq("1.5")).toBe(0);
    expect(parseAfterSeq("bad")).toBe(0);
  });
});
