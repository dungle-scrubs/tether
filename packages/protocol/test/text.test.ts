import { describe, expect, it } from "vitest";

import { utf8ByteLength } from "../src/text.js";

const encoder = new TextEncoder();

describe("utf8ByteLength", () => {
  it("matches TextEncoder for ascii, multibyte, and astral input", () => {
    for (const value of [
      "",
      "account-primary:inbox",
      "café",
      "日本語のスコープ",
      "emoji \u{1F600} tail",
      "\u{10FFFF}",
    ]) {
      expect(utf8ByteLength(value)).toBe(encoder.encode(value).byteLength);
    }
  });

  it("matches TextEncoder replacement sizing for unpaired surrogates", () => {
    for (const value of ["\uD800", "\uDC00", "lead \uD83D tail", "trail \uDE00"]) {
      expect(utf8ByteLength(value)).toBe(encoder.encode(value).byteLength);
    }
  });
});
