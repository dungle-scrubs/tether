import { describe, expect, it } from "vitest";

import { BrowserOperatorClient } from "../dist/index.js";

describe("compiled browser package entry", () => {
  it("loads in Chromium without a Node transport runtime", () => {
    const client = new BrowserOperatorClient({
      csrfToken: "c".repeat(43),
      serviceUrl: "https://hub.example.test",
    });

    expect(globalThis.window).toBeDefined();
    expect(client.debugInfo()).toMatchObject({ ticketCount: 0 });
  });
});
