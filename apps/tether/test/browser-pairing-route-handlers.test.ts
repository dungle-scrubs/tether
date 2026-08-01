import { describe, expect, it } from "vitest";

import { serializeBrowserSessionCookie } from "../src/http-browser-pairing-route-handlers.js";

describe("browser pairing HTTP routes", () => {
  it("serializes the bearer only into a host-only Secure HttpOnly SameSite=Strict cookie", () => {
    expect(
      serializeBrowserSessionCookie({
        bearer: "tgr2.payload.signature",
        expiresAt: "2026-08-02T00:00:00.000Z",
      }),
    ).toBe(
      "__Host-Http-tether-operator=tgr2.payload.signature; Path=/; Expires=Sun, 02 Aug 2026 00:00:00 GMT; Secure; HttpOnly; SameSite=Strict",
    );
  });
});
