import { defineHttpRoute } from "./http-route-spec.js";

/** Process-level HTTP routes dispatched directly by the app server. */
export const directHttpRoutes = {
  health: defineHttpRoute({
    control: "not-applicable",
    method: "GET",
    name: "server.health",
    pattern: /^\/health$/u,
  }),
  serverDebug: defineHttpRoute({
    control: "not-applicable",
    method: "GET",
    name: "server.debug",
    pattern: /^\/debug\/server$/u,
  }),
  ui: defineHttpRoute({
    control: "not-applicable",
    method: "GET",
    name: "server.ui",
    pattern: /^\/(?:ui)?$/u,
  }),
} as const;
