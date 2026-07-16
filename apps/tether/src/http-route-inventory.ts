/**
 * Central fail-closed inventory of declared HTTP route metadata. Importing this
 * module makes every route's participant-control classification reviewable
 * through one public seam.
 */

import { clientBindingRoutes } from "./http-client-binding-route-handlers.js";
import { directHttpRoutes } from "./http-direct-routes.js";
import type { HttpRouteSpec } from "./http-route-spec.js";
import { sessionDebugRoutes } from "./http-session-debug-route-handlers.js";
import { sessionResourceRoutes } from "./http-session-route-handlers.js";
import { taskHttpRoutes } from "./http-task-route-handlers.js";

/** Every route declared through defineHttpRoute, sorted by stable name. */
export const httpRouteInventory: readonly HttpRouteSpec[] = [
  ...Object.values(clientBindingRoutes),
  ...Object.values(directHttpRoutes),
  ...sessionDebugRoutes.map((entry) => entry.route),
  ...Object.values(sessionResourceRoutes),
  ...Object.values(taskHttpRoutes),
].sort((left, right) => left.name.localeCompare(right.name));

/** Stable names of participant-owned mutations requiring an atomic fence. */
export const fencedHttpRouteNames: readonly string[] = httpRouteInventory
  .filter((route) => route.control === "fenced")
  .map((route) => route.name);
