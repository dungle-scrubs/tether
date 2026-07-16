import type { IncomingMessage, ServerResponse } from "node:http";
import type { URL } from "node:url";

import { directHttpRoutes } from "./http-direct-routes.js";
import { matchHttpRoute } from "./http-route-spec.js";
import { sessionsDashboardHtml } from "./ui/sessions-dashboard-html.js";

interface UiHttpRouteHandlerInput {
  readonly request: IncomingMessage;
  readonly response: ServerResponse;
  readonly url: URL;
}

/** Serves the embedded operator session dashboard at the site root and /ui. */
export function handleUiHttpRoute(input: UiHttpRouteHandlerInput): boolean {
  const { request, response, url } = input;
  if (!matchHttpRoute(directHttpRoutes.ui, request.method, url.pathname)) {
    return false;
  }
  response.writeHead(200, {
    "cache-control": "no-store",
    "content-type": "text/html; charset=utf-8",
  });
  response.end(sessionsDashboardHtml);
  return true;
}
