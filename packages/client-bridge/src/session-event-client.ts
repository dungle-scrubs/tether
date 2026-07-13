import { eventListResponseSchema } from "@dungle-scrubs/tether-protocol";

import { clientBridgeRoutes } from "./routes.js";
import {
  ClientBridgeRequestError,
  type ClientBridgeTransport,
  createClientBridgeTransport,
} from "./transport.js";
import type {
  ClientBridgeListSessionEventsOptions,
  ClientBridgeSessionEvent,
  ClientBridgeSessionEventClientConfig,
  ClientBridgeSessionEventClientDebugInfo,
  ClientBridgeSessionEventClientOptions,
} from "./types.js";

/**
 * Performs client-facing durable session event reads through Tether's REST API.
 * External bridges use this instead of copying event pagination and response
 * validation locally. Transport, HTTP, validation, and pagination failures
 * surface as `ClientBridgeRequestError` with a stable request error code.
 */
export class ClientBridgeSessionEventClient {
  private readonly transport: ClientBridgeTransport;

  /** Creates a session event client for one Tether service URL. */
  constructor(
    config: ClientBridgeSessionEventClientConfig,
    options: ClientBridgeSessionEventClientOptions = {},
  ) {
    this.transport = createClientBridgeTransport({
      ...(config.authToken === undefined ? {} : { authToken: config.authToken }),
      ...(options.fetch === undefined ? {} : { fetch: options.fetch }),
      serviceUrl: config.serviceUrl,
    });
  }

  /** Returns inspectable runtime state for session event REST reads. */
  debugInfo(): ClientBridgeSessionEventClientDebugInfo {
    return this.transport.debugInfo();
  }

  /** Lists all durable session events after the supplied cursor, following pages. */
  async listEvents(
    sessionId: string,
    options: ClientBridgeListSessionEventsOptions = {},
  ): Promise<readonly ClientBridgeSessionEvent[]> {
    const events: ClientBridgeSessionEvent[] = [];
    let afterSeq = options.afterSeq ?? 0;
    while (true) {
      const body = await this.transport.requestJson({
        body: null,
        method: "GET",
        path: clientBridgeRoutes.sessionEvents(sessionId, afterSeq),
        schema: eventListResponseSchema,
      });
      events.push(...body.events);
      if (!body.pagination.hasMore) {
        return events;
      }
      if (body.pagination.nextAfterSeq <= afterSeq) {
        throw new ClientBridgeRequestError({
          code: "INVALID_RESPONSE",
          details: {
            afterSeq,
            nextAfterSeq: body.pagination.nextAfterSeq,
            path: clientBridgeRoutes.sessionEvents(sessionId, afterSeq),
            sessionId,
          },
          message: "Tether returned non-advancing session event pagination",
        });
      }
      afterSeq = body.pagination.nextAfterSeq;
    }
  }

  /** Reads the highest durable event sequence currently visible for a session. */
  async readLatestEventSeq(sessionId: string): Promise<number> {
    const events = await this.listEvents(sessionId);
    return events.reduce((maxSeq, event) => Math.max(maxSeq, event.seq), 0);
  }
}
