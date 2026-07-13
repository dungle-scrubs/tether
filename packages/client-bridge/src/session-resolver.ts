import { clientBridgeRoutes } from "./routes.js";
import {
  clientSessionBindingResponseSchema,
  clientSessionBindingsResponseSchema,
} from "./schemas.js";
import { type ClientBridgeTransport, createClientBridgeTransport } from "./transport.js";
import type {
  ClientBridgeResolveSessionResult,
  ClientBridgeSessionBinding,
  ClientBridgeSessionResolverConfig,
  ClientBridgeSessionResolverDebugInfo,
  ClientBridgeSessionResolverOptions,
} from "./types.js";

/**
 * Resolves provider-specific conversations to durable Tether sessions. Client
 * bridges use this instead of duplicating `/client-bindings` REST calls,
 * response validation, and in-process session-id caching.
 */
export class ClientBridgeSessionResolver {
  private currentSessionId: string | null;
  private readonly externalSessionIds = new Map<string, string>();
  private readonly transport: ClientBridgeTransport;

  /** Creates a resolver for one external provider. */
  constructor(
    private readonly config: ClientBridgeSessionResolverConfig,
    options: ClientBridgeSessionResolverOptions = {},
  ) {
    this.currentSessionId = config.defaultSessionId ?? null;
    this.transport = createClientBridgeTransport({
      ...(config.authToken === undefined ? {} : { authToken: config.authToken }),
      ...(options.fetch === undefined ? {} : { fetch: options.fetch }),
      serviceUrl: config.serviceUrl,
    });
  }

  /** Returns inspectable resolver state for bridge debug surfaces. */
  debugInfo(): ClientBridgeSessionResolverDebugInfo {
    return {
      currentSessionId: this.currentSessionId,
      requestCount: this.transport.debugInfo().requestCount,
      resolvedExternalIdCount: this.externalSessionIds.size,
    };
  }

  /** Lists active bindings for this resolver's provider and seeds the local session cache. */
  async listBindings(): Promise<readonly ClientBridgeSessionBinding[]> {
    const body = await this.transport.requestJson({
      body: null,
      method: "GET",
      path: clientBridgeRoutes.clientBindings(this.config.provider),
      schema: clientSessionBindingsResponseSchema,
    });
    for (const binding of body.bindings) {
      this.externalSessionIds.set(binding.externalId, binding.sessionId);
    }
    return body.bindings;
  }

  /** Returns a cached session id or resolves/creates a binding for the external id. */
  async ensureSessionId(externalId: string): Promise<string> {
    const cachedSessionId = this.externalSessionIds.get(externalId);
    if (cachedSessionId) {
      this.currentSessionId = cachedSessionId;
      return cachedSessionId;
    }
    const result = await this.resolveSession(externalId);
    return result.session.sessionId;
  }

  /** Resolves or creates a binding for one provider conversation. */
  async resolveSession(externalId: string): Promise<ClientBridgeResolveSessionResult> {
    const body = await this.transport.requestJson({
      body: {
        externalId,
        provider: this.config.provider,
        ...(this.config.defaultSessionId ? { sessionId: this.config.defaultSessionId } : {}),
      },
      method: "POST",
      path: clientBridgeRoutes.clientBindingSession(),
      schema: clientSessionBindingResponseSchema,
    });
    this.currentSessionId = body.session.sessionId;
    this.externalSessionIds.set(externalId, body.session.sessionId);
    return body;
  }
}
