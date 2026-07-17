import type {
  HostPresenceInventory,
  LiveHostPresence,
  WebSocketPresenceEnvelope,
} from "@dungle-scrubs/tether-protocol";
import {
  buildWebSocketPresenceEnvelope,
  replicaPresenceScope,
} from "@dungle-scrubs/tether-protocol";

import type { SessionListItem } from "./types.js";

/**
 * Owns Host-presence session inventory projection and process-local passive
 * stream presence. It does not own native Tether participant registration,
 * durable control leases, task lifecycle behavior, or event persistence.
 */

export type HostPresenceState = "live" | "none" | "stale";
export type HostSessionActivity = "idle" | "queued" | "running" | "settled";

/** Process-local diagnostics for Host-presence stream classification. */
export interface HostPresenceDebugInfo {
  readonly nativeParticipantControlSocketCount: number;
  readonly passiveSocketCount: number;
}

/**
 * Passive read-only stream kinds selected by runtime-kind classification.
 *
 * - `host` and `viewer` deliver process-local host-presence frames alongside
 *   the durable event stream.
 * - `observer` is a passive full-event reader (bridges, approval observers,
 *   dashboards) that receives the durable event stream without presence frames,
 *   without registering a durable participant, and without acquiring or
 *   refreshing a control lease.
 */
export type HostPresenceStreamKind = "host" | "viewer" | "observer";

/** Tracks passive Host-presence sockets separately from durable participants. */
export class HostPresenceRuntime {
  private readonly hostsBySession = new Map<string, Map<string, LiveHostPresence>>();
  private readonly listenersBySession = new Map<string, Set<() => void>>();
  private nativeParticipantControlSocketCount = 0;
  private passiveSocketCount = 0;

  /** Returns payload-free process-local compatibility diagnostics. */
  debugInfo(): HostPresenceDebugInfo {
    return {
      nativeParticipantControlSocketCount: this.nativeParticipantControlSocketCount,
      passiveSocketCount: this.passiveSocketCount,
    };
  }

  /** Returns currently connected Host-presence hosts for one session. */
  hosts(sessionId: string): readonly LiveHostPresence[] {
    return [...(this.hostsBySession.get(sessionId)?.values() ?? [])].sort((left, right) =>
      left.instanceId.localeCompare(right.instanceId),
    );
  }

  /** Records a native participant-control socket lifetime for diagnostics. */
  registerNativeSocket(): () => void {
    this.nativeParticipantControlSocketCount += 1;
    return () => {
      this.nativeParticipantControlSocketCount = Math.max(
        0,
        this.nativeParticipantControlSocketCount - 1,
      );
    };
  }

  /** Records a passive Host-presence socket lifetime for diagnostics. */
  registerPassiveSocket(): () => void {
    this.passiveSocketCount += 1;
    return () => {
      this.passiveSocketCount = Math.max(0, this.passiveSocketCount - 1);
    };
  }

  /** Registers one live Host-presence host socket without writing durable participant rows. */
  upsertHost(sessionId: string, host: LiveHostPresence): void {
    const hosts = this.hostsBySession.get(sessionId) ?? new Map<string, LiveHostPresence>();
    hosts.set(host.instanceId, host);
    this.hostsBySession.set(sessionId, hosts);
  }

  /** Removes one live Host-presence host socket from process-local presence. */
  removeHost(sessionId: string, instanceId: string): void {
    const hosts = this.hostsBySession.get(sessionId);
    if (!hosts) {
      return;
    }
    hosts.delete(instanceId);
    if (hosts.size === 0) {
      this.hostsBySession.delete(sessionId);
    }
  }

  /** Registers a process-local presence listener for one passive stream socket. */
  subscribePresence(sessionId: string, listener: () => void): () => void {
    const listeners = this.listenersBySession.get(sessionId) ?? new Set<() => void>();
    listeners.add(listener);
    this.listenersBySession.set(sessionId, listeners);
    return () => {
      listeners.delete(listener);
      if (listeners.size === 0) {
        this.listenersBySession.delete(sessionId);
      }
    };
  }

  /** Notifies all passive stream sockets for a session that host presence changed. */
  notifyPresence(sessionId: string): void {
    for (const listener of this.listenersBySession.get(sessionId) ?? []) {
      listener();
    }
  }
}

/** Projects Tether operator inventory into the Host-presence superset. */
export function projectSessionInventory(input: {
  readonly replicaId: string;
  readonly runtime: HostPresenceRuntime;
  readonly sessions: readonly SessionListItem[];
}): HostPresenceInventory<SessionListItem> {
  return {
    replicaId: input.replicaId,
    scope: replicaPresenceScope,
    sessions: input.sessions.map((session) =>
      projectSession({
        liveHosts: input.runtime.hosts(session.sessionId),
        session,
      }),
    ),
  };
}

/** Projects local host state into the protocol-owned WebSocket envelope. */
export function projectWebSocketPresenceEnvelope(input: {
  readonly replicaId: string;
  readonly runtime: HostPresenceRuntime;
  readonly sessionId: string;
}): WebSocketPresenceEnvelope {
  return buildWebSocketPresenceEnvelope({
    hosts: input.runtime.hosts(input.sessionId),
    replicaId: input.replicaId,
  });
}

/** Finds one projected session summary by id for delete eligibility checks. */
export function findProjectedSession(input: {
  readonly liveHosts: readonly LiveHostPresence[];
  readonly session: SessionListItem | null;
}): SessionListItem | null {
  return input.session
    ? projectSession({
        liveHosts: input.liveHosts,
        session: input.session,
      })
    : null;
}

/** Applies Host-presence's permanent-delete eligibility rule to a projected session. */
export function permanentDeleteEligibility(summary: SessionListItem | null):
  | { readonly ok: true }
  | {
      readonly detail: string;
      readonly ok: false;
      readonly reason: "not-archived" | "not-found" | "protected";
    } {
  if (!summary) {
    return { detail: "session not found", ok: false, reason: "not-found" };
  }
  if (summary.archived !== true) {
    return {
      detail: "only archived sessions can be permanently deleted",
      ok: false,
      reason: "not-archived",
    };
  }
  if (summary.host === "live") {
    return { detail: "a host is live on this session", ok: false, reason: "protected" };
  }
  if (summary.activity === "running" || summary.activity === "queued") {
    return { detail: "a turn is active on this session", ok: false, reason: "protected" };
  }
  return { ok: true };
}

/** Classifies exact passive read-only runtime-kind values. */
export function classifyHostPresenceStream(
  runtimeKind: string | null,
): HostPresenceStreamKind | null {
  if (runtimeKind === "host") {
    return "host";
  }
  if (runtimeKind === "viewer") {
    return "viewer";
  }
  if (runtimeKind === "observer") {
    return "observer";
  }
  return null;
}

function projectSession(input: {
  readonly liveHosts: readonly LiveHostPresence[];
  readonly session: SessionListItem;
}): SessionListItem {
  return {
    ...input.session,
    host: input.liveHosts.length > 0 ? "live" : (input.session.host ?? "none"),
  };
}
