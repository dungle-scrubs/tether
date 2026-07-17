import {
  buildWebSocketPresenceEnvelope,
  replicaPresenceScope,
} from "@dungle-scrubs/tether-protocol";
import type {
  HostPresenceInventory,
  LiveHostPresence,
  WebSocketPresenceEnvelope,
} from "@dungle-scrubs/tether-protocol";

import type { SessionEvent, SessionLineage, SessionListItem, TangentAnchor } from "./types.js";

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
  readonly eventsBySession: ReadonlyMap<string, readonly SessionEvent[]>;
  readonly replicaId: string;
  readonly runtime: HostPresenceRuntime;
  readonly sessions: readonly SessionListItem[];
}): HostPresenceInventory<SessionListItem> {
  return {
    replicaId: input.replicaId,
    scope: replicaPresenceScope,
    sessions: input.sessions.map((session) =>
      projectSession({
        events: input.eventsBySession.get(session.sessionId) ?? [],
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
  readonly events: readonly SessionEvent[];
  readonly liveHosts: readonly LiveHostPresence[];
  readonly session: SessionListItem | null;
}): SessionListItem | null {
  return input.session
    ? projectSession({
        events: input.events,
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
  readonly events: readonly SessionEvent[];
  readonly liveHosts: readonly LiveHostPresence[];
  readonly session: SessionListItem;
}): SessionListItem {
  const hostOnline = latestEvent(input.events, "host.online");
  const firstUser = input.events.find((event) => event.type === "user.message") ?? null;
  const titleEvent = latestEvent(input.events, "session.title");
  const archivedEvent = latestEvent(input.events, "session.archived");
  const deletedEvent = latestEvent(input.events, "session.deleted");
  const forkedEvent = latestEvent(input.events, "session.forkedFrom");
  const tangentEvent = latestEvent(input.events, "session.tangentOf");
  const lifecycle = input.events.filter(
    (event) =>
      event.type === "assistant.started" ||
      event.type === "assistant.completed" ||
      event.type === "user.command",
  );
  const hostPayload = hostOnline?.payload ?? {};
  const cwd = stringField(hostPayload, "cwd");
  const workspace = stringField(hostPayload, "workspace");
  return {
    ...input.session,
    activity: activityFromLog(lifecycle),
    archived: booleanField(archivedEvent?.payload, "archived") ?? false,
    branch: stringField(hostPayload, "branch"),
    cwd,
    deleted: booleanField(deletedEvent?.payload, "deleted") ?? false,
    forkedFrom: forkedFromPayload(forkedEvent),
    git: recordField(hostPayload, "git"),
    host: hostPresence(input.liveHosts, hostOnline),
    project: projectOf(workspace, cwd),
    tangentOf: tangentOfPayload(tangentEvent, input.session.createdAt),
    title: titleFrom(firstUser, titleEvent, input.session.sessionId),
    updatedAt: input.session.lastEventAt ?? input.session.createdAt,
    workspace,
  };
}

function latestEvent(events: readonly SessionEvent[], type: string): SessionEvent | null {
  for (let index = events.length - 1; index >= 0; index -= 1) {
    const event = events[index];
    if (event?.type === type) {
      return event;
    }
  }
  return null;
}

function titleFrom(
  firstUser: SessionEvent | null,
  titleEvent: SessionEvent | null,
  sessionId: string,
): string {
  const explicitTitle = stringField(titleEvent?.payload, "title")?.trim().replace(/\s+/gu, " ");
  if (explicitTitle) {
    return truncateTitle(explicitTitle);
  }
  const userText =
    stringField(firstUser?.payload, "text") ??
    stringField(firstUser?.payload, "message") ??
    stringField(recordField(firstUser?.payload, "message"), "text");
  const normalized = userText?.trim().replace(/\s+/gu, " ");
  return normalized ? truncateTitle(normalized) : sessionId;
}

function truncateTitle(value: string): string {
  return value.length > 60 ? value.slice(0, 60) : value;
}

function activityFromLog(events: readonly SessionEvent[]): HostSessionActivity {
  const completed = new Set<string>();
  let lastStarted: string | null = null;
  let everCompleted = false;
  for (const event of events) {
    if (event.type === "user.command" && stringField(event.payload, "command") === "/clear") {
      completed.clear();
      everCompleted = false;
      lastStarted = null;
      continue;
    }
    const runId = stringField(event.payload, "runId");
    if (event.type === "assistant.started" && runId) {
      lastStarted = runId;
      continue;
    }
    if (event.type === "assistant.completed" && runId) {
      completed.add(runId);
      everCompleted = true;
    }
  }
  return lastStarted !== null && !completed.has(lastStarted)
    ? "running"
    : everCompleted
      ? "settled"
      : "idle";
}

function hostPresence(
  liveHosts: readonly LiveHostPresence[],
  hostOnline: SessionEvent | null,
): HostPresenceState {
  if (liveHosts.length > 0) {
    return "live";
  }
  return hostOnline ? "stale" : "none";
}

function forkedFromPayload(event: SessionEvent | null): SessionLineage | null {
  const parentSessionId = stringField(event?.payload, "parentSessionId");
  const forkSeq = numberField(event?.payload, "forkSeq");
  return parentSessionId && forkSeq !== null ? { forkSeq, parentSessionId } : null;
}

function tangentOfPayload(
  event: SessionEvent | null,
  fallbackCreatedAt: string,
): TangentAnchor | null {
  const parentSessionId = stringField(event?.payload, "parentSessionId");
  const sourceMessageId = stringField(event?.payload, "sourceMessageId");
  const quote = stringField(event?.payload, "quote");
  if (!parentSessionId || !sourceMessageId || !quote) {
    return null;
  }
  return {
    createdAt: event?.createdAt ?? fallbackCreatedAt,
    label: stringField(event?.payload, "label"),
    parentSessionId,
    quote,
    sourceMessageId,
  };
}

function projectOf(workspace: string | null, cwd: string | null): string | null {
  const path = workspace ?? cwd;
  if (!path) {
    return null;
  }
  const trimmed = path.replace(/\/+$/u, "");
  const base = trimmed.split("/").at(-1);
  return base && base.length > 0 ? base : trimmed;
}

function stringField(value: unknown, key: string): string | null {
  if (!isRecord(value)) {
    return null;
  }
  const field = value[key];
  return typeof field === "string" ? field : null;
}

function booleanField(value: unknown, key: string): boolean | null {
  if (!isRecord(value)) {
    return null;
  }
  const field = value[key];
  return typeof field === "boolean" ? field : null;
}

function numberField(value: unknown, key: string): number | null {
  if (!isRecord(value)) {
    return null;
  }
  const field = value[key];
  return typeof field === "number" && Number.isSafeInteger(field) ? field : null;
}

function recordField(value: unknown, key: string): Record<string, unknown> | null {
  if (!isRecord(value)) {
    return null;
  }
  const field = value[key];
  return isRecord(field) ? field : null;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}
