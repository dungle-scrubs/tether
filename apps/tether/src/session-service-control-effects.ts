import { Effect } from "effect";

import type { SessionPersistenceStores } from "./db-store-contracts.js";
import type { ModuleObservability } from "./observability.js";
import { newParticipantId } from "./protocol.js";
import {
  type ControlProtectedResult,
  type HeartbeatParticipantInput,
  type RegisteredParticipantResult,
  type RegisterParticipantInput,
  type RegisterWebSocketParticipantInput,
  type RestControlledInput,
  type RestControlOutcome,
  restControlLeaseTtlMs,
  type SessionServiceFailure,
} from "./session-service-contracts.js";
import { catchAtomicEpochStale, trySessionPromise } from "./session-service-runtime.js";
import type { AssertBroadcastEvents } from "./session-service-task-effects.js";
import type { ParticipantRecord, ParticipantRuntimeKind } from "./types.js";

/** Dependencies for participant control and presence Effect builders. */
export interface SessionControlEffectsInput {
  readonly assertBroadcastEvents: AssertBroadcastEvents;
  /**
   * Whether a control-protected REST request that omits its Control Epoch is
   * rejected. Off preserves legacy REST callers; a supplied epoch is always
   * validated regardless of this flag.
   */
  readonly controlEpochEnforcement: boolean;
  readonly eventSourceId: string;
  readonly observability: ModuleObservability;
  readonly stores: SessionPersistenceStores;
  readonly wsControlLeaseTtlMs: number;
}

/** Participant control and presence Effect programs used by the session service. */
export interface SessionControlEffects {
  readonly claimRestControlEffect: (
    input: RestControlledInput,
  ) => Effect.Effect<RestControlOutcome, SessionServiceFailure>;
  readonly heartbeatRestParticipantEffect: (
    input: HeartbeatParticipantInput,
  ) => Effect.Effect<
    ControlProtectedResult<{ readonly participant: ParticipantRecord | null }>,
    SessionServiceFailure
  >;
  readonly refreshWebSocketControlLeaseEffect: (input: {
    readonly controlEpoch: number;
    readonly instanceId: string;
    readonly participantId: string;
    readonly sessionId: string;
  }) => Effect.Effect<ControlProtectedResult<{ readonly refreshed: true }>, SessionServiceFailure>;
  readonly registerRestParticipantEffect: (
    input: RegisterParticipantInput,
  ) => Effect.Effect<ControlProtectedResult<RegisteredParticipantResult>, SessionServiceFailure>;
  readonly registerWebSocketParticipantEffect: (
    input: RegisterWebSocketParticipantInput,
  ) => Effect.Effect<
    ControlProtectedResult<
      RegisteredParticipantResult & {
        readonly context: {
          readonly controlEpoch: number;
          readonly instanceId: string;
          readonly participantId: string;
        };
      }
    >,
    SessionServiceFailure
  >;
  readonly releaseControlLeaseEffect: (input: {
    readonly controlChannel: "rest" | "ws";
    readonly controlEpoch?: number;
    readonly instanceId: string;
    readonly participantId: string;
    readonly sessionId: string;
  }) => Effect.Effect<void, SessionServiceFailure>;
}

/** Builds participant control and presence effects for one service instance. */
export function createSessionControlEffects(
  input: SessionControlEffectsInput,
): SessionControlEffects {
  const claimRestControlEffect = (
    restInput: RestControlledInput,
  ): Effect.Effect<RestControlOutcome, SessionServiceFailure> =>
    trySessionPromise(() =>
      validateRestControl(input.stores, restInput, input.controlEpochEnforcement),
    );
  const upsertVisibleParticipantEffect = (participantInput: {
    readonly capabilities: Record<string, unknown>;
    readonly controlEpoch: number;
    readonly displayName: string;
    readonly participantId: string;
    readonly runtimeKind: ParticipantRuntimeKind;
    readonly sessionId: string;
  }): Effect.Effect<RegisteredParticipantResult, SessionServiceFailure> =>
    Effect.gen(function* () {
      const result = yield* trySessionPromise(() =>
        input.stores.participants.upsertWithEvent({
          capabilities: participantInput.capabilities,
          displayName: participantInput.displayName,
          eventSourceId: input.eventSourceId,
          participantId: participantInput.participantId,
          runtimeKind: participantInput.runtimeKind,
          sessionId: participantInput.sessionId,
        }),
      );
      return {
        controlEpoch: participantInput.controlEpoch,
        events: result.events,
        participant: result.registration.participant,
        registrationStatus: result.registration.status,
        status: "ok" as const,
      };
    });

  return {
    claimRestControlEffect,
    heartbeatRestParticipantEffect: (heartbeatInput) =>
      Effect.gen(function* () {
        const control = yield* claimRestControlEffect(heartbeatInput);
        if (control.status !== "ok") {
          return control;
        }
        // Fence the presence refresh and the durable heartbeat event append to
        // the supplied epoch inside one transaction. Validating the epoch atomically
        // before the participant row mutation ensures a superseded socket can
        // neither refresh presence/capabilities nor write a heartbeat event. When
        // no epoch is supplied (enforcement off), the guard is absent and legacy
        // behavior is preserved.
        const controlGuard =
          heartbeatInput.controlEpoch !== undefined
            ? {
                controlChannel: "rest" as const,
                controlEpoch: heartbeatInput.controlEpoch,
                instanceId: heartbeatInput.instanceId,
                participantId: heartbeatInput.participantId,
                sessionId: heartbeatInput.sessionId,
              }
            : undefined;
        const heartbeatOrStale = yield* catchAtomicEpochStale(
          trySessionPromise(() =>
            input.stores.participants.heartbeatWithEvent({
              ...(heartbeatInput.capabilities !== undefined
                ? { capabilities: heartbeatInput.capabilities }
                : {}),
              ...(controlGuard !== undefined ? { controlGuard } : {}),
              eventSourceId: input.eventSourceId,
              participantId: heartbeatInput.participantId,
              sessionId: heartbeatInput.sessionId,
            }),
          ),
        );
        if ("status" in heartbeatOrStale) {
          return heartbeatOrStale;
        }
        if (heartbeatOrStale.participant === null) {
          return { events: [], participant: null, status: "ok" as const };
        }
        const result = {
          events: [heartbeatOrStale.event],
          participant: heartbeatOrStale.participant,
          status: "ok" as const,
        };
        yield* Effect.sync(() =>
          input.assertBroadcastEvents(
            input.observability,
            "heartbeatRestParticipant",
            heartbeatInput.sessionId,
            result.events,
            1,
          ),
        );
        return result;
      }),
    refreshWebSocketControlLeaseEffect: (leaseInput) =>
      Effect.gen(function* () {
        const renewal = yield* trySessionPromise(() =>
          input.stores.controlLeases.renew({
            controlChannel: "ws",
            controlEpoch: leaseInput.controlEpoch,
            instanceId: leaseInput.instanceId,
            leaseTtlMs: input.wsControlLeaseTtlMs,
            participantId: leaseInput.participantId,
            sessionId: leaseInput.sessionId,
          }),
        );
        if (renewal.status === "renewed") {
          return { events: [], refreshed: true as const, status: "ok" as const };
        }
        if (renewal.status === "conflict") {
          return { leaseClaim: renewal, status: "control_conflict" as const };
        }
        return {
          currentEpoch: renewal.status === "stale" ? renewal.currentEpoch : null,
          status: "control_epoch_stale" as const,
        };
      }),
    registerRestParticipantEffect: (participantInput) =>
      Effect.gen(function* () {
        const participantId =
          participantInput.participantId ?? newParticipantId(participantInput.runtimeKind);
        const instanceId = participantInput.instanceId ?? participantId;
        const claim = yield* trySessionPromise(() =>
          input.stores.controlLeases.claim({
            controlChannel: "rest",
            instanceId,
            leaseTtlMs: restControlLeaseTtlMs,
            participantId,
            sessionId: participantInput.sessionId,
          }),
        );
        if (claim.status === "conflict") {
          return { leaseClaim: claim, status: "control_conflict" as const };
        }
        return yield* upsertVisibleParticipantEffect({
          capabilities: participantInput.capabilities,
          controlEpoch: claim.lease.epoch,
          displayName: participantInput.displayName ?? participantId,
          participantId,
          runtimeKind: participantInput.runtimeKind,
          sessionId: participantInput.sessionId,
        }).pipe(
          Effect.catchAll((error) =>
            releaseClaimedControlLease(input.stores, {
              controlChannel: "rest",
              controlEpoch: claim.lease.epoch,
              instanceId,
              participantId,
              sessionId: participantInput.sessionId,
            }).pipe(Effect.flatMap(() => Effect.fail(error))),
          ),
        );
      }),
    registerWebSocketParticipantEffect: (participantInput) =>
      Effect.gen(function* () {
        const claim = yield* trySessionPromise(() =>
          input.stores.controlLeases.claim({
            controlChannel: "ws",
            instanceId: participantInput.instanceId,
            leaseTtlMs: input.wsControlLeaseTtlMs,
            participantId: participantInput.participantId,
            sessionId: participantInput.sessionId,
          }),
        );
        if (claim.status === "conflict") {
          return { leaseClaim: claim, status: "control_conflict" as const };
        }
        // The claim already committed the new epoch, superseding any prior socket.
        // If the participant upsert then fails, the just-claimed lease is released
        // so a half-applied superseding epoch is not left dangling behind a failed
        // registration. This mirrors the REST compensation above.
        return yield* upsertVisibleParticipantEffect({
          capabilities: participantInput.capabilities,
          controlEpoch: claim.lease.epoch,
          displayName: participantInput.displayName,
          participantId: participantInput.participantId,
          runtimeKind: participantInput.runtimeKind,
          sessionId: participantInput.sessionId,
        }).pipe(
          Effect.map((registration) => ({
            context: {
              controlEpoch: claim.lease.epoch,
              instanceId: participantInput.instanceId,
              participantId: participantInput.participantId,
            },
            controlEpoch: claim.lease.epoch,
            events: registration.events,
            participant: registration.participant,
            registrationStatus: registration.registrationStatus,
            status: "ok" as const,
          })),
          Effect.catchAll((error) =>
            releaseClaimedControlLease(input.stores, {
              controlChannel: "ws",
              controlEpoch: claim.lease.epoch,
              instanceId: participantInput.instanceId,
              participantId: participantInput.participantId,
              sessionId: participantInput.sessionId,
            }).pipe(Effect.flatMap(() => Effect.fail(error))),
          ),
        );
      }),
    releaseControlLeaseEffect: (leaseInput) =>
      trySessionPromise(() => input.stores.controlLeases.release(leaseInput)),
  };
}

/**
 * Releases a control lease that was claimed immediately before participant
 * registration failed, on the REST or WebSocket channel. The lease claim already
 * committed and may have superseded a prior epoch, so rolling it back prevents a
 * half-applied superseding lease from being left dangling behind a failed
 * registration and blocking retries. The compensating release is best-effort: a
 * release failure is swallowed so it never masks the original registration error.
 */
function releaseClaimedControlLease(
  stores: SessionPersistenceStores,
  input: {
    readonly controlChannel: "rest" | "ws";
    readonly controlEpoch: number;
    readonly instanceId: string;
    readonly participantId: string;
    readonly sessionId: string;
  },
): Effect.Effect<void, never> {
  return trySessionPromise(() =>
    stores.controlLeases.release({
      controlChannel: input.controlChannel,
      controlEpoch: input.controlEpoch,
      instanceId: input.instanceId,
      participantId: input.participantId,
      sessionId: input.sessionId,
    }),
  ).pipe(Effect.catchAll(() => Effect.void));
}

/**
 * Validates REST participant control before a protected mutation. When the
 * caller supplies a Control Epoch it is compared against the current durable
 * generation (renewal). A missing epoch is rejected only when enforcement is
 * enabled; otherwise the legacy acquire-or-supersede path keeps existing
 * callers working.
 */
async function validateRestControl(
  stores: SessionPersistenceStores,
  input: RestControlledInput,
  enforcement: boolean,
): Promise<RestControlOutcome> {
  if (input.controlEpoch !== undefined) {
    const renewal = await stores.controlLeases.renew({
      controlChannel: "rest",
      controlEpoch: input.controlEpoch,
      instanceId: input.instanceId,
      leaseTtlMs: restControlLeaseTtlMs,
      participantId: input.participantId,
      sessionId: input.sessionId,
    });
    if (renewal.status === "renewed") {
      return { status: "ok" };
    }
    if (renewal.status === "conflict") {
      return { leaseClaim: renewal, status: "control_conflict" };
    }
    return {
      currentEpoch: renewal.status === "stale" ? renewal.currentEpoch : null,
      status: "control_epoch_stale",
    };
  }
  if (enforcement) {
    return { currentEpoch: null, status: "control_epoch_stale" };
  }
  const claim = await stores.controlLeases.claim({
    controlChannel: "rest",
    instanceId: input.instanceId,
    leaseTtlMs: restControlLeaseTtlMs,
    participantId: input.participantId,
    sessionId: input.sessionId,
  });
  return claim.status === "conflict"
    ? { leaseClaim: claim, status: "control_conflict" }
    : { status: "ok" };
}
