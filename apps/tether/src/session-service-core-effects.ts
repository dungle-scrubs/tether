import { Effect } from "effect";

import type { SessionPersistenceStores } from "./db-store-contracts.js";
import {
  type AppendSessionEventInput,
  buildPublishedEventInput,
  buildSessionCreatedEventInput,
  newSessionId,
} from "./protocol.js";
import type {
  ClientSessionBindingResult,
  ControlProtectedResult,
  PublishEventInput,
  PublishedEventResult,
  PublishRestEventInput,
  ResolveClientSessionInput,
  RestControlledInput,
  RestControlOutcome,
  SessionCreatedResult,
  SessionServiceFailure,
} from "./session-service-contracts.js";
import { catchAtomicEpochStale, trySessionPromise } from "./session-service-runtime.js";
import type { ClientSessionBindingRecord, SessionEvent, SessionRecord } from "./types.js";

/** Verifies broadcast event invariants for one service operation. */
export type AssertBroadcastEvents = (
  operation: string,
  sessionId: string,
  events: readonly SessionEvent[],
  expectedCount?: number,
) => void;

/** Appends a durable event using the owning service source id. */
export type AppendEventEffect = (
  input: AppendSessionEventInput,
) => Effect.Effect<SessionEvent, SessionServiceFailure>;

/** Validates the REST control lease and Control Epoch for a participant operation. */
export type ClaimRestControlEffect = (
  input: RestControlledInput,
  routeName?: string,
) => Effect.Effect<RestControlOutcome, SessionServiceFailure>;

/** Dependencies for durable session, binding, and event Effect builders. */
export interface SessionCoreEffectsInput {
  readonly appendEventEffect: AppendEventEffect;
  readonly assertBroadcastEvents: AssertBroadcastEvents;
  readonly claimRestControlEffect: ClaimRestControlEffect;
  /**
   * Whether a control-protected REST request that omits its fenced control
   * context is rejected. A participant-owned publish is always fenced when a
   * context is supplied; enforcement additionally rejects a missing context.
   */
  readonly controlEpochEnforcement: boolean;
  readonly eventSourceId: string;
  readonly stores: SessionPersistenceStores;
}

/** Durable session, binding, and generic event Effect programs. */
export interface SessionCoreEffects {
  readonly archiveClientSessionBindingEffect: (input: {
    readonly externalId: string;
    readonly provider: string;
  }) => Effect.Effect<ClientSessionBindingRecord | null, SessionServiceFailure>;
  readonly createSessionEffect: (input: {
    readonly sessionId: string | undefined;
  }) => Effect.Effect<SessionCreatedResult, SessionServiceFailure>;
  readonly deleteSessionEffect: (input: {
    readonly sessionId: string;
  }) => Effect.Effect<boolean, SessionServiceFailure>;
  readonly ensurePublicSessionEffect: (input: {
    readonly sessionId: string | undefined;
  }) => Effect.Effect<
    { readonly created: boolean; readonly session: SessionRecord },
    SessionServiceFailure
  >;
  readonly listClientSessionBindingsEffect: (input?: {
    readonly provider?: string | undefined;
  }) => Effect.Effect<ClientSessionBindingRecord[], SessionServiceFailure>;
  readonly publishEventEffect: (
    input: PublishEventInput,
  ) => Effect.Effect<PublishedEventResult, SessionServiceFailure>;
  readonly publishRestEventEffect: (
    input: PublishRestEventInput,
  ) => Effect.Effect<ControlProtectedResult<PublishedEventResult>, SessionServiceFailure>;
  readonly resolveClientSessionEffect: (
    input: ResolveClientSessionInput,
  ) => Effect.Effect<ClientSessionBindingResult, SessionServiceFailure>;
}

/** Builds core session effects for one service instance. */
export function createSessionCoreEffects(input: SessionCoreEffectsInput): SessionCoreEffects {
  const createSessionEffect = (createInput: {
    readonly sessionId: string | undefined;
  }): Effect.Effect<SessionCreatedResult, SessionServiceFailure> =>
    Effect.gen(function* () {
      const session = yield* trySessionPromise(() =>
        input.stores.sessions.create(createInput.sessionId ?? newSessionId()),
      );
      const events = session.created
        ? [yield* input.appendEventEffect(buildSessionCreatedEventInput(session.session.sessionId))]
        : [];
      const result = { events, session: session.session };
      yield* Effect.sync(() =>
        input.assertBroadcastEvents(
          "createSession",
          result.session.sessionId,
          result.events,
          session.created ? 1 : 0,
        ),
      );
      return result;
    });

  const publishEventEffect = (
    publishInput: PublishEventInput,
  ): Effect.Effect<PublishedEventResult, SessionServiceFailure> =>
    Effect.gen(function* () {
      const eventInput = buildPublishedEventInput({
        ...(publishInput.eventId !== undefined ? { eventId: publishInput.eventId } : {}),
        payload: publishInput.payload,
        producerId: publishInput.producerId,
        sessionId: publishInput.sessionId,
        type: publishInput.type,
      });
      // A participant-owned producer carries a controlGuard; the epoch is then
      // validated in the same transaction that appends the event so a superseded
      // socket can never append after being fenced. Unfenced publishes keep the
      // shared append effect so their behavior is unchanged.
      const controlGuard = publishInput.controlGuard;
      const appendOptions = {
        ...(controlGuard !== undefined ? { controlGuard } : {}),
        sourceId: input.eventSourceId,
      };
      const appendEvent: Effect.Effect<SessionEvent, SessionServiceFailure> =
        controlGuard === undefined
          ? input.appendEventEffect(eventInput)
          : trySessionPromise(
              () => input.stores.events.append(eventInput, appendOptions),
              "append",
            );
      const result =
        publishInput.eventId === undefined
          ? yield* appendEvent.pipe(
              Effect.map((event) => ({
                event,
                events: [event] as const,
                status: "created" as const,
              })),
            )
          : yield* trySessionPromise(() =>
              input.stores.events.appendIdempotent(eventInput, appendOptions),
            );
      yield* Effect.sync(() =>
        input.assertBroadcastEvents(
          "publishEvent",
          publishInput.sessionId,
          result.events,
          result.status === "created" ? 1 : 0,
        ),
      );
      return result;
    });

  return {
    archiveClientSessionBindingEffect: (bindingInput) =>
      trySessionPromise(() => input.stores.clientBindings.archive(bindingInput)),
    createSessionEffect,
    deleteSessionEffect: (deleteInput) =>
      trySessionPromise(() => input.stores.sessions.delete(deleteInput.sessionId)),
    ensurePublicSessionEffect: (ensureInput) =>
      Effect.gen(function* () {
        const result = yield* trySessionPromise(() =>
          input.stores.sessions.create(ensureInput.sessionId ?? newSessionId()),
        );
        yield* Effect.sync(() =>
          input.assertBroadcastEvents("ensurePublicSession", result.session.sessionId, [], 0),
        );
        return { created: result.created, session: result.session };
      }),
    listClientSessionBindingsEffect: (bindingInput = {}) =>
      trySessionPromise(() => input.stores.clientBindings.list(bindingInput)),
    publishEventEffect,
    publishRestEventEffect: (publishInput) =>
      Effect.gen(function* () {
        // Every client REST publish is a participant-owned producer: the publish
        // policy resolves producerId to the participant identity and denies the
        // system producer. Such a publish must be epoch-fenced independently of
        // whether instanceId was supplied, otherwise enforcement could be
        // bypassed by omitting instanceId.
        if (publishInput.instanceId !== undefined) {
          const control = yield* input.claimRestControlEffect(
            {
              ...(publishInput.controlEpoch !== undefined
                ? { controlEpoch: publishInput.controlEpoch }
                : {}),
              instanceId: publishInput.instanceId,
              participantId: publishInput.producerId,
              sessionId: publishInput.sessionId,
            },
            "session.events.append",
          );
          if (control.status !== "ok") {
            return control;
          }
          const controlGuard =
            publishInput.controlEpoch !== undefined
              ? {
                  controlChannel: "rest" as const,
                  controlEpoch: publishInput.controlEpoch,
                  instanceId: publishInput.instanceId,
                  participantId: publishInput.producerId,
                  sessionId: publishInput.sessionId,
                }
              : undefined;
          return yield* catchAtomicEpochStale(
            publishEventEffect({
              ...publishInput,
              ...(controlGuard !== undefined ? { controlGuard } : {}),
            }),
          );
        }
        // instanceId absent: the fenced control context is incomplete. A caller
        // that supplied a controlEpoch is attempting a fenced publish but cannot
        // be fenced without an instanceId to bind the epoch to, so it is rejected
        // as stale regardless of enforcement rather than silently downgraded to an
        // unfenced append. The legacy unfenced fallback applies only when neither a
        // controlEpoch nor a control context is supplied and enforcement is off, so
        // existing pre-epoch clients keep working.
        if (publishInput.controlEpoch === undefined && input.controlEpochEnforcement) {
          return { status: "control_epoch_required" as const };
        }
        if (publishInput.controlEpoch !== undefined) {
          return { currentEpoch: null, status: "control_epoch_stale" as const };
        }
        return yield* publishEventEffect(publishInput);
      }),
    resolveClientSessionEffect: (resolveInput) =>
      Effect.gen(function* () {
        const binding = yield* trySessionPromise(() =>
          input.stores.clientBindings.upsert({
            externalId: resolveInput.externalId,
            provider: resolveInput.provider,
            ...(resolveInput.sessionId !== undefined ? { sessionId: resolveInput.sessionId } : {}),
          }),
        );
        const session = yield* trySessionPromise(() =>
          input.stores.sessions.read(binding.binding.sessionId),
        );
        const events = binding.sessionCreated
          ? [yield* input.appendEventEffect(buildSessionCreatedEventInput(session.sessionId))]
          : [];
        yield* Effect.sync(() =>
          input.assertBroadcastEvents(
            "resolveClientSession",
            session.sessionId,
            events,
            events.length,
          ),
        );
        return {
          binding: binding.binding,
          bindingStatus: binding.status,
          created: binding.created,
          events,
          session,
        };
      }),
  };
}
