import type {
  SessionScalabilityBackfillOutcome,
  SessionScalabilityVerificationOutcome,
} from "@dungle-scrubs/tether-protocol";

const maximumTrackedSessions = 1_024;

/** Process-local, content-free outcomes produced by maintenance boundaries. */
export class SessionScalabilityRuntimeState {
  readonly #backfills = new Map<string, SessionScalabilityBackfillOutcome>();
  readonly #verifications = new Map<string, SessionScalabilityVerificationOutcome>();

  /** Records one bounded projection backfill outcome. */
  recordBackfill(
    sessionId: string,
    outcome: Exclude<SessionScalabilityBackfillOutcome, { readonly status: "not_observed" }>,
  ): void {
    setBounded(this.#backfills, sessionId, outcome);
  }

  /** Records one bounded projection verification outcome. */
  recordVerification(
    sessionId: string,
    outcome: Exclude<SessionScalabilityVerificationOutcome, { readonly status: "not_observed" }>,
  ): void {
    setBounded(this.#verifications, sessionId, outcome);
  }

  /** Reads process-local safe outcomes for one durable session. */
  read(sessionId: string): {
    readonly latestBackfill: SessionScalabilityBackfillOutcome;
    readonly latestVerification: SessionScalabilityVerificationOutcome;
  } {
    return {
      latestBackfill: this.#backfills.get(sessionId) ?? { status: "not_observed" },
      latestVerification: this.#verifications.get(sessionId) ?? { status: "not_observed" },
    };
  }
}

/** Shared tracker updated by service-owned projection operations. */
export const sessionScalabilityRuntimeState = new SessionScalabilityRuntimeState();

function setBounded<TValue>(map: Map<string, TValue>, key: string, value: TValue): void {
  map.delete(key);
  map.set(key, value);
  if (map.size <= maximumTrackedSessions) {
    return;
  }
  const oldest = map.keys().next().value;
  if (oldest !== undefined) {
    map.delete(oldest);
  }
}
