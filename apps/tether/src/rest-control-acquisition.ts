/**
 * Public composite REST participant-control acquisition seam. The module owns
 * retry-idempotent acquisition semantics while the database adapter owns the
 * PostgreSQL transaction implementation and advisory-lock mechanics.
 */

import {
  acquireRestParticipantControl,
  type DatabasePool,
  type RestControlAcquisition,
} from "./db.js";

/** Input for one logical REST participant control acquisition. */
export interface RestControlAcquisitionInput {
  readonly acquisitionId: string;
  readonly capabilities: Record<string, unknown>;
  readonly displayName: string;
  readonly eventSourceId: string;
  readonly instanceId: string;
  readonly leaseTtlMs: number;
  readonly participantId: string;
  readonly runtimeKind: string;
  readonly sessionId: string;
}

export type { RestControlAcquisition };

/** Runs one atomic, retry-idempotent REST participant acquisition. */
export function acquireRestControl(
  database: DatabasePool,
  input: RestControlAcquisitionInput,
): Promise<RestControlAcquisition> {
  return acquireRestParticipantControl(database, input);
}
