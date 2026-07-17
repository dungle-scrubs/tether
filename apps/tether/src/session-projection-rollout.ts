/**
 * Deployment-order guards for Session Projection reducer and event contracts.
 * The module evaluates capabilities reported by an externally supplied active
 * replica set. It does not discover replicas or enable contracts itself.
 */

import {
  SESSION_PROJECTION_EVENT_CONTRACTS,
  SESSION_PROJECTION_REDUCER_VERSION,
} from "./session-projection.js";

/** Projection capabilities reported by one active Tether replica. */
export interface SessionProjectionReplicaCapabilities {
  readonly eventContracts: readonly string[];
  readonly reducerVersion: number;
  readonly replicaId: string;
}

/** Builds the capabilities reported by the current Tether binary. */
export function sessionProjectionReplicaCapabilities(
  replicaId: string,
): SessionProjectionReplicaCapabilities {
  return {
    eventContracts: SESSION_PROJECTION_EVENT_CONTRACTS,
    reducerVersion: SESSION_PROJECTION_REDUCER_VERSION,
    replicaId,
  };
}

/** Returns whether every supplied active replica runs the current reducer. */
export function canUseCurrentSessionProjectionReducer(
  replicas: readonly SessionProjectionReplicaCapabilities[],
): boolean {
  return (
    replicas.length > 0 &&
    replicas.every((replica) => replica.reducerVersion === SESSION_PROJECTION_REDUCER_VERSION)
  );
}

/** Inputs for activating one newly projection-affecting event contract. */
export interface SessionProjectionEventContractActivation {
  readonly eventContract: string;
  readonly reducerVersion: number;
}

/**
 * Returns whether every active replica advertises both the exact reducer and
 * the event contract before producers are allowed to emit that contract.
 */
export function canActivateSessionProjectionEventContract(
  replicas: readonly SessionProjectionReplicaCapabilities[],
  activation: SessionProjectionEventContractActivation,
): boolean {
  return (
    replicas.length > 0 &&
    Number.isSafeInteger(activation.reducerVersion) &&
    activation.reducerVersion > 0 &&
    replicas.every(
      (replica) =>
        replica.reducerVersion === activation.reducerVersion &&
        replica.eventContracts.includes(activation.eventContract),
    )
  );
}
