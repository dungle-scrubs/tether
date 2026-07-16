/**
 * Owns the deployment-order gate for the REST Control Epoch default flip. It
 * models binary capabilities rather than traffic volume because process-local
 * compatibility counters cannot prove dormant callers are gone.
 */

/** Server binary capabilities required before enforcement becomes the default. */
export interface RestControlReplicaCapabilities {
  readonly idempotentRestAcquisition: boolean;
  readonly noClaimCompatibility: boolean;
}

/** Returns whether every eligible replica is safe to run after the default flip. */
export function canFlipRestControlDefault(
  replicas: readonly RestControlReplicaCapabilities[],
): boolean {
  return (
    replicas.length > 0 &&
    replicas.every((replica) => replica.idempotentRestAcquisition && replica.noClaimCompatibility)
  );
}
