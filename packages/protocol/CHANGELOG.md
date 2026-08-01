# @dungle-scrubs/tether-protocol

## 0.3.0

### Minor Changes

- 0c6025d: Replace provider-specific scheduled-work identity with bounded versioned opaque scope keys, add generic completed-result target manifests, and return canonical approval records for first-committer-wins decisions.

  Add provider-neutral browser pairing, scoped operator authority, manifest-bound operator approval, command, and one-time WebSocket admission contracts.

  Add a browser-only operator client for pairing, bootstrap, snapshots, awaited replay, bounded reconnect, commands, and canonical approvals.

## 0.2.0

### Minor Changes

- e15e555: Add protocol-owned scalability diagnostics, reserved recovery taxonomy, and
  safe replay-window metadata. Participant and observer clients now preserve
  typed recovery details, and client bridges re-export the passive stream
  transport for external event consumers.
- 1611a14: Add protocol-owned REST participant acquisition, renewal, and release
  contracts. Publish the reusable REST participant control lifecycle client and
  compose it into bridge cancellation and approval. Bridge participant identity
  moves from per-operation inputs into task-client control configuration.

  The migration release retains explicit compatibility for one full published
  interval. Compatibility is removed in the immediately following breaking
  release.

- 5905f76: Add protocol-owned Host Presence inventory and WebSocket envelope validators
  with mandatory Replica Scope and replica identity metadata.
- 08d7f1c: Add a server-issued `claimId` to `TaskRecord` and require non-null
  `claimExpiresAt` values to be RFC 3339 timestamps with an offset. Claim-owned
  task mutations are fenced by the current Claim ID generation.
- 21d51cf: Deliver participant and observer events serially with handled-only cursor advancement, bounded participant recovery, serialized durable cursor retries, subscriber-first runtime hooks, and awaitable graceful shutdown.

  Protocol error envelopes now preserve safe machine-readable replay reasons. Participant diagnostics add received, handled, and persisted cursors while retaining `lastObservedSeq` through the 0.2.x line as a deprecated alias of the corrected handled cursor.

  Durable cursor stores must persist sequence values with atomic maximum semantics so a timed-out earlier write cannot regress a later acknowledged cursor.

- c682b66: Add protocol-owned Session Summary records, structured content, cumulative
  generation jobs, fenced candidate submissions, lifecycle inspection, and
  bounded failure contracts.

### Patch Changes

- bab400b: Preserve and validate durable schedule identity on task records across protocol boundaries.
