# @dungle-scrubs/tether-client

## 1.0.1

### Patch Changes

- Updated dependencies [0c6025d]
  - @dungle-scrubs/tether-protocol@0.3.0

## 1.0.0

### Major Changes

- 7764abb: Bound participant and observer event delivery by both a 2,000-item count and a
  16 MiB retained raw-frame byte high-water mark, pass an `AbortSignal` to event
  handlers, and hold replay until a timed-out handler settles. Give the observer
  finite delivery-recovery attempts with an inspectable paused state and bound
  both error backlogs to 32 entries with a dropped-error count.

  Claim-owned task mutations now require the current Claim ID: `completeTask`,
  `failTask`, and `refreshTaskClaim` take the server-issued `claimId` from the
  claimed record, and the claim flow declines a claim whose deadline is missing
  or is not an RFC 3339 timestamp with an offset.

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

- 21d51cf: Deliver participant and observer events serially with handled-only cursor advancement, bounded participant recovery, serialized durable cursor retries, subscriber-first runtime hooks, and awaitable graceful shutdown.

  Protocol error envelopes now preserve safe machine-readable replay reasons. Participant diagnostics add received, handled, and persisted cursors while retaining `lastObservedSeq` through the 0.2.x line as a deprecated alias of the corrected handled cursor.

  Durable cursor stores must persist sequence values with atomic maximum semantics so a timed-out earlier write cannot regress a later acknowledged cursor.

### Patch Changes

- Updated dependencies [bab400b]
- Updated dependencies [e15e555]
- Updated dependencies [1611a14]
- Updated dependencies [5905f76]
- Updated dependencies [08d7f1c]
- Updated dependencies [21d51cf]
- Updated dependencies [c682b66]
  - @dungle-scrubs/tether-protocol@0.2.0
