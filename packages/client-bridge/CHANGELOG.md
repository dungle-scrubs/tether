# @dungle-scrubs/tether-client-bridge

## 2.0.0

### Major Changes

- 0c6025d: Replace provider-specific scheduled-work identity with bounded versioned opaque scope keys, add generic completed-result target manifests, and return canonical approval records for first-committer-wins decisions.

  Add provider-neutral browser pairing, scoped operator authority, manifest-bound operator approval, command, and one-time WebSocket admission contracts.

  Add a browser-only operator client for pairing, bootstrap, snapshots, awaited replay, bounded reconnect, commands, and canonical approvals.

### Patch Changes

- Updated dependencies [0c6025d]
  - @dungle-scrubs/tether-protocol@0.3.0
  - @dungle-scrubs/tether-client@1.0.1

## 1.0.0

### Major Changes

- 1611a14: Add protocol-owned REST participant acquisition, renewal, and release
  contracts. Publish the reusable REST participant control lifecycle client and
  compose it into bridge cancellation and approval. Bridge participant identity
  moves from per-operation inputs into task-client control configuration.

  The migration release retains explicit compatibility for one full published
  interval. Compatibility is removed in the immediately following breaking
  release.

- 7764abb: Bound `ClientBridgeSessionResolver` with a deterministic 1,024-entry LRU cache,
  30-minute idle expiry, explicit `invalidate`, same-key in-flight deduplication,
  and a 64-entry distinct in-flight cap that rejects overflow with the new
  `ClientBridgeSessionResolverResourceLimitError`. These bounds cannot be
  disabled. The resolver debug shape now reports bounded cache, in-flight,
  eviction, and expiry counters in place of `resolvedExternalIdCount`.

### Minor Changes

- e15e555: Add protocol-owned scalability diagnostics, reserved recovery taxonomy, and
  safe replay-window metadata. Participant and observer clients now preserve
  typed recovery details, and client bridges re-export the passive stream
  transport for external event consumers.

### Patch Changes

- Updated dependencies [bab400b]
- Updated dependencies [e15e555]
- Updated dependencies [1611a14]
- Updated dependencies [5905f76]
- Updated dependencies [08d7f1c]
- Updated dependencies [7764abb]
- Updated dependencies [21d51cf]
- Updated dependencies [c682b66]
  - @dungle-scrubs/tether-protocol@0.2.0
  - @dungle-scrubs/tether-client@1.0.0
