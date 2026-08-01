# @dungle-scrubs/tether-browser

## 0.2.0

### Minor Changes

- 0c6025d: Replace provider-specific scheduled-work identity with bounded versioned opaque scope keys, add generic completed-result target manifests, and return canonical approval records for first-committer-wins decisions.

  Add provider-neutral browser pairing, scoped operator authority, manifest-bound operator approval, command, and one-time WebSocket admission contracts.

  Add a browser-only operator client for pairing, bootstrap, snapshots, awaited replay, bounded reconnect, commands, and canonical approvals.

### Patch Changes

- b7764ae: Build the browser package before typechecking its compiled-entry test so clean release checkouts pass the commit gate.
- Updated dependencies [0c6025d]
  - @dungle-scrubs/tether-protocol@0.3.0
