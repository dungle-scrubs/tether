# @dungle-scrubs/tether-browser

## 0.3.0

### Minor Changes

- 8e090b8: Deduplicate the browser operator session wire shape and share transport helpers.

  - `browserOperatorSessionSchema` no longer carries a top-level `sessionIds`
    copy of `scope.sessionIds`; the `/operator/browser-session` response emits
    the list once, under `scope`.
  - `BrowserOperatorClient` throws the new `BrowserOperatorConfigurationError`
    for local precondition failures (missing CSRF token, invalid service URL)
    instead of a fake `BrowserOperatorHttpError` with status 0.
  - Browser session stream pause reasons stay in the protocol-owned recovery
    taxonomy, falling back to `server_stream_error` for unknown server reasons.
  - New shared protocol exports: `buildSessionStreamUrl`,
    `serialDeliveryFailureReason`, `utf8ByteLength`, `exactHttpOriginSchema`,
    `participantRecordSchema`, `defaultReconnectDelayPolicy`,
    `boundedReconnectDelayMs`; client and browser packages delegate to them.
  - `taskResultSchema` output is now a loose object with a typed optional
    `targetManifest` instead of `Record<string, unknown>`.

### Patch Changes

- Updated dependencies [8e090b8]
- Updated dependencies [1163dae]
- Updated dependencies [ff9906f]
  - @dungle-scrubs/tether-protocol@0.4.0

## 0.2.0

### Minor Changes

- 0c6025d: Replace provider-specific scheduled-work identity with bounded versioned opaque scope keys, add generic completed-result target manifests, and return canonical approval records for first-committer-wins decisions.

  Add provider-neutral browser pairing, scoped operator authority, manifest-bound operator approval, command, and one-time WebSocket admission contracts.

  Add a browser-only operator client for pairing, bootstrap, snapshots, awaited replay, bounded reconnect, commands, and canonical approvals.

### Patch Changes

- b7764ae: Build the browser package before typechecking its compiled-entry test so clean release checkouts pass the commit gate.
- Updated dependencies [0c6025d]
  - @dungle-scrubs/tether-protocol@0.3.0
