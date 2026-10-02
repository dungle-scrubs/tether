---
"@dungle-scrubs/tether-protocol": minor
"@dungle-scrubs/tether-browser": minor
"@dungle-scrubs/tether-client": patch
"@dungle-scrubs/tether-client-bridge": patch
---

Deduplicate the browser operator session wire shape and share transport helpers.

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
