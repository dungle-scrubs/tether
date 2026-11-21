---
"@dungle-scrubs/tether-client": minor
---

Close four gaps in the REST participant clients:

- `RestParticipantControlClient.invalidate` now clears the current context when
  the snapshot names the same generation (acquisition id and control epoch),
  including a snapshot taken before a renewal. Previously it returned `true`
  and left the renewed context in place.
- New `requestTimeoutMs` option (default 10,000 ms, clamped to 100..60,000 ms)
  bounds each acquire, renew and release request, including the response body.
  A timeout fails as `TRANSPORT`, so acquisitions retry, renewals recover and
  releases count as uncertain, as for any other transport failure.
- New `onRenewalFailure` and `onControlLost` callbacks report each failed
  background renewal and each context the client clears other than by
  `release()` or `stop()`. A throwing callback is traced and ignored.
- `RestParticipantTaskClient.createTask` sends an optional `schedule` identity.
