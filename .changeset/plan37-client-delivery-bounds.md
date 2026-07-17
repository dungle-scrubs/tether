---
"@dungle-scrubs/tether-client": major
---

Bound participant and observer event delivery by both a 2,000-item count and a
16 MiB retained raw-frame byte high-water mark, pass an `AbortSignal` to event
handlers, and hold replay until a timed-out handler settles. Give the observer
finite delivery-recovery attempts with an inspectable paused state and bound
both error backlogs to 32 entries with a dropped-error count.

Claim-owned task mutations now require the current Claim ID: `completeTask`,
`failTask`, and `refreshTaskClaim` take the server-issued `claimId` from the
claimed record, and the claim flow declines a claim whose deadline is missing
or is not an RFC 3339 timestamp with an offset.
