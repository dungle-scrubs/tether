---
"@dungle-scrubs/tether-protocol": minor
---

Add a server-issued `claimId` to `TaskRecord` and require non-null
`claimExpiresAt` values to be RFC 3339 timestamps with an offset. Claim-owned
task mutations are fenced by the current Claim ID generation.
