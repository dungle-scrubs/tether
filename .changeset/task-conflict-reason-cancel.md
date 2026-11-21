---
"@dungle-scrubs/tether-client": minor
---

A task conflict carries its reason, and tasks can be cancelled:

- A `CLAIM_CONFLICT` from `RestParticipantTaskClient` now carries
  `conflictReason`: `CONTROL_CONFLICT` or `CONTROL_EPOCH_STALE` when the server
  refused the caller's control lease, a task-identity reason when it refused a
  create, and `null` when the 409 body names no known code or cannot be read
  within 4096 bytes and two seconds. The `code` is unchanged, and a 409 still
  settles promptly.
- New `isControlConflict(error)` is true exactly for the control-lease reasons,
  so a caller knows to take control again. A false result does not prove the
  task's state refused the call: treat a `null` reason as unknown.
- New `RestParticipantTaskClient.cancelTask(sessionId, taskId, { controlEpoch,
  reason })` cancels a task that is not yet terminal and resolves with the
  cancelled record.
