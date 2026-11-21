---
"@dungle-scrubs/tether-client": minor
---

A task conflict carries its reason, and tasks can be cancelled:

- A `CLAIM_CONFLICT` from `RestParticipantTaskClient` now carries
  `conflictReason`: `CONTROL_CONFLICT` or `CONTROL_EPOCH_STALE` when the server
  refused the caller's control lease, a task-identity reason when it refused a
  create, and `null` when the 409 body names no known code. The `code` is
  unchanged.
- New `isControlConflict(error)` is true exactly for the control-lease reasons,
  so a caller can tell "take control again" from "the task's state refused
  this".
- New `RestParticipantTaskClient.cancelTask(sessionId, taskId, { controlEpoch,
  reason })` cancels a task that is not yet terminal and resolves with the
  cancelled record.
