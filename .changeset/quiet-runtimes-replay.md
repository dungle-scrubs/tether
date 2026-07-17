---
"@dungle-scrubs/tether-client": minor
"@dungle-scrubs/tether-protocol": minor
---

Deliver participant and observer events serially with handled-only cursor advancement, bounded participant recovery, serialized durable cursor retries, subscriber-first runtime hooks, and awaitable graceful shutdown.

Protocol error envelopes now preserve safe machine-readable replay reasons. Participant diagnostics add received, handled, and persisted cursors while retaining `lastObservedSeq` through the 0.2.x line as a deprecated alias of the corrected handled cursor.

Durable cursor stores must persist sequence values with atomic maximum semantics so a timed-out earlier write cannot regress a later acknowledged cursor.
