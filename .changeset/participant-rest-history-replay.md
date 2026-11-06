---
"@dungle-scrubs/tether-client": minor
---

Add optional REST history replay for participant runtimes that exceed the WebSocket replay window. Discover a finite witnessed head, adapt history pages to byte limits, and keep cursors behind successful handlers. Bound streamed reads and retained task state, execute recovered tasks serially through the existing claim flow, and retry transient failures through the existing reconnect backoff.

`RestParticipantTaskError.code` includes `RESOURCE_LIMIT`; that failure variant requires `details.resourceLimit` containing `{ resource, limit, observed }`. Errors retain enumerable own `code` and `details` fields for JSON serialization and object spread. Recovery read policy remains internal rather than an exported client option.

In default WebSocket mode, `runClaimableTasks({ once: true })` now rejects if terminal pause wins before task settlement completes. REST once mode rejects under the same condition.
