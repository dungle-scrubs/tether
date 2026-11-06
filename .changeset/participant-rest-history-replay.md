---
"@dungle-scrubs/tether-client": minor
---

Add optional REST history replay for participant runtimes that exceed the WebSocket replay window. Discover a finite witnessed head, adapt history pages to byte limits, and keep cursors behind successful handlers. Bound streamed reads and retained task state, execute recovered tasks serially through the existing claim flow, and retry transient failures through the existing reconnect backoff.

`RestParticipantTaskError.code` includes `RESOURCE_LIMIT`. Its optional `details.resourceLimit` contains `{ resource, limit, observed }`. Recovery read policy remains internal rather than an exported client option.
