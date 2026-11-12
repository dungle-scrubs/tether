---
"@dungle-scrubs/tether-client": minor
---

Add `since` and `exclude` to the session event-list query. `listEvents` accepts an
optional `since` ISO 8601 instant with an explicit offset or `Z` and an optional
`exclude` list of event types, and sends them as the `since` and `exclude` query
parameters of `GET /sessions/{id}/events`. The service starts the page at the
first event created at or after `since` and leaves the listed event types out of
the page, so a read-only panel can load a recent window without paging through
every heartbeat.
