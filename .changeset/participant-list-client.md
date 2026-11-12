---
"@dungle-scrubs/tether-client": minor
"@dungle-scrubs/tether-protocol": minor
---

Add `listParticipants` to the REST client and `participantListResponseSchema` to
the protocol. `listParticipants(sessionId)` reads `GET /sessions/{id}/participants`
and validates each participant record, so a read-only panel can show presence
without hand-writing the request.
