# Multi-Replica Runtime Contract

Tether supports multiple service replicas sharing one PostgreSQL database when
each process starts with `RUNTIME_TOPOLOGY=multi`. PostgreSQL is the durable
authority for sessions, events, tasks, Control Epoch fences, and mutation
ordering. WebSocket connections and Host Presence remain process-local.

## Support matrix

| Capability | Single replica | Multiple replicas |
| --- | --- | --- |
| Durable session and event state | Supported | Supported through shared PostgreSQL |
| Event delivery to sockets on every replica | Supported | Supported through LISTEN plus durable cursor repair |
| Lost or coalesced notification repair | Supported | Supported through fair bounded polling |
| Per-session event ordering | Supported | Supported from durable sequence cursors |
| Control Epoch mutation fencing | Supported | Supported and PostgreSQL-authoritative |
| Host Presence inventory | Replica Scope | Replica Scope only, not cluster-complete |
| Permanent session delete | Local presence precondition | Rejected with `presence_scope_insufficient` |
| WebSocket admission and message limits | Per socket and process-local | Per socket and process-local |

The inbound WebSocket size, rate, replay, and backpressure limits protect one
socket and one process. They are not identity, IP, or cluster-wide abuse
control.

## Presence protocol

Every Host Presence HTTP inventory and WebSocket presence frame contains
`scope: "replica"` and an opaque `replicaId`. The identifier is stable for the
app lifetime and reuses the session service `eventSourceId`. Consumers must not
merge these responses into an implied cluster-complete inventory.

## Health and readiness

`GET /health` is liveness. Unauthenticated `GET /ready` verifies PostgreSQL
reachability and local subscriber catch-up health. It returns only the bounded
failure reasons `database_unavailable` and `fanout_catchup_stale`, and recovers
automatically when the failing condition clears.

Polling-only operation is supported when LISTEN is intentionally disabled. A
replica with local subscribers is not ready if both LISTEN and polling repair
are unavailable. Tune the failure-age boundary with
`EVENT_FANOUT_CATCH_UP_STALE_MS` without removing bounded per-session rounds.
