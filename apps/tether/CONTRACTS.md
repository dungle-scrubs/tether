# Tether HTTP and WebSocket Contracts

## Session REST Compatibility

`GET /sessions` returns the native Tether operator inventory fields plus
Host-presence summary fields: `title`, `cwd`, `workspace`, `project`,
`branch`, `git`, `updatedAt`, `host`, `activity`, `archived`, `deleted`,
`forkedFrom`, and `tangentOf`. Missing Host-presence-specific source data is returned
as `null`, `false`, or a stable default instead of omitting the field.

`POST /sessions` is a public idempotent ensure operation. It creates the
session row and sequence row when missing, but does not append
`session.created`. Internal Tether flows that need durable lifecycle events keep
using explicit service methods.

`POST /sessions/{id}/delete` returns Host-presence's permanent-delete shape:
`{ ok: true, sessionId }` on success, or `{ ok: false, reason, detail }` for
typed precondition failures. Only archived sessions without live Host-presence host
presence and without active Host-presence activity are eligible.

Browser CORS is exact-origin allowlisted through `BROWSER_ALLOWED_ORIGINS`.
Wildcard credentialed access is not supported.

Coverage: `apps/tether/test/e2e.test.ts` includes REST inventory, public ensure,
CORS, and permanent delete contract tests.

## WebSocket Streams

Native Tether streams remain participant-control streams. Non-Host-presence runtime
kinds still register durable participants, claim and refresh WebSocket control
leases, release leases on close, dispatch supported task commands through
`websocket-command-spec.ts`, emit correlated `command.result` and `error`
envelopes, preserve heartbeat pings, and close control conflicts with code
`1008`.

Host-presence streams are selected by exact query values:
`runtimeKind=host` for host sockets and `runtimeKind=viewer` for viewer sockets.
All other runtime kinds remain participant-control streams. Passive streams
replay and tail events, emit `replay.complete`, and emit Host-presence `presence`
frames from process-local host state. They do not register durable participants,
claim control leases, or append `participant.*` events.

Coverage: `apps/tether/test/e2e.test.ts` includes encoded session id replay,
passive Host-presence presence, native participant-control diagnostics, command-result
correlation, heartbeat, close-code, and task lifecycle contract tests.
