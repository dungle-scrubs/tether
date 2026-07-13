# Tether Service

`apps/tether` is the public service package. It owns durable sessions, events,
participants, task lifecycle state, approvals, external client bindings, auth,
and the HTTP and WebSocket boundaries.

## Run

From the repository root:

```sh
pnpm install
cp .env.example .env
docker compose up -d postgres
pnpm --filter tether dev
```

Build and start the compiled service:

```sh
pnpm --filter tether build
pnpm --filter tether start
```

## Auth

Use `AUTH_MODE=required` outside narrow local experiments. Mint a scoped token:

```sh
pnpm --filter tether mint --participant part_cli --session '*' --role admin
```

Participants and external clients should use scoped tokens for only the session
and role they need.

## Public APIs

- REST session APIs create, list, archive, delete, and inspect durable
  sessions.
- REST task APIs create, claim, refresh, complete, fail, cancel, release, and
  inspect tasks.
- WebSocket streams replay session events, tail live events, and register
  participant control leases when a participant identity is supplied.
- Host-presence streams use `runtimeKind=host` and `runtimeKind=viewer` for
  passive, process-local presence without durable participant rows.
- Client binding APIs let provider-neutral bridges bind external conversations
  to durable Tether sessions.

The canonical protocol shapes live in `@dungle-scrubs/tether-protocol`. Runtime integrations
should use `@dungle-scrubs/tether-client`; external client bridges should use
`@dungle-scrubs/tether-client-bridge`.

## Validation

```sh
pnpm --filter tether build
pnpm --filter tether lint
pnpm --filter tether typecheck
pnpm --filter tether test
pnpm --filter tether test:e2e
```

## Public Boundary

This package should stay provider-neutral. Adapter runtimes, private command
catalogs, provider credentials, private runbooks, and local generated artifacts
belong outside the public service tree.
