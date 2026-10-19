# Tether

Agents and applications stay informed without manual updates.

Share a durable conversation across coding agents, scripts, and applications instead of relaying updates through one interactive session. Each participant follows the same event stream and catches up on what it missed after a disconnect or restart. Your participants run independently. Tether keeps their shared conversation, tasks, and approvals in PostgreSQL.

```sh
export TETHER_URL=http://127.0.0.1:3025
curl -fsS "$TETHER_URL/sessions" -H 'content-type: application/json' \
  -d '{"sessionId":"demo"}' >/dev/null
ACQUISITION_ID="readme-watcher-$(date +%s)-$$"
CONTROL_EPOCH=$(curl -fsS "$TETHER_URL/sessions/demo/participants" \
  -H 'content-type: application/json' \
  -d "{\"acquisitionId\":\"$ACQUISITION_ID\",\"instanceId\":\"watcher-1\",\"participantId\":\"watcher\",\"runtimeKind\":\"generic_agent\"}" | jq -r '.controlEpoch')
curl -fsS "$TETHER_URL/sessions/demo/events" -H 'content-type: application/json' \
  -d "{\"producerId\":\"watcher\",\"instanceId\":\"watcher-1\",\"controlEpoch\":$CONTROL_EPOCH,\"type\":\"file.changed\",\"payload\":{\"path\":\"README.md\"}}" >/dev/null
for consumer in agent dashboard; do
  printf '%s: ' "$consumer"
  curl -fsS "$TETHER_URL/sessions/demo/events?after=0" \
    | jq -c '[.events[] | select(.type == "file.changed")][-1].payload'
done
# agent: {"path":"README.md"}
# dashboard: {"path":"README.md"}
```

## Install

Run the example against the local demo below. You need Git, Docker with Compose, `curl`, and `jq`. The container uses Node.js 22 and PostgreSQL 17.

This demo disables authentication. Keep both published ports on `127.0.0.1`, and use it only for local experiments. For an authenticated service, see [authentication and deployment](docs/reference.md#authentication-and-deployment).

```sh
git clone https://github.com/dungle-scrubs/tether.git
cd tether
cp .env.example .env
```

Edit `.env` before starting:

- Set `AUTH_MODE=disabled` for this local demo.
- Set `POSTGRES_PASSWORD` to a local password.
- Set `DATABASE_URL` to `postgres://tether:<password>@postgres:5432/tether`, using that password, URL-encoded. The hostname `postgres` is inside the Compose network.

Leave `APP_HOST_BIND` and `POSTGRES_HOST_BIND` at their loopback defaults, and `RUNTIME_TOPOLOGY=single`.

```sh
docker compose up -d --build
curl -fsS http://127.0.0.1:3025/ready | jq '.ready'
# true
```

The service and its dashboard are at `http://127.0.0.1:3025/ui`. PostgreSQL is published on `127.0.0.1:54329`. You can now run the top example.

## Usage

The shell examples use the local demo with authentication disabled and `TETHER_URL` from the top example. Authenticated requests also need an `Authorization: Bearer ...` header.

### Create a shared conversation

```sh
curl -fsS "$TETHER_URL/sessions" -H 'content-type: application/json' \
  -d '{"sessionId":"demo"}' | jq -r '.session.sessionId'
# demo
```

Creating the same session again returns the existing conversation. Give unrelated conversations different session IDs.

### Catch up after a disconnect

```sh
AFTER=0
while :; do
  PAGE=$(curl -fsS "$TETHER_URL/sessions/demo/events?after=$AFTER")
  printf '%s\n' "$PAGE" | jq -c '.events[] | {seq, type, payload}'
  AFTER=$(printf '%s\n' "$PAGE" | jq -r '.pagination.nextAfterSeq')
  [ "$(printf '%s\n' "$PAGE" | jq -r '.pagination.hasMore')" = false ] && break
done
```

Persist the last sequence you have processed, and use it instead of `0` on the next run. Follow `pagination.nextAfterSeq` until `hasMore` is false, because event pages have count and byte limits.

### Read history with an observer

After [building the SDKs](#contributing), run this from the repository root:

```sh
pnpm --filter tether exec node --input-type=module -e '
const { SessionEventStreamClient } = await import("@dungle-scrubs/tether-client");
const stream = await SessionEventStreamClient.connect({
  serviceUrl: process.env.TETHER_URL,
  sessionId: "demo",
  afterSeq: 0,
});
try {
  stream.onEvent(event => {
    if (event.type === "file.changed") console.log(event.payload.path);
  });
  await stream.waitForReplayComplete();
} finally {
  stream.close();
  await stream.waitForClose();
}
'
# README.md
```

This observer reads history and exits. To keep following live events, keep the connection open instead of closing it after replay. Passive observers hold no participant control lease.

Participants that claim tasks use `runParticipantRuntime`. The [client guide](packages/client/README.md) covers task execution, cancellation, reconnects, and durable cursors.

### Concepts

A session is a durable conversation, with events ordered by a per-session sequence number. Readers can process the same events independently.

Publishing participants hold finite control leases. Keep an acquisition ID stable across retries of one registration, and use a new ID for the next acquisition. Use the returned `controlEpoch`, renew according to `renewAfterMs`, and release control on shutdown. Do not reuse an expired acquisition or replay an uncertain mutation blindly. The [REST control guide](apps/tether/docs/rest-control-migration.md) covers acquisition, heartbeat, and release.

Delivery is at least once, not exactly once. Make external side effects idempotent using a stable key such as an event ID or task ID.

## Reference

### API and packages

The main HTTP operations are:

```text
POST /sessions                         create or return a conversation
GET  /sessions/{id}/events?after={seq}   read event history
POST /sessions/{id}/participants        acquire participant control
POST /sessions/{id}/events              publish with the current control epoch
GET  /sessions/{id}/stream              WebSocket replay and live events
POST /sessions/{id}/tasks               create work for another participant
```

| Package | Use |
| --- | --- |
| `@dungle-scrubs/tether-protocol` | Shared records, validators, and event builders |
| `@dungle-scrubs/tether-client` | Participant runtimes and passive observers |
| `@dungle-scrubs/tether-client-bridge` | External conversation bindings and task clients |
| `@dungle-scrubs/tether-browser` | Browser-safe operator client |

The SDKs publish to GitHub Packages, which requires authentication even for public packages. See the [full reference](docs/reference.md) for registry setup, route details, configuration defaults, CLI commands, and releases.

### Configuration and security

Native processes read their environment. Compose forwards the settings declared in `docker-compose.yml`; adding an arbitrary key to `.env` does not automatically forward it.

```text
AUTH_MODE=required
AUTH_ISSUER=your-service-issuer
AUTH_SIGNING_SECRET=<secret supplied by your secret manager>
DATABASE_URL=<PostgreSQL connection string>
RUNTIME_TOPOLOGY=single
```

`AUTH_MODE` defaults to `required`. `RUNTIME_TOPOLOGY` is mandatory natively, accepts `single` or `multi`, and defaults to `single` in Compose. The native port defaults to 3025, but the native server binds all interfaces. Compose publishes to loopback by default.

Authenticated callers use scoped grants. The legacy `mint` CLI produces tokens rejected by default. Keep bearer grants and signing keys out of logs and browser code. Follow [SECURITY.md](SECURITY.md) for vulnerability reports.

The administrative CLIs exit with `0` on success and `1` on failure. For HTTP errors and the gated grant-issuance flow, see the [full reference](docs/reference.md).

Multiple replicas share PostgreSQL for durable state, but Host Presence remains replica-local. See the [multi-replica runtime contract](apps/tether/docs/multi-replica-runtime.md) before deploying more than one replica.

## Contributing

Use Node.js 22 and the pinned package manager, `pnpm@11.1.3`:

```sh
corepack enable
pnpm install
pnpm run build
pnpm lint
pnpm typecheck
pnpm test
pnpm test:e2e
```

The end-to-end suite starts its own Docker Compose project. For native development, build the workspace first, export the required environment settings, and run `pnpm dev`. Use `127.0.0.1:54329` in the database URL when connecting to Compose's PostgreSQL from the host. The native server binds all interfaces, so retain authentication and restrict network access. `pnpm ui` runs the same service on port 17421.

This repository owns the service and provider-neutral SDKs. Provider-specific adapters, private workflows, and machine runbooks belong outside this tree. See [CONTRIBUTING.md](CONTRIBUTING.md) for conventions and [release details](docs/reference.md#releases) for Changesets. Do not edit generated package changelogs.

## Status

Tether supplies coordination, not intelligence or a runtime that executes your participants.

Planned examples include Claude Code, Codex, and Pi integrations, plus deterministic heartbeat, webhook, file-watcher, and approval-relay participants. Memory agents, briefing digests, cost watchers, and watchdogs are possible consumers, not bundled applications. Package integration guides remain incomplete.

## License

[MIT](LICENSE)
