# Tether reference

## HTTP operations

This is a synopsis of the session, event, task, and grant APIs. REST request schemas are in [rest-schemas.ts](../packages/protocol/src/rest-schemas.ts). Route declarations are collected in [http-route-inventory.ts](../apps/tether/src/http-route-inventory.ts), with separate [browser operator routes](../apps/tether/src/http-browser-operator-route-handlers.ts).

```text
GET  /health                                           liveness
GET  /ready                                            readiness, unauthenticated
GET  /sessions                                         session inventory
POST /sessions                                         create or return a session
GET  /sessions/{id}/context                             context view
POST /sessions/{id}/delete                              permanent delete, single replica only
GET  /sessions/{id}/events?after={seq}&limit={count}      event page
POST /sessions/{id}/events                              publish an event
GET  /sessions/{id}/stream                              WebSocket upgrade
GET  /sessions/{id}/participants                        participant inventory
POST /sessions/{id}/participants                        acquire REST control
POST /sessions/{id}/participants/{p}/heartbeat          renew control
POST /sessions/{id}/participants/{p}/control/release    release control
GET  /sessions/{id}/tasks                               task inventory
POST /sessions/{id}/tasks                               create a task
GET  /sessions/{id}/tasks/{t}                            read a task
POST /sessions/{id}/tasks/{t}/claim                     claim a task
POST /sessions/{id}/tasks/{t}/claim/refresh             renew a task claim
POST /sessions/{id}/tasks/{t}/complete                  complete a task
POST /sessions/{id}/tasks/{t}/fail                      fail a task
POST /sessions/{id}/tasks/{t}/cancel                    cancel a task
POST /sessions/{id}/tasks/{t}/release                   release a task claim
POST /sessions/{id}/tasks/{t}/approval                  record an approval
GET  /sessions/{id}/task-contracts                      task contracts
GET  /sessions/{id}/task-contracts/{kind}               contracts for one kind
POST /sessions/{id}/scheduled-runs/supersede            supersede scheduled work
POST /auth/grants                                      create a grant, rollout-gated
GET  /auth/grants                                      list grants
GET  /auth/grants/{id}                                 inspect a grant
POST /auth/grants/{id}/revoke                          revoke a grant
```

Event pages contain `events` and `pagination`. Pagination fields are `afterSeq`, `hasMore`, `limit`, `nextAfterSeq`, and `returned`. The request parameter is `after`, not `afterSeq`.

Participant-owned mutations require the current `controlEpoch`. Registration is the acquisition operation. The [REST control guide](../apps/tether/docs/rest-control-migration.md) describes acquisition IDs, renewal, release, migration compatibility, and uncertain outcomes.

Common HTTP errors include `400` for invalid bodies, `401` for failed authentication, `403` for denied authority, and `409` for control or state conflicts. Resource limits can return `413` or `429`. Grant creation returns `503` while issuance is gated. Inspect the response's bounded `reason` or `error` before deciding whether to retry.

## Configuration

The native server reads process environment variables. It does not load the repository's `.env` file itself. Compose uses `.env` for interpolation, then forwards only the variables declared under each service's `environment` block.

The following defaults are native defaults, from [config.ts](../apps/tether/src/config.ts), [resource-limits.ts](../apps/tether/src/resource-limits.ts), [session-event-fanout.ts](../apps/tether/src/session-event-fanout.ts), and [task-claim-sweeper.ts](../apps/tether/src/task-claim-sweeper.ts). Durations are milliseconds and byte limits are bytes.

| Key | Default | Meaning |
| --- | --- | --- |
| `AUTH_MODE` | `required` | `required` or `disabled` |
| `AUTH_ISSUER` | none | Required grant issuer when auth is required |
| `AUTH_SIGNING_SECRET` | none | Required signing secret when auth is required |
| `AUTH_SIGNING_KID` | `default` | Active signing key ID |
| `AUTH_ACCEPTED_SIGNING_SECRETS` | `{}` | Additional verification keys as a JSON object |
| `AUTH_ALLOW_LEGACY_TOKENS` | `false` | Temporary acceptance of revocation-blind legacy tokens |
| `DATABASE_URL` | none | Required PostgreSQL connection string |
| `DATABASE_POOL_MAX` | `10` | Maximum pool connections |
| `RUNTIME_TOPOLOGY` | none | Required `single` or `multi` |
| `PORT` | `3025` | HTTP and WebSocket listen port |
| `CONTROL_EPOCH_ENFORCEMENT` | `true` | Fence REST participant mutations |
| `EVENT_FANOUT_CATCH_UP_MS` | `1000` | Polling repair interval, `0` disables polling |
| `EVENT_FANOUT_CATCH_UP_STALE_MS` | `30000` | Subscriber catch-up readiness deadline |
| `EVENT_FANOUT_BATCH_LIMIT` | `500` | Events per fan-out batch |
| `EVENT_LIST_DEFAULT_LIMIT` | `500` | Default event page count |
| `EVENT_LIST_MAX_LIMIT` | `1000` | Maximum event page count |
| `REST_EVENT_LIST_MAX_BYTES` | `16777216` | Event page byte budget |
| `HTTP_MAX_BODY_BYTES` | `2097152` | HTTP body limit |
| `TASK_CLAIM_SWEEP_MS` | `1000` | Expired-claim sweep interval, `0` disables it |
| `TASK_CLAIM_SWEEP_BATCH_SIZE` | `50` | Claims per sweep |
| `WS_MAX_PAYLOAD_BYTES` | `2097152` | Inbound WebSocket message limit |
| `WS_MESSAGE_RATE_LIMIT` | `60` | Inbound messages per rate window |
| `WS_MESSAGE_RATE_WINDOW_MS` | `10000` | Rate window duration |
| `WS_REPLAY_MAX_EVENTS` | `2000` | Replay event budget |
| `WS_REPLAY_MAX_BYTES` | `67108864` | Replay byte budget |
| `WS_GAP_REPAIR_GRACE_MS` | `10000` | Stream gap repair grace period |
| `WS_BACKPRESSURE_BUFFERED_BYTES` | `4194304` | Per-socket buffered byte limit |

Compose defaults `RUNTIME_TOPOLOGY` to `single` and `WS_BACKPRESSURE_BUFFERED_BYTES` to `67108864`. It fixes the container's `PORT` at `3025`. Its host bindings default to `127.0.0.1:3025` for the app and `127.0.0.1:54329` for PostgreSQL. `APP_HOST_BIND`, `APP_HOST_PORT`, `POSTGRES_HOST_BIND`, and `POSTGRES_HOST_PORT` control these published endpoints. The native server binds all interfaces.

These settings illustrate the required native configuration. Supply actual secrets through your secret manager, not checked-in files:

```text
AUTH_MODE=required
AUTH_ISSUER=your-service-issuer
AUTH_SIGNING_SECRET=<signing secret>
DATABASE_URL=<PostgreSQL connection string>
RUNTIME_TOPOLOGY=single
```

`EVENT_LIST_MAX_LIMIT` is raised to the default page limit if configured below it. WebSocket limits apply to one socket and process, not to an identity or the entire cluster. See the [multi-replica contract](../apps/tether/docs/multi-replica-runtime.md) for readiness and Host Presence limits.

## Authentication and deployment

The README's unauthenticated demo is not a deployment configuration. Use `AUTH_MODE=required`, set `AUTH_ISSUER` and `AUTH_SIGNING_SECRET`, and protect transport with TLS when traffic leaves the machine. Keep database access restricted. Do not put admin grants or signing secrets in browser code.

Durable bearer grants use roles `admin`, `participant`, and `observer`, with a session ID or `*` as their session scope. Grant lifecycle endpoints require a service-scoped admin. Task authority also supports narrower grants. Their types and checks are in [grant-stores.ts](../apps/tether/src/auth/grant-stores.ts) and [task-grants-policy.ts](../apps/tether/src/auth/task-grants-policy.ts).

The host-local `bootstrap-admin` CLI creates a recovery admin grant directly against the database. Before issuing one, confirm that every deployed replica supports `tgr2` grants. The CLI requires the explicit `AUTH_GRANT_BOOTSTRAP_COMPATIBILITY_CONFIRMED=true` gate. With the authenticated Compose app already running:

```sh
docker compose exec -e AUTH_GRANT_BOOTSTRAP_COMPATIBILITY_CONFIRMED=true app \
  pnpm --filter tether bootstrap-admin --subject operator --ttl 1d
```

The output contains a bearer credential exactly once. Store it securely, not in logs. The command uses the container's database, issuer, and signing settings. Its default lifetime is one day. CLI failures exit `1`; success exits `0`.

HTTP grant creation is currently rollout-gated: the server's default wiring leaves issuance disabled, so `POST /auth/grants` returns `503`. The bootstrap CLI has its own explicit compatibility gate. Do not assume that bootstrapping also enables HTTP issuance.

The `mint` CLI generates legacy stateless tokens. Required auth rejects them unless `AUTH_ALLOW_LEGACY_TOKENS=true` explicitly enables temporary compatibility. Legacy tokens have no durable revocation. Do not use this flag as a substitute for grant migration.

## CLI synopsis

Run package scripts from the repository root:

```text
pnpm dev                                  watch the native service
pnpm start                                run compiled service
pnpm ui                                   watch the service on port 17421
pnpm --filter tether ensure-session --identity <key> --session <id>
pnpm --filter tether bootstrap-admin --subject <subject> [--ttl <duration>]
pnpm --filter tether pair-browser create|inspect|confirm <flags>
pnpm --filter tether mint --participant <id> --session <id> --role <role> [--ttl <duration>] [--kid <id>]
```

`ensure-session` requires `DATABASE_URL` and maintains a durable deployment identity binding. `bootstrap-admin` also requires issuer, signing settings, and the compatibility gate above. `pair-browser` creates, inspects, or confirms operator pairing requests against the database. See its [argument parser](../apps/tether/src/auth/browser-pairing-cli.ts) for each operation's required flags.

These administrative CLIs exit `0` on success and `1` on failure. Their definitions are in [apps/tether/package.json](../apps/tether/package.json).

## Install the published SDKs

The four SDKs publish to `https://npm.pkg.github.com`. GitHub requires authentication for public npm packages too. Follow the [official GitHub Packages instructions](https://docs.github.com/en/packages/working-with-a-github-packages-registry/working-with-the-npm-registry) to create a personal access token **classic** with `read:packages` and access to the package.

Configure the scope and a token supplied through your environment in a project or user `.npmrc`:

```text
@dungle-scrubs:registry=https://npm.pkg.github.com
//npm.pkg.github.com/:_authToken=${GITHUB_TOKEN}
```

Never commit a literal token. In your consuming project, install the packages you need:

```sh
pnpm add @dungle-scrubs/tether-client @dungle-scrubs/tether-protocol
```

The other packages are `@dungle-scrubs/tether-client-bridge` and `@dungle-scrubs/tether-browser`. Runtime examples and delivery guarantees are in the [client guide](../packages/client/README.md).

## Releases

Published packages use [Changesets](https://github.com/changesets/changesets). A change to a published package includes a changeset:

```sh
pnpm changeset
```

The [release workflow](../.github/workflows/release.yml) runs on pushes to `main`. Pending changesets produce a `chore: version packages` pull request. After that pull request merges, a subsequent run builds and publishes the packages to GitHub Packages using the workflow's `GITHUB_TOKEN`.

The root scripts are `pnpm version-packages` for version updates and `pnpm release` for package builds and publishing. These change versions or publish packages, so they are not validation commands. The current workflow does not publish to npmjs.org or use npm trusted publishing.

Package `CHANGELOG.md` files are generated by Changesets. Do not edit them by hand. The service app, session-summary worker, and workspace root remain private.
