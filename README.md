# Tether

Tether is a public coordination service for durable sessions, event streams,
participants, tasks, approvals, and external client bindings.

## Why use Tether

Tether holds no intelligence of its own, and it requires none from its
participants. A participant can be a language model, a plain deterministic
script, or any mix of the two - Tether only coordinates them.

Participants are muscles. Tether is the substrate they share:

- **One durable, replayable event stream.** Every agent action and decision
  lands in one append-only log, decoupled from where it happened, so a
  cross-cutting agent can observe the whole without being coupled to any host.
- **A read-only passive-observer mode.** An analytical agent watches the full
  stream while holding no control lease, so it can never interfere with the
  workers it observes.
- **Durable tasks** with atomic claim, lease, and supersession, plus a
  deterministic scheduler - autonomy that survives restarts, never double-acts,
  and de-dupes. Any agent can hand work to another as a task.
- **One human channel with a real approval protocol**, built once and reused by
  every agent.

The leverage is a shared nervous system, a single human interface, reliable
autonomy, and composability where one agent's output is another's input.

## Examples

- **Memory agent.** A passive observer builds a cross-project knowledge graph
  and writes memories back as durable events, nudging when you enter a context
  rather than on a clock.
- **Skill miner.** Detect repeated manual sequences across sessions, gate an
  automation behind approval, and generate a reusable skill.
- **Daily briefing agent.** A scheduled digest of what got done, what is
  blocked, and what is awaiting you.
- **Cost and routing agent.** Watch token spend, latency, and outcomes per
  agent, then tune model routing.
- **Watchdog agent.** Turn stuck-task, stale-lease, and expired-canary state
  into proactive alerts.
- **Universal front door.** A single entry point: free text in, dispatched as a
  scoped task to the right agent.

## Roadmap

- Ship model-backed example participants and clients as working references,
  including **Claude Code**, **Codex**, and **Pi**.
- Ship small deterministic examples that need no model at all:
  - a **heartbeat** participant that emits an event on a timer,
  - a **webhook receiver** that turns inbound requests into tasks,
  - a **file watcher** that opens a task when a path changes,
  - an **approval relay** that forwards pending approvals to a chat and writes
    the response back.
- Publish usage instructions for the packages: a quickstart and integration
  guide for `@dungle-scrubs/tether-client`, `@dungle-scrubs/tether-client-bridge`,
  and `@dungle-scrubs/tether-protocol`, covering connecting a participant,
  claiming tasks, streaming session events, and binding external clients.

## Workspace

- `apps/tether`: HTTP and WebSocket service, database schema, migrations, and
  service tests.
- `packages/protocol`: shared protocol records, validators, and event builders.
- `packages/client`: participant runtime client for REST and WebSocket
  integration.
- `packages/client-bridge`: helpers for binding external client conversations
  to Tether sessions and task APIs.

## Local Setup

Install dependencies:

```sh
pnpm install
```

Copy the public environment template and set local secrets:

```sh
cp .env.example .env
```

Start Postgres and the app with Docker Compose:

```sh
docker compose up -d --build
```

Run the service without Docker:

```sh
pnpm dev
```

The service listens on `127.0.0.1:3025` by default. The operator UI script uses
the registered local UI port:

```sh
pnpm ui
```

## Authentication

Set `AUTH_MODE=required` for local and deployed services. Mint a scoped token
for an operator or integration:

```sh
pnpm --filter tether mint --participant part_cli --session '*' --role admin
```

Use `SERVICE_AUTH_TOKEN` for trusted local scripts. Browser or third-party
clients should receive scoped `observer` or `participant` tokens for the
session they need.

## Validation

Run the public checks before opening a change:

```sh
pnpm run build
pnpm lint
pnpm typecheck
pnpm test
pnpm test:e2e
```

Validate compose changes with disposable required values:

```sh
POSTGRES_PASSWORD=compose-test DATABASE_URL=postgres://tether:compose-test@postgres:5432/tether docker compose config
```

## Public Boundary

This repository owns the service boundary and provider-neutral SDKs. Adapter
runtime code, private provider workflows, local machine runbooks, and
provider-specific command catalogs belong outside this public tree. Public
bridges should integrate through the published SDKs instead of duplicating
WebSocket replay, task claiming, session binding, or protocol record shapes:

```sh
pnpm add @dungle-scrubs/tether-client @dungle-scrubs/tether-client-bridge @dungle-scrubs/tether-protocol
```

- `@dungle-scrubs/tether-protocol`: shared protocol records and validators.
- `@dungle-scrubs/tether-client`: participant runtime client for REST and
  WebSocket integration.
- `@dungle-scrubs/tether-client-bridge`: external client conversation and
  session binding helpers.

The `tether` service app and the workspace root stay private and are not
published.

## Releases

Published packages are versioned with
[Changesets](https://github.com/changesets/changesets). `CHANGELOG.md` files
under `packages/*` are generated by Changesets; do not edit them by hand.

### Day to day

With every change to a published package, add a changeset and commit it with
your code:

```sh
pnpm changeset
```

### Automated release (CI)

The `release` workflow (`.github/workflows/release.yml`) runs on every push to
`main`:

1. When unreleased changesets exist, it opens or updates a **"chore: version
   packages"** pull request that applies the pending bumps and changelogs.
2. Merging that pull request triggers the workflow again; with no changesets
   left, it builds the packages and publishes them to npm.

Publishing uses npm **OIDC trusted publishing** with provenance - no npm token
is stored in the repository.

### One-time bootstrap

OIDC trusted publishing can only be configured for packages that already exist
on npm, so each package needs one manual first publish. Use `pnpm publish` (not
`npm publish`) so the `workspace:*` internal dependency ranges are rewritten to
real versions in the published tarballs. Run it in an interactive terminal so
npm can prompt for two-factor authentication:

```sh
npm login
# protocol first: the client packages depend on it
pnpm --filter @dungle-scrubs/tether-protocol publish --access public
pnpm --filter @dungle-scrubs/tether-client-bridge publish --access public
pnpm --filter @dungle-scrubs/tether-client publish --access public
```

Add `--no-git-checks` if you publish before committing (pnpm otherwise refuses
on a dirty working tree). These first publishes run from your machine and carry
no provenance; provenance is added automatically once releases run through the
CI workflow.

Then, for each package on npmjs.com, open **Settings > Trusted Publisher >
GitHub Actions** and set:

- Organization/user: `dungle-scrubs`
- Repository: `tether`
- Workflow filename: `release.yml`

After that, all future releases flow through the version pull request described
above.

### Manual release (fallback)

The same steps can be run locally without CI:

```sh
pnpm version-packages   # apply changesets: bump versions, write changelogs
pnpm release            # build packages, then changeset publish
```
