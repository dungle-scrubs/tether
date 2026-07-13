# Tether Context

Tether is a coordination service. It does not embed participant runtimes or
provider adapters.

## Domain Terms

**Session**:
A durable coordination container. Events, participants, tasks, approvals, and
client bindings are scoped to a session.

**Session Event**:
An append-only record with a monotonic per-session sequence. Events are replayed
over REST and WebSocket boundaries.

**Participant**:
A runtime identity that can publish events, advertise capabilities, claim tasks,
and hold a control lease.

**Control Lease**:
The current REST or WebSocket owner for one participant identity. Tether keeps
lease history so reconnects, conflicts, and releases are inspectable.

**Control Epoch**:
A monotonic fencing generation assigned whenever the same participant instance
supersedes its previous Control Lease.
_Avoid_: reconnect count, task claim generation

**Task**:
A durable unit of work with nullable timestamp state columns for claim,
completion, failure, cancellation, release, and claim-expiration transitions.

**Approval**:
A durable decision for a task or a task target. Approval target identity is
owned by the service; provider-specific prompt deduplication belongs to bridges.

**Client Binding**:
A durable association between an external provider conversation and one Tether
session. Bindings let provider-neutral bridges resume the same session after
restart.

**Source Event Stream**:
The durable, provider-neutral session history that bridges consume; it records
what happened but does not claim that an external provider received it.
_Avoid_: delivery queue, provider outbox

**Host Presence**:
A process-local, passive WebSocket projection for session inventory consumers.
`runtimeKind=host` announces live host metadata. `runtimeKind=viewer` receives
presence frames. Neither mode creates durable participant rows.

**Runtime Process Contract**:
The supervisor-neutral startup, readiness, shutdown, durable-state, and restart
requirements published by Tether or an external adapter.
_Avoid_: launchd job, Docker service, deployment manifest

**Runtime State Inventory**:
The read-only status view that lists exact local durable-state paths and whether
an operator-configured backup is known.
_Avoid_: backup implementation, secret dump

**E2E Run**:
One isolated verification execution whose Docker resources share a unique run
identity, ownership label, and creation timestamp.
_Avoid_: developer stack, persistent service deployment

**Compatibility Target**:
The public Tether commit or package version against which a private adapter
workspace is verified.
_Avoid_: copied public source, implicit neighboring checkout

## Ownership

Tether owns:

- database schema and persistence for sessions, events, participants, leases,
  tasks, approvals, and client bindings
- HTTP and WebSocket protocol boundaries
- provider-neutral protocol validators and event builders
- participant runtime and client bridge SDK behavior
- auth checks, resource limits, debug summaries, and replay semantics

External adapters own:

- provider credentials and API clients
- provider-specific rendering and command catalogs
- provider-specific durable delivery state, retry policy, and dead-letter state
- local machine runbooks and runtime bootstrap
- semantic task handlers and domain-specific task policy

## Boundary Rules

Use `@dungle-scrubs/tether-protocol` for shared records and validators. Use `@dungle-scrubs/tether-client`
for participant runtimes. Use `@dungle-scrubs/tether-client-bridge` for external conversation
binding and task APIs.

Do not duplicate WebSocket replay, task claiming, claim refresh, cancellation,
or durable record shapes in adapters. Do not add provider-specific adapters or
private runtime packages to this public service package.

Private adapters consume public packages through declared dependencies and
record a **Compatibility Target**. Public boundary checks reject private runtime
trees; private verification builds and tests against its declared target rather
than duplicated public source.

A bridge may persist a provider-specific outbox keyed to the **Source Event
Stream**. That outbox tracks external delivery independently from Tether event
replay; successful session replay is not evidence of provider delivery.

A reconnect with the same Participant and runtime instance supersedes its prior
**Control Lease** and advances the **Control Epoch**. Commands from an older
epoch are rejected. A different runtime instance remains a control conflict
until the current lease is released, superseded by policy, or expires.

Tether and external adapters publish a **Runtime Process Contract**, not a
required supervisor. Operators may satisfy that contract with Docker, systemd,
launchd, launchdawg, Kubernetes, a shell, or another process manager.

The **Runtime Process Contract** names and validates ordinary environment
variables. Resolving secrets through 1Password, files, a platform secret store,
or another mechanism happens before process start and is not owned by Tether or
an adapter runtime.

Off-machine backup and restore are deployment concerns and are not configured by
the current product. Local durability survives process restart, not loss of the
host, unless an operator later adds and verifies a backup target.

Every **Runtime Process Contract** exposes a **Runtime State Inventory**. The
current deployment reports `backup: not configured`; it does not infer safety
from the presence of local durable storage.

An **E2E Run** forwards termination signals, performs cleanup exactly once, and
preserves the original exit status. A separate cleanup operation may remove only
expired resources carrying Tether E2E ownership labels.
