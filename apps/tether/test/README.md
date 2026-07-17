# PostgreSQL Concurrency Tests

The Tether E2E suite contains both invariant tests and mechanism tests. Keep the
distinction explicit when adding or reviewing concurrency coverage.

## Invariant Tests

An invariant test drives a public HTTP, service, or persistence interface
against PostgreSQL and asserts the durable or public behavior that Tether must
preserve. The coordinator may force a specific database interleaving, but the
assertion must remain about observable behavior rather than SQL order alone.

The headline invariant scenarios are:

- Control Epoch serialization:
  - `commits an epoch-N REST claim refresh before replacement installs epoch N+1`
  - `rejects an epoch-N REST claim refresh after replacement installs epoch N+1`
- Single-winner task claiming:
  - `allows exactly one of two synchronized REST claimants to claim one task`
- Event sequence commit order:
  - `keeps concurrent event sequence visibility ordered through allocator commit`
  - `reuses a rolled-back event allocation without leaving a durable gap`

These tests use independent PostgreSQL clients. When one actor must wait, they
also verify the wait through PostgreSQL lock state before releasing the actor
that holds the relevant row lock.

## Mechanism Tests

A mechanism test proves that test support or a lower-level database mechanism
works as expected. It is useful supporting evidence, but it does not by itself
prove a product invariant.

Examples include the coordinator tests that verify exact before/after query
phases, independent actor clients, local database timeouts, backend
cancellation, rollback, client release, bounded diagnostics, and redaction.
The tests named `reports bounded redacted diagnostics and cleans up after a
barrier timeout` and `reports cleanup failures after attempting every actor
release` are mechanism tests.

Do not replace the headline invariant assertions with mechanism-only
assertions such as query traces, mock call counts, or the presence of a SQL
predicate.

## Negative-Control Evidence

Negative controls are run locally and restored before commit. Never commit a
broken production guard or a test-only production switch. For each control,
record the temporary mutation, the intended failing assertion, and proof that
the changed production file was restored byte-for-byte.

| Invariant | Temporary local mutation | Intended failure |
| --- | --- | --- |
| Control Epoch serialization | Remove only the current-epoch equality predicate from the protected mutation fence. | The supersession-first epoch-N mutation writes after epoch N+1 is current instead of returning the stale-control rejection. |
| Single-winner task claiming | Remove only the `claimed_at IS NULL` and `claimed_by IS NULL` claim guards. | Both synchronized REST claimants return success instead of one HTTP 200 and one HTTP 409. |
| Event sequence commit order | Commit the outer append transaction immediately before the event insert. | Publisher B is not blocked on the allocator row, so lock-wait proof fails. |
| Rolled-back event allocation | Commit instead of rolling back after the injected publisher-A insert failure. | Publisher B receives N+1 instead of reusing N, exposing a durable gap. |

After every control, restore the production file and confirm both its checksum
and `git diff --exit-code -- <file>` match the pre-control state. The committed
suite contains only the passing variants.

## CI Contract

The repository `test:e2e` script runs `pnpm --filter tether test:e2e`, which
starts a real PostgreSQL service and executes `test/e2e.test.ts`. GitHub Actions
invokes that repository script directly. The invariant tests therefore run in
CI without production test hooks or production-only feature switches.

All coordinator interception and fault injection stays in `test/` and wraps
test-owned database pools. Production modules do not read a concurrency-test
environment variable or expose a concurrency-test branch.

Coordinator timeouts and cleanup failures must remain bounded and actionable.
Diagnostics may include actor, phase, query class, backend PID, bounded lock
state, and cleanup operation. They must not include SQL values, event payloads,
credentials, or unbounded database text.
