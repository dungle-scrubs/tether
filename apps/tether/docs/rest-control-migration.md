# REST Control Epoch migration

REST participant mutations now default to enforced Control Epoch fencing. Set
`CONTROL_EPOCH_ENFORCEMENT=false` only as a temporary migration-release
compatibility override. Compatibility keeps `/health` ready and accepts a
missing epoch without creating or renewing lease history, but a supplied stale
epoch is still rejected.

Compatibility remains available for one full published migration-release
interval. It is removed in the immediately following breaking release.

## Deployment order

1. Apply migration `0013_misty_leo.sql`.
2. Deploy only server replicas that support idempotent Acquisition IDs and
   no-claim compatibility.
3. Upgrade direct REST consumers and `@dungle-scrubs/tether-client-bridge`.
4. Drain every older server binary.
5. Remove the explicit false override so absence selects enforced mode.

Configuration rollback to explicit false is supported during the migration
release. Binary rollback to a server that claims a Lease Generation for an
epoch-less mutation is not supported after the default flip.

## Protected routes

Participant registration is the acquisition route:

- `POST /sessions/{sessionId}/participants`

These routes require the exact current Control Epoch:

- `POST /sessions/{sessionId}/events`
- `POST /sessions/{sessionId}/participants/{participantId}/heartbeat`
- `POST /sessions/{sessionId}/participants/{participantId}/control/release`
- `POST /sessions/{sessionId}/tasks/{taskId}/approval`
- `POST /sessions/{sessionId}/tasks/{taskId}/cancel`
- `POST /sessions/{sessionId}/tasks/{taskId}/claim`
- `POST /sessions/{sessionId}/tasks/{taskId}/claim/refresh`
- `POST /sessions/{sessionId}/tasks/{taskId}/complete`
- `POST /sessions/{sessionId}/tasks/{taskId}/fail`
- `POST /sessions/{sessionId}/tasks/{taskId}/release`

Reads, task creation, scheduled-run supersession, session deletion, and
client-binding mutations do not participate in participant control.

## Direct REST lifecycle

Acquire once with an Acquisition ID that remains stable across transport
retries of this request:

```bash
curl -sS -X POST "$TETHER_URL/sessions/$SESSION_ID/participants" \
  -H "authorization: Bearer $TETHER_AUTH_TOKEN" \
  -H "content-type: application/json" \
  --data '{
    "acquisitionId": "0190-example-logical-acquisition",
    "controlChannel": "rest",
    "instanceId": "worker-process-1",
    "participantId": "worker",
    "runtimeKind": "generic_agent"
  }'
```

Retain `acquisitionId`, `controlEpoch`, `leaseExpiresAt`, and `renewAfterMs`
from the response. Renew using the exact epoch:

```bash
curl -sS -X POST \
  "$TETHER_URL/sessions/$SESSION_ID/participants/worker/heartbeat" \
  -H "authorization: Bearer $TETHER_AUTH_TOKEN" \
  -H "content-type: application/json" \
  --data '{"controlEpoch":1,"instanceId":"worker-process-1"}'
```

Attach the same instance and epoch to each protected mutation. A stale or
uncertain mutation response must be surfaced to the caller and must not be
automatically replayed:

```bash
curl -sS -X POST "$TETHER_URL/sessions/$SESSION_ID/tasks/$TASK_ID/cancel" \
  -H "authorization: Bearer $TETHER_AUTH_TOKEN" \
  -H "content-type: application/json" \
  --data '{
    "controlEpoch": 1,
    "instanceId": "worker-process-1",
    "participantId": "worker",
    "reason": {}
  }'
```

Release the exact generation during orderly shutdown:

```bash
curl -sS -X POST \
  "$TETHER_URL/sessions/$SESSION_ID/participants/worker/control/release" \
  -H "authorization: Bearer $TETHER_AUTH_TOKEN" \
  -H "content-type: application/json" \
  --data '{"controlEpoch":1,"instanceId":"worker-process-1"}'
```

## Client bridge upgrade

Participant identity moves from each cancellation or approval input into the
task client configuration. The bridge composes
`RestParticipantControlClient`, acquires once per session, reuses the context,
and releases active contexts through `shutdown()`.

```typescript
const tasks = new ClientBridgeTaskClient({
  authToken,
  control: {
    instanceId: "worker-process-1",
    participantId: "worker",
    runtimeKind: "generic_agent",
  },
  serviceUrl,
});

await tasks.cancelTask(sessionId, { reason: {}, taskId });
await tasks.recordTaskApproval(sessionId, {
  decision: "approved",
  taskId,
});
await tasks.shutdown();
```

## Diagnostics

Explicit compatibility emits one structured startup warning. `/health` stays
HTTP 200 with `ok: true` and adds `REST_CONTROL_COMPATIBILITY_ENABLED`.
`/debug/server` exposes bounded per-route REST control outcomes. These surfaces
use stable route names and never include tokens, participant identity,
Acquisition IDs, epochs, request payloads, or database details.

### Rejected task contracts

Registration and heartbeat success responses include `rejectedContracts`, also
available on records returned by `GET /sessions/{sessionId}/participants`.
Each rejection contains the capability-array index, validation issue paths and
codes, and `taskKind` when it is a string of at most 200 UTF-8 bytes. No other
payload values or validation messages are included. Invalid advertisements stay
excluded from task-contract discovery; valid advertisements are unchanged.

Diagnostics examine at most 128 contracts, report at most 32 rejections, and
retain at most 8 issues per rejection. `rejectedContractsTruncated` counts known
rejections omitted plus unexamined entries, which may be valid. A rejection's
optional `truncated` counts omitted validation issues. These limits affect only
diagnostics, not discovery or admission. Correct the reported entries and
republish capabilities to reveal further diagnostics.

The REST control client exposes both fields from its latest acquisition or
renewal response. An absent `rejectedContracts` means the server did not report
diagnostics; an empty array with a zero truncation count means none were rejected.
Derived diagnostics are not stored in participant lifecycle events.

The structured `participant.contract_rejected` warning contains session and
participant ids and the same bounded diagnostic data. Each process retains up
to 4,096 participant rejection fingerprints, covering all examined rejections.
An unchanged retained fingerprint does not expire; a change or a return to
invalid capabilities after correction can warn again. LRU eviction and process
restart can permit another warning. A process-wide limit of 4,096 warnings per
minute also bounds overflow cycles; response diagnostics are never suppressed.
