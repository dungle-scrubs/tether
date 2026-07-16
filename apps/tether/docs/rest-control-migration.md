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
