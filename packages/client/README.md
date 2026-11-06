# @dungle-scrubs/tether-client

Public adapter API for external Tether participant runtimes.

Use this package from agents that live outside the Tether service. The package
owns WebSocket connection setup, replay, task claiming, claim refresh,
cancellation, task completion, task failure, and reconnect handling.

## Agent Shape

New agents usually provide only three pieces:

- Runtime identity and session configuration for `runParticipantRuntime`.
- A list of task `workKinds` the agent is allowed to claim.
- A `ParticipantTaskExecutor` containing the domain-specific work.

```ts
import { runParticipantRuntime, type ParticipantTaskExecutor } from "@dungle-scrubs/tether-client";

const executor: ParticipantTaskExecutor = async ({ signal, task }) => {
  if (signal.aborted) {
    throw new Error("Task cancelled");
  }
  const summary = `Handled ${task.taskId}`;
  return {
    output: summary,
    result: { summary },
  };
};

await runParticipantRuntime({
  afterSeq: 0,
  capabilities: { workKinds: ["example_work"] },
  claimRefreshMs: 5_000,
  displayName: "Example Agent",
  executor,
  instanceId: "inst_example",
  participantId: "part_example",
  runtimeKind: "example_agent",
  serviceUrl: "http://127.0.0.1:3025",
  sessionId: "sess_example",
  workKinds: ["example_work"],
});
```

When Tether runs with the default `AUTH_MODE=required`, pass `authToken` or set
`SERVICE_AUTH_TOKEN`. The package falls back to `TETHER_AUTH_TOKEN` only for
external consumers that need a namespaced environment variable.

The executor receives a claimed task, the active cancellation `AbortSignal`,
participant/session identity, and helper methods for extra progress or output
events. It returns the structured durable completion payload and optional
user-visible output text.

## Runtime API Selection

Use `runParticipantRuntime` for most external agents. It constructs the client,
installs the error channel, runs pre-open setup, opens the participant stream,
waits for handled replay, runs post-replay setup, processes claimable tasks,
refreshes claims, and awaits hook cleanup plus graceful shutdown.

Use `ParticipantRuntimeClient` directly only when an adapter needs lower-level
control over participant WebSocket commands such as `appendEvent`, `claimTask`,
`refreshTaskClaim`, `completeTask`, or `failTask`.

Use `SessionEventStreamClient` for passive observers that need session replay
and live events without participant identity or task authority. Bridges,
dashboards, and approval observers should use this API when they only need
`onEvent`, `onError`, `waitForReplayComplete`, reconnect behavior, and stream
diagnostics.

## Replay-Safe Startup

Replay-consuming subscriptions must exist before the participant socket opens.
Register them in `onClientReady`. Use `onReplayComplete` only for setup that
must run after every historical event handler has completed. Replayed claimable
tasks remain buffered until the asynchronous `onReplayComplete` hook succeeds.

```ts
await runParticipantRuntime({
  afterSeq: 0,
  capabilities: { workKinds: ["example_work"] },
  claimRefreshMs: 5_000,
  displayName: "Example Agent",
  executor,
  hooks: {
    onClientReady: (client) => {
      const unsubscribe = client.onEvent(async (event) => {
        await observeHistoricalOrLiveEvent(event);
      });
      return unsubscribe;
    },
    onReplayComplete: async (client) => {
      await publishReadyState(client);
      return () => stopReadyStatePublisher();
    },
  },
  instanceId: "inst_example",
  once: false,
  participantId: "part_example",
  runtimeKind: "example_agent",
  serviceUrl: "http://127.0.0.1:3025",
  sessionId: "sess_example",
  workKinds: ["example_work"],
});
```

The old pattern of registering replay observers from `onReplayComplete` misses
historical events. Move those subscriptions to `onClientReady`. Cleanup
callbacks returned by both hooks are awaited in reverse acquisition order.

### Replay long sessions

Set `replayMode: "rest"` in `runParticipantRuntime` or
`ParticipantRuntimeClient.create` configuration when history exceeds the server's
WebSocket replay window. The default, `"websocket"`, keeps the existing behavior.

REST mode discovers a witnessed history head with single-event exponential and
binary probes bounded by the safe-integer sequence domain, then opens the
WebSocket at that head. After the wire replay marker supplies the control epoch,
it delivers history through the serial event handlers. Discovery never advances
the handled or durable cursor. A history read that exceeds available bytes or
item slots, or reaches its deadline, retries with half the rows at the same
cursor. The accepted limit carries forward and can double toward the normal
page limit, within the available slots. At one row, a deadline failure uses
reconnect backoff; resource overflow pauses. Command responses remain available
while a historical handler awaits a command.

Direct clients must register an `onEvent` subscriber before opening. REST replay
waits for actual handler success, including when a subscriber attaches late.
`runParticipantRuntime` installs its task subscriber before opening.

Live event frames and replay markers retain their arrival order behind history.
History pages and held frames share the configured delivery item and byte limits.
`waitForReplayComplete()` resolves only after historical handlers and the queued
wire replay marker complete. The task runner also loads the active-task snapshot
before its post-replay hook and task dispatch. This recovers pending tasks whose
request events precede a persisted cursor. REST task execution is serial, with
one active claim flow. Pending candidates, running work, and snapshot-touched IDs
have a separate task-state budget equal to the delivery count and byte limits.
Responses are bounded while reading, and raw snapshot row counts are checked
before validation. Selected tasks must fit the retained-state budget before
any dispatch. Claims still use the existing server claim, refresh, and
cancellation fences.

REST replay rejects any malformed snapshot row to avoid silently losing pending
work. Direct `RestParticipantTaskClient.listActiveTasks` callers retain their
existing tolerant behavior and receive `invalidRowCount`.

REST requests and the wait for wire control use `commandTimeoutMs`. Ending a
connection generation aborts its REST requests, including stalled bodies.
Transport and body-read failures, and HTTP 408, 429, 500, 502, 503, or 504, recover
through the existing reconnect backoff from the handled cursor. Consecutive
transient failures grow the delay up to `maxDelayMs`, including failures after a
socket opens. Handled progress or completed recovery resets the delay. Error
responses are classified by status and read under a fixed 4096-byte body cap.
Direct `open()` still rejects discovery and opening errors. Other HTTP statuses,
including 501, authentication failures, invalid pages, sequence gaps, malformed
task rows, and resource overflow pause without skipping work.

A gap beyond the WebSocket replay window still fails closed. REST mode does not
provide exactly-once effects or increase any delivery limit.

## Delivery and Side-Effect Contract

Participant and observer handlers run serially in event sequence order. The
handled cursor advances only after every handler in that event's snapshot
completes. A synchronous throw, asynchronous rejection, or timeout keeps the
handled cursor fixed and prevents later delivery.

Delivery is at least once across reconnects and process restarts. Handler code
that calls external systems must therefore make side effects idempotent using a
stable key such as the Tether event id, task id, or provider operation id.

Participant delivery uses finite defaults:

| Policy | Default |
| --- | ---: |
| Handler timeout | 30,000 ms |
| Delivery queue | 2,000 entries |
| Recovery attempts per sequence | 5 |
| Cursor write timeout | 5,000 ms |
| Cursor write attempts | 5 |
| Cursor retry backoff | 100 ms exponential, capped at 2,000 ms |
| Graceful shutdown timeout | 30,000 ms |

Override these with `eventDelivery.handlerTimeoutMs`,
`eventDelivery.maxQueueSize`, `eventDelivery.maxRecoveryAttempts`,
`cursorPersist.writeTimeoutMs`, `cursorPersist.retryAttempts`,
`cursorPersist.retryBaseDelayMs`, `cursorPersist.retryMaxDelayMs`, and
`shutdownTimeoutMs`.

## Recovery, Errors, and Diagnostics

Delivery failure starts bounded reconnect recovery from the handled cursor. If
one poison event exhausts its recovery budget, or the server reports an
unrepairable replay condition, the participant enters Paused State. Repair the
handler or replay condition, then call `reconnect()` explicitly. The client does
not skip the failed event or reconnect indefinitely.

Use the exported typed errors for safe control flow and logging:

- `ParticipantRuntimeEventDeliveryError` for handler rejection or timeout.
- `ParticipantRuntimeTerminalStreamError` for Paused State and terminal replay failures, including `rest_replay_failed` for REST authentication or malformed-response failures.
- `ParticipantRuntimeCursorPersistError` after one bounded cursor-write cycle exhausts.
- `ParticipantRuntimeCommandOutcomeUnknownError` when recovery interrupts a command whose server-side outcome may already have occurred. Do not blindly retry that command.
- `ParticipantRuntimeShutdownError` when graceful delivery, socket, or cursor settlement remains incomplete.

These errors expose safe correlation metadata. Do not log raw event payloads,
authorization tokens, or arbitrary provider error bodies. `debugInfo()` exposes
received, handled, and persisted cursors, pending cursor progress, queue and
active delivery state, recovery counters, failure counters, Paused State, and
socket state.

## Durable Cursors and Recent Events

When `cursorStore.read()` returns a non-null sequence, that durable value is the
authoritative startup resume point even when it is below configured `afterSeq`.
`afterSeq` seeds startup only when the store is empty or absent. Cursor writes
are serialized and coalesced, and `lastPersistedSeq` advances only after the
store acknowledges the write.

The cursor store must implement `write(seq)` as an atomic maximum operation,
such as `stored = max(stored, seq)`. A write that exceeds its client deadline
can still complete later, so a plain last-writer-wins assignment can regress
durable state after a newer sequence has already been acknowledged.

`lastHandledSeq` is the runtime's safe resume cursor. `lastObservedSeq` remains a
deprecated alias of `lastHandledSeq` through the 0.2.x line. Recent events are
unique, bounded, successfully handled history only. Received but unhandled
events never enter recent-event context or durable progress.

## Graceful Shutdown

`close()` remains an immediate stop signal with a best-effort cursor flush. Use
`closeAndWait()` when the host can await shutdown. It waits, under one overall
deadline, for the active event handler to settle, the socket to close, and the
final serialized cursor write to be acknowledged.

For lower-level startup that needs the same subscriber-first ordering, use
`ParticipantRuntimeClient.create()`, register handlers, then call `open()`.

```ts
const client = await ParticipantRuntimeClient.create(config);
const unsubscribe = client.onEvent(handleEvent);

try {
  await client.open();
  await client.waitForReplayComplete();
} finally {
  unsubscribe();
  await client.closeAndWait();
}
```
