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

Use `runParticipantRuntime` for most external agents. It connects the
participant stream, installs the default structured error channel, waits for
replay, runs optional replay-ready hooks, processes claimable tasks, refreshes
claims, and runs hook cleanup after the claim loop exits.

Use `ParticipantRuntimeClient` directly only when an adapter needs lower-level
control over participant WebSocket commands such as `appendEvent`, `claimTask`,
`refreshTaskClaim`, `completeTask`, or `failTask`.

Use `SessionEventStreamClient` for passive observers that need session replay
and live events without participant identity or task authority. Bridges,
dashboards, and approval observers should use this API when they only need
`onEvent`, `onError`, `waitForReplayComplete`, reconnect behavior, and stream
diagnostics.
