import { webSocketOperation } from "./protocol.js";

/** Client-originated WebSocket command operation accepted by the server. */
export type ClientWebSocketOperation =
  | typeof webSocketOperation.publish
  | typeof webSocketOperation.taskCancel
  | typeof webSocketOperation.taskClaim
  | typeof webSocketOperation.taskComplete
  | typeof webSocketOperation.taskFail
  | typeof webSocketOperation.taskRefresh
  | typeof webSocketOperation.taskRelease;

/** Named WebSocket command spec used by the HTTP adapter dispatch boundary. */
export interface WebSocketCommandSpec<TOperation extends ClientWebSocketOperation> {
  /** Stable command name for diagnostics. */
  readonly name: string;
  /** Protocol operation string received from clients. */
  readonly operation: TOperation;
  /** Whether the command requires participant presence/control. */
  readonly requiresParticipant: boolean;
}

/** Server-supported client WebSocket commands. */
export const clientWebSocketCommandSpecs = [
  {
    name: "publish",
    operation: webSocketOperation.publish,
    requiresParticipant: false,
  },
  {
    name: "task.claim",
    operation: webSocketOperation.taskClaim,
    requiresParticipant: true,
  },
  {
    name: "task.cancel",
    operation: webSocketOperation.taskCancel,
    requiresParticipant: true,
  },
  {
    name: "task.refresh",
    operation: webSocketOperation.taskRefresh,
    requiresParticipant: true,
  },
  {
    name: "task.complete",
    operation: webSocketOperation.taskComplete,
    requiresParticipant: true,
  },
  {
    name: "task.fail",
    operation: webSocketOperation.taskFail,
    requiresParticipant: true,
  },
  {
    name: "task.release",
    operation: webSocketOperation.taskRelease,
    requiresParticipant: true,
  },
] as const satisfies readonly WebSocketCommandSpec<ClientWebSocketOperation>[];

/** Finds the command spec for a client-supplied operation string. */
export function findClientWebSocketCommandSpec(
  operation: string,
): WebSocketCommandSpec<ClientWebSocketOperation> | null {
  return clientWebSocketCommandSpecs.find((spec) => spec.operation === operation) ?? null;
}
