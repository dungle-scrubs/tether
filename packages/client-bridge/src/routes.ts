/** Builds stable Tether REST paths used by client bridges. */
export const clientBridgeRoutes = {
  /** Lists or creates task records in one session. */
  sessionTasks: (sessionId: string): string => `/sessions/${encodeURIComponent(sessionId)}/tasks`,
  /** Lists durable session events after one replay cursor. */
  sessionEvents: (sessionId: string, afterSeq = 0): string =>
    `/sessions/${encodeURIComponent(sessionId)}/events?after=${encodeURIComponent(String(afterSeq))}`,
  /** Lists task records in one session with a status filter. */
  sessionTasksWithStatus: (sessionId: string, status: string): string =>
    `/sessions/${encodeURIComponent(sessionId)}/tasks?status=${encodeURIComponent(status)}`,
  /** Reads one task record. */
  task: (sessionId: string, taskId: string): string =>
    `/sessions/${encodeURIComponent(sessionId)}/tasks/${encodeURIComponent(taskId)}`,
  /** Reads one task with its currently advertised task contract. */
  taskInspection: (sessionId: string, taskId: string): string =>
    `${clientBridgeRoutes.task(sessionId, taskId)}?include=contract`,
  /** Cancels one task. */
  taskCancel: (sessionId: string, taskId: string): string =>
    `${clientBridgeRoutes.task(sessionId, taskId)}/cancel`,
  /** Records approval intent for one completed task. */
  taskApproval: (sessionId: string, taskId: string): string =>
    `${clientBridgeRoutes.task(sessionId, taskId)}/approval`,
  /** Lists external client bindings for one provider. */
  clientBindings: (provider: string): string =>
    `/client-bindings?provider=${encodeURIComponent(provider)}`,
  /** Resolves or creates a client binding. */
  clientBindingSession: (): string => "/client-bindings/session",
} as const;
