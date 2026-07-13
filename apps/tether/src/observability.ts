/**
 * Compatibility exports for service modules that still import observability
 * from the app-local path. The implementation is owned by @dungle-scrubs/tether-client.
 */
export {
  ConsoleStructuredLogger,
  ModuleObservability,
  readModuleObservabilityOptions,
  TetherInvariantError,
} from "@dungle-scrubs/tether-client/observability";

export type {
  BoundaryDebugInfo,
  ModuleObservabilityOptions,
  StructuredErrorInfo,
  StructuredLogEntry,
  StructuredLogger,
} from "@dungle-scrubs/tether-client/observability";
