import { randomUUID } from "node:crypto";

import { SpanStatusCode, trace } from "@opentelemetry/api";
import type { Attributes } from "@opentelemetry/api";

type LogLevel = "debug" | "error" | "info" | "warn";

type SpanAttributeValue = Attributes[string];

/**
 * Snapshot of a module boundary helper's current counters and last-known
 * failure state.
 */
export interface BoundaryDebugInfo {
  /** Number of boundary operations currently running. */
  readonly activeOperations: number;
  /** Total number of boundary calls observed since helper creation. */
  readonly boundaryCalls: number;
  /** Total number of boundary calls that ended by throwing. */
  readonly boundaryFailures: number;
  /** Whether structured enter, exit, and error boundary logs are enabled. */
  readonly boundaryLogsEnabled: boolean;
  /** Whether debug-level diagnostic logs are enabled. */
  readonly debugEnabled: boolean;
  /** Most recent boundary failure metadata, or null when none has failed. */
  readonly lastError: StructuredErrorInfo | null;
  /** Most recent operation name observed by the helper. */
  readonly lastOperation: string | null;
  /** Logical module name attached to logs, spans, errors, and debug snapshots. */
  readonly moduleName: string;
}

/**
 * Construction options for module-boundary observability helpers.
 */
export interface ModuleObservabilityOptions {
  /** Enables structured boundary enter, exit, and error logs when true. */
  readonly boundaryLogsEnabled?: boolean;
  /** Enables scoped debug logs when true. */
  readonly debugEnabled?: boolean;
  /** Optional structured logger; defaults to JSON console logging. */
  readonly logger?: StructuredLogger;
  /** Logical module name attached to logs, spans, errors, and debug snapshots. */
  readonly moduleName: string;
  /** Optional OpenTelemetry tracer name; defaults to the module name. */
  readonly tracerName?: string;
}

/**
 * Stable error metadata captured from an unknown thrown value.
 */
export interface StructuredErrorInfo {
  /** Human-readable error message. */
  readonly message: string;
  /** Error class or normalized error name. */
  readonly name: string;
}

/**
 * Structured module-boundary log entry emitted by ModuleObservability.
 */
export interface StructuredLogEntry {
  /** ISO timestamp for when the log entry was emitted. */
  readonly at: string;
  /** Operation-specific structured payload with sensitive data already redacted by the caller. */
  readonly data: Record<string, unknown>;
  /** Severity level for routing the entry to the matching log sink. */
  readonly level: LogLevel;
  /** Stable event name or debug message for the boundary event. */
  readonly message: string;
  /** Logical module name that emitted the entry. */
  readonly moduleName: string;
  /** Public boundary operation associated with the entry. */
  readonly operation: string;
  /**
   * Synthetic module-boundary correlation id for pairing enter, exit, and error
   * logs; this is not an OpenTelemetry trace id.
   */
  readonly traceId: string;
}

/**
 * Sink for structured module-boundary log entries.
 */
export interface StructuredLogger {
  /** Receives one structured log entry. */
  readonly log: (entry: StructuredLogEntry) => void;
}

/**
 * Error thrown when a Tether module invariant breaks.
 */
export class TetherInvariantError extends Error {
  readonly code = "TETHER_INVARIANT_VIOLATION";
  readonly details: Record<string, unknown>;
  readonly moduleName: string;
  readonly operation: string;

  /**
   * Captures a violated invariant with machine-readable module and operation
   * context.
   */
  constructor(input: {
    readonly cause?: unknown;
    readonly details?: Record<string, unknown>;
    readonly message: string;
    readonly moduleName: string;
    readonly operation: string;
  }) {
    super(input.message, input.cause === undefined ? undefined : { cause: input.cause });
    this.details = input.details ?? {};
    this.moduleName = input.moduleName;
    this.name = "TetherInvariantError";
    this.operation = input.operation;
  }
}

/**
 * JSON logger used when boundary logs are enabled through module options.
 */
export class ConsoleStructuredLogger implements StructuredLogger {
  /**
   * Writes structured log entries to the matching console level.
   */
  log(entry: StructuredLogEntry): void {
    const encoded = JSON.stringify(entry);
    if (entry.level === "error") {
      console.error(encoded);
      return;
    }
    if (entry.level === "warn") {
      console.warn(encoded);
      return;
    }
    console.log(encoded);
  }
}

/**
 * Boundary observability helper for deep modules.
 */
export class ModuleObservability {
  private activeOperations = 0;
  private boundaryCalls = 0;
  private boundaryFailures = 0;
  private readonly boundaryLogsEnabled: boolean;
  private readonly debugEnabled: boolean;
  private lastError: StructuredErrorInfo | null = null;
  private lastOperation: string | null = null;
  private readonly logger: StructuredLogger;
  private readonly moduleName: string;
  private readonly tracer;

  /**
   * Creates an observability boundary helper with optional logging and tracing
   * dependencies.
   */
  constructor(options: ModuleObservabilityOptions) {
    this.boundaryLogsEnabled = options.boundaryLogsEnabled ?? false;
    this.debugEnabled = options.debugEnabled ?? false;
    this.logger = options.logger ?? new ConsoleStructuredLogger();
    this.moduleName = options.moduleName;
    this.tracer = trace.getTracer(options.tracerName ?? options.moduleName);
  }

  /**
   * Snapshots boundary counters and the most recent failure.
   */
  debugInfo(): BoundaryDebugInfo {
    return {
      activeOperations: this.activeOperations,
      boundaryCalls: this.boundaryCalls,
      boundaryFailures: this.boundaryFailures,
      boundaryLogsEnabled: this.boundaryLogsEnabled,
      debugEnabled: this.debugEnabled,
      lastError: this.lastError,
      lastOperation: this.lastOperation,
      moduleName: this.moduleName,
    };
  }

  /**
   * Runs one public boundary method inside logs and an OpenTelemetry span.
   */
  async traceBoundary<TValue>(
    operation: string,
    input: Record<string, unknown>,
    action: () => Promise<TValue>,
    summarize: (value: TValue) => Record<string, unknown> = () => ({}),
  ): Promise<TValue> {
    const traceId = `trace_${randomUUID()}`;
    this.activeOperations += 1;
    this.boundaryCalls += 1;
    this.lastOperation = operation;
    this.logBoundary("debug", "boundary.enter", operation, traceId, input);
    const startedAt = performance.now();
    return this.tracer.startActiveSpan(
      `${this.moduleName}.${operation}`,
      { attributes: toSpanAttributes({ ...input, traceId }) },
      async (span) => {
        try {
          const value = await action();
          const durationMs = Math.round(performance.now() - startedAt);
          const summary = summarize(value);
          span.setAttributes(toSpanAttributes({ ...summary, durationMs }));
          span.setStatus({ code: SpanStatusCode.OK });
          this.logBoundary("info", "boundary.exit", operation, traceId, {
            ...summary,
            durationMs,
          });
          return value;
        } catch (error) {
          const durationMs = Math.round(performance.now() - startedAt);
          const errorInfo = toStructuredErrorInfo(error);
          this.boundaryFailures += 1;
          this.lastError = errorInfo;
          span.recordException(error instanceof Error ? error : String(error));
          span.setAttributes(toSpanAttributes({ durationMs, errorName: errorInfo.name }));
          span.setStatus({ code: SpanStatusCode.ERROR, message: errorInfo.message });
          this.logBoundary("error", "boundary.error", operation, traceId, {
            durationMs,
            error: errorInfo,
          });
          throw error;
        } finally {
          span.end();
          this.activeOperations -= 1;
        }
      },
    );
  }

  /**
   * Throws a typed invariant error when internal module assumptions break.
   */
  assertInvariant(
    condition: boolean,
    operation: string,
    message: string,
    details: Record<string, unknown> = {},
  ): asserts condition {
    if (condition) {
      return;
    }
    throw new TetherInvariantError({
      details,
      message,
      moduleName: this.moduleName,
      operation,
    });
  }

  /**
   * Emits scoped verbose data without turning on global logging.
   */
  debug(operation: string, message: string, data: Record<string, unknown> = {}): void {
    if (!this.debugEnabled) {
      return;
    }
    this.logBoundary("debug", message, operation, `trace_${randomUUID()}`, data);
  }

  private logBoundary(
    level: LogLevel,
    message: string,
    operation: string,
    traceId: string,
    data: Record<string, unknown>,
  ): void {
    if (!this.boundaryLogsEnabled && !this.debugEnabled) {
      return;
    }
    this.logger.log({
      at: new Date().toISOString(),
      data,
      level,
      message,
      moduleName: this.moduleName,
      operation,
      traceId,
    });
  }
}

/**
 * Reads module observability toggles from environment variables.
 */
export function readModuleObservabilityOptions(
  moduleName: string,
  env: NodeJS.ProcessEnv = process.env,
): ModuleObservabilityOptions {
  return {
    boundaryLogsEnabled: parseBooleanEnv(env.OBSERVABILITY),
    debugEnabled: parseBooleanEnv(env.DEBUG),
    moduleName,
  };
}

/**
 * Parses truthy boolean environment values used by observability toggles.
 */
function parseBooleanEnv(value: string | undefined): boolean {
  return value === "1" || value === "true" || value === "yes";
}

/**
 * Converts structured log fields into OpenTelemetry-compatible span attributes.
 */
function toSpanAttributes(fields: Record<string, unknown>): Attributes {
  const attributes: Record<string, SpanAttributeValue> = {};
  for (const [key, value] of Object.entries(fields)) {
    const attribute = toSpanAttribute(value);
    if (attribute !== null) {
      attributes[key] = attribute;
    }
  }
  return attributes;
}

/**
 * Keeps only scalar and homogeneous scalar-array values supported by
 * OpenTelemetry span attributes.
 */
function toSpanAttribute(value: unknown): SpanAttributeValue | null {
  if (typeof value === "boolean" || typeof value === "number" || typeof value === "string") {
    return value;
  }
  if (Array.isArray(value)) {
    if (value.every((item): item is string => typeof item === "string")) {
      return value;
    }
    if (value.every((item): item is number => typeof item === "number")) {
      return value;
    }
    if (value.every((item): item is boolean => typeof item === "boolean")) {
      return value;
    }
    return null;
  }
  return null;
}

/**
 * Normalizes unknown thrown values into structured error metadata.
 */
function toStructuredErrorInfo(error: unknown): StructuredErrorInfo {
  if (error instanceof Error) {
    return { message: error.message, name: error.name };
  }
  return { message: "Unknown error", name: "UnknownError" };
}
