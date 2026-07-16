import { z } from "zod";

/** Approval behavior advertised for an orchestratable task contract. */
export type TaskContractApproval = "none" | "optional" | "required_for_mutation";

/** Lightweight JSON Schema object used for participant contract discovery. */
export type TaskContractJsonSchema = Readonly<Record<string, unknown>>;

/** Common Tether task contract envelope shared by orchestratable participants. */
export interface TaskContractEnvelope {
  /** Whether this task can require an approval.recorded event before mutation. */
  readonly approval: TaskContractApproval;
  /** Stable reference to the participant-owned input schema. */
  readonly inputSchemaRef: string;
  /** Participant runtime kind that owns this task kind. */
  readonly participantRuntimeKind: string;
  /** Whether coordinators may create this task without expecting immediate mutation. */
  readonly readOnlyByDefault: boolean;
  /** Stable reference to the participant-owned result schema. */
  readonly resultSchemaRef: string;
  /** Durable Tether task kind. */
  readonly taskKind: string;
  /** Contract schema version. */
  readonly version: string;
}

/** Compact task contract summary safe to publish in participant capabilities. */
export interface TaskContractSummary extends TaskContractEnvelope {
  /** Short description for coordinators and clients. */
  readonly description: string;
  /** Optional participant-owned input JSON Schema for local validation and inspection. */
  readonly inputJsonSchema?: TaskContractJsonSchema;
  /** Optional participant-owned result JSON Schema for inspection and orchestration. */
  readonly resultJsonSchema?: TaskContractJsonSchema;
  /** Human-readable title. */
  readonly title: string;
}

/** Full task contract advertisement with common envelope plus domain schemas. */
export interface TaskContractAdvertisement<TDomain extends object = Record<string, unknown>> {
  /** Common Tether task contract envelope. */
  readonly common: TaskContractEnvelope;
  /** Participant-owned domain contract or schema bundle. */
  readonly domain: TDomain;
}

/** Runtime validator for task contract approval behavior. */
export const taskContractApprovalSchema = z.enum(["none", "optional", "required_for_mutation"]);

/** Runtime validator for a task contract envelope. */
export const taskContractEnvelopeSchema = z.object({
  approval: taskContractApprovalSchema,
  inputSchemaRef: z.string().min(1),
  participantRuntimeKind: z.string().min(1),
  readOnlyByDefault: z.boolean(),
  resultSchemaRef: z.string().min(1),
  taskKind: z.string().min(1),
  version: z.string().min(1),
});

/** Runtime validator for compact task contract summaries in capabilities. */
export const taskContractSummarySchema = taskContractEnvelopeSchema.extend({
  description: z.string().min(1),
  inputJsonSchema: z.record(z.string(), z.unknown()).optional(),
  resultJsonSchema: z.record(z.string(), z.unknown()).optional(),
  title: z.string().min(1),
});

/** Runtime validator for full task contract advertisements. */
export const taskContractAdvertisementSchema = z.object({
  common: taskContractEnvelopeSchema,
  domain: z.record(z.string(), z.unknown()),
});

/**
 * Validates the conservative JSON Schema subset Tether uses for advertised
 * task-contract input checks. This intentionally covers only the subset used
 * by participant contracts; full domain validation remains participant-owned.
 */
export function validateJsonSchemaSubset(
  value: unknown,
  schema: Readonly<Record<string, unknown>>,
  path: string,
): readonly string[] {
  const issues: string[] = [];
  const type = schema.type;
  if (typeof type === "string" && !jsonSchemaTypeMatches(value, type)) {
    return [`${path} must be ${type}`];
  }
  if (Array.isArray(type) && type.every((item) => typeof item === "string")) {
    const allowedTypes = type.filter((item): item is string => typeof item === "string");
    if (!allowedTypes.some((allowedType) => jsonSchemaTypeMatches(value, allowedType))) {
      return [`${path} must be one of ${allowedTypes.join(", ")}`];
    }
  }
  const enumValues = schema.enum;
  if (Array.isArray(enumValues) && !enumValues.some((item) => Object.is(item, value))) {
    issues.push(`${path} must be one of ${enumValues.map(String).join(", ")}`);
  }
  if ("const" in schema && !Object.is(schema.const, value)) {
    issues.push(`${path} must equal ${String(schema.const)}`);
  }
  if (typeof value === "number") {
    const minimum = schema.minimum;
    if (typeof minimum === "number" && value < minimum) {
      issues.push(`${path} must be >= ${minimum}`);
    }
    const maximum = schema.maximum;
    if (typeof maximum === "number" && value > maximum) {
      issues.push(`${path} must be <= ${maximum}`);
    }
  }
  if (schema.type === "object" && isPlainRecord(value)) {
    issues.push(...validateObjectSchemaSubset(value, schema, path));
  }
  if (schema.type === "array" && Array.isArray(value)) {
    const items = schema.items;
    if (isPlainRecord(items)) {
      value.forEach((item, index) => {
        issues.push(...validateJsonSchemaSubset(item, items, `${path}[${index}]`));
      });
    }
  }
  return issues;
}

/** Validates object-specific JSON Schema subset fields. */
function validateObjectSchemaSubset(
  value: Readonly<Record<string, unknown>>,
  schema: Readonly<Record<string, unknown>>,
  path: string,
): readonly string[] {
  const issues: string[] = [];
  const required = schema.required;
  if (Array.isArray(required)) {
    for (const field of required) {
      if (typeof field === "string" && !(field in value)) {
        issues.push(`${path}.${field} is required`);
      }
    }
  }
  const properties = isPlainRecord(schema.properties) ? schema.properties : {};
  if (schema.additionalProperties === false) {
    for (const field of Object.keys(value)) {
      if (!(field in properties)) {
        issues.push(`${path}.${field} is not allowed`);
      }
    }
  }
  for (const [field, propertySchema] of Object.entries(properties)) {
    if (field in value && isPlainRecord(propertySchema)) {
      issues.push(...validateJsonSchemaSubset(value[field], propertySchema, `${path}.${field}`));
    }
  }
  return issues;
}

/** Checks JSON Schema primitive type names against a runtime value. */
function jsonSchemaTypeMatches(value: unknown, type: string): boolean {
  switch (type) {
    case "array":
      return Array.isArray(value);
    case "boolean":
      return typeof value === "boolean";
    case "integer":
      return Number.isInteger(value);
    case "null":
      return value === null;
    case "number":
      return typeof value === "number" && Number.isFinite(value);
    case "object":
      return isPlainRecord(value);
    case "string":
      return typeof value === "string";
    default:
      return true;
  }
}

/** Returns whether a value is a non-array JSON object. */
function isPlainRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

/**
 * Current Schedule Window algorithm version. The algorithm version is part of
 * scheduled task identity so a future windowing change produces a distinct task
 * rather than colliding with runs bucketed by the previous algorithm.
 */
export const scheduleWindowAlgorithmVersion = 1;

/**
 * Provider plus opaque configured-account identity for one mailbox. A
 * provider-local message id never identifies work outside its Mailbox Scope.
 */
export interface MailboxScope {
  /** Immutable opaque configured account identity within one provider. */
  readonly accountId: string;
  /** Provider that owns the account, such as `fastmail` or `gmail`. */
  readonly provider: string;
}

/**
 * Deterministic half-open UTC time bucket for recurring work identity. Version 1
 * is `[floor(unix_ms / interval_ms) * interval_ms, start + interval_ms)`.
 */
export interface ScheduleWindow {
  /** Windowing algorithm version that produced this bucket. */
  readonly algorithmVersion: number;
  /** Exclusive end of the half-open interval, in unix milliseconds. */
  readonly endMs: number;
  /** Configured interval width in milliseconds; part of task identity. */
  readonly intervalMs: number;
  /** Inclusive start of the half-open interval, in unix milliseconds. */
  readonly startMs: number;
}

/** Inputs that deterministically identify one Scheduled Maintenance Run. */
export interface ScheduledMaintenanceIdentity {
  /** Durable Tether task kind, such as `email_organization`. */
  readonly kind: string;
  /** Mailbox Scope the scheduled run is restricted to. */
  readonly mailboxScope: MailboxScope;
  /** Deterministic Schedule Window the run belongs to. */
  readonly scheduleWindow: ScheduleWindow;
  /** Durable Tether session that owns the scheduled run. */
  readonly sessionId: string;
}

/**
 * Computes the deterministic Schedule Window bucket for a timestamp. The
 * interval must be a positive integer number of milliseconds; a rolled-back
 * clock inside the same bucket resolves to the same window.
 */
export function computeScheduleWindow(
  unixMs: number,
  intervalMs: number,
  algorithmVersion: number = scheduleWindowAlgorithmVersion,
): ScheduleWindow {
  if (!Number.isSafeInteger(intervalMs) || intervalMs <= 0) {
    throw new Error(`Schedule Window interval must be a positive integer, received ${intervalMs}`);
  }
  if (!Number.isFinite(unixMs)) {
    throw new Error(`Schedule Window timestamp must be finite, received ${unixMs}`);
  }
  const startMs = Math.floor(unixMs / intervalMs) * intervalMs;
  return { algorithmVersion, endMs: startMs + intervalMs, intervalMs, startMs };
}

/**
 * Encodes a Schedule Window as a stable, human-inspectable bucket key. The key
 * carries algorithm version, interval, and start so distinct configurations can
 * never share one bucket string.
 */
export function scheduleWindowKey(window: ScheduleWindow): string {
  return `v${window.algorithmVersion}:${window.intervalMs}:${window.startMs}`;
}

/**
 * Derives a stable, opaque scheduled task id from a Scheduled Maintenance
 * Identity. Repeated derivation for one identity is deterministic; any change to
 * session, kind, Mailbox Scope, interval, algorithm version, or window start
 * yields a different id. Callers pass this into the existing task-creation
 * idempotency seam instead of performing a read-then-create race.
 */
export function deriveScheduledTaskId(identity: ScheduledMaintenanceIdentity): string {
  const canonical = JSON.stringify([
    identity.scheduleWindow.algorithmVersion,
    identity.sessionId,
    identity.kind,
    identity.mailboxScope.provider,
    identity.mailboxScope.accountId,
    identity.scheduleWindow.intervalMs,
    identity.scheduleWindow.startMs,
  ]);
  return `task_sched_${fnv1a64Hex(canonical)}`;
}

/** Reason a scheduled-supersession operation refuses to cancel a candidate task. */
export type ScheduledSupersessionRefusalReason =
  | "claimed"
  | "current_window"
  | "manual"
  | "schedule_identity_mismatch"
  | "terminal";

/** Every scheduled-supersession refusal reason, for exhaustive validation. */
export const scheduledSupersessionRefusalReasons = [
  "claimed",
  "current_window",
  "manual",
  "schedule_identity_mismatch",
  "terminal",
] as const satisfies readonly ScheduledSupersessionRefusalReason[];

/** Durable schedule identity attached to a scheduled task candidate. */
export interface CandidateScheduleIdentity {
  readonly mailboxScope: MailboxScope;
  readonly scheduleWindow: ScheduleWindow;
}

/**
 * Minimal candidate view a supersession decision is made against. It mirrors the
 * durable task columns that decide eligibility without pulling in the full
 * task record shape.
 */
export interface ScheduledSupersessionCandidate {
  readonly cancelledAt: string | null;
  readonly claimedBy: string | null;
  readonly completedAt: string | null;
  readonly failedAt: string | null;
  readonly kind: string;
  /** Schedule identity, or null for a manual (non-scheduled) task. */
  readonly schedule: CandidateScheduleIdentity | null;
  readonly sessionId: string;
}

/** Outcome of classifying a candidate against a current scheduled identity. */
export type ScheduledSupersessionOutcome =
  | { readonly decision: "supersede" }
  | { readonly decision: "refuse"; readonly reason: ScheduledSupersessionRefusalReason };

/**
 * Decides whether a candidate task may be atomically superseded by the current
 * scheduled run. This is the authoritative refusal logic the atomic persistence
 * predicate mirrors: only an older, matching, unclaimed, nonterminal scheduled
 * run is superseded. A racing claim, a manual task, a terminal task, a task with
 * a different schedule identity, or the current/newer window all refuse.
 */
export function classifyScheduledSupersession(
  candidate: ScheduledSupersessionCandidate,
  target: ScheduledMaintenanceIdentity,
): ScheduledSupersessionOutcome {
  if (candidate.schedule === null) {
    return { decision: "refuse", reason: "manual" };
  }
  if (!scheduleIdentityMatches(candidate, target)) {
    return { decision: "refuse", reason: "schedule_identity_mismatch" };
  }
  if (
    candidate.completedAt !== null ||
    candidate.failedAt !== null ||
    candidate.cancelledAt !== null
  ) {
    return { decision: "refuse", reason: "terminal" };
  }
  if (candidate.claimedBy !== null) {
    return { decision: "refuse", reason: "claimed" };
  }
  if (candidate.schedule.scheduleWindow.startMs >= target.scheduleWindow.startMs) {
    return { decision: "refuse", reason: "current_window" };
  }
  return { decision: "supersede" };
}

/** Returns whether a candidate shares the exact schedule identity of the target. */
function scheduleIdentityMatches(
  candidate: ScheduledSupersessionCandidate,
  target: ScheduledMaintenanceIdentity,
): boolean {
  const schedule = candidate.schedule;
  if (schedule === null) {
    return false;
  }
  return (
    candidate.sessionId === target.sessionId &&
    candidate.kind === target.kind &&
    schedule.mailboxScope.provider === target.mailboxScope.provider &&
    schedule.mailboxScope.accountId === target.mailboxScope.accountId &&
    schedule.scheduleWindow.algorithmVersion === target.scheduleWindow.algorithmVersion &&
    schedule.scheduleWindow.intervalMs === target.scheduleWindow.intervalMs
  );
}

/** One refused candidate in a scheduled-supersession result. */
export interface ScheduledSupersessionRefusalRecord {
  readonly reason: ScheduledSupersessionRefusalReason;
  readonly taskId: string;
}

/** Typed result of a scheduled-supersession operation over one schedule identity. */
export interface ScheduledSupersessionResultRecord {
  readonly refusals: readonly ScheduledSupersessionRefusalRecord[];
  readonly supersededTaskIds: readonly string[];
}

/** Runtime validator for a mailbox scope. */
export const mailboxScopeSchema = z.object({
  accountId: z.string().min(1),
  provider: z.string().min(1),
});

/** Runtime validator for a deterministic Schedule Window. */
export const scheduleWindowSchema = z
  .object({
    algorithmVersion: z.number().int().positive(),
    endMs: z.number().int().nonnegative(),
    intervalMs: z.number().int().positive(),
    startMs: z.number().int().nonnegative(),
  })
  .refine((window) => Number.isSafeInteger(window.startMs + window.intervalMs), {
    message: "Schedule Window start plus interval must be a safe integer",
    path: ["startMs"],
  })
  .refine((window) => window.endMs === window.startMs + window.intervalMs, {
    message: "Schedule Window end must equal start plus interval",
    path: ["endMs"],
  });

/** Runtime validator for durable schedule identity attached to a task candidate. */
export const candidateScheduleIdentitySchema = z.object({
  mailboxScope: mailboxScopeSchema,
  scheduleWindow: scheduleWindowSchema,
});

/** Runtime validator for a scheduled-supersession refusal reason. */
export const scheduledSupersessionRefusalReasonSchema = z.enum(scheduledSupersessionRefusalReasons);

/** Runtime validator for a scheduled-supersession result envelope. */
export const scheduledSupersessionResultSchema = z.object({
  refusals: z.array(
    z.object({
      reason: scheduledSupersessionRefusalReasonSchema,
      taskId: z.string().min(1),
    }),
  ),
  supersededTaskIds: z.array(z.string().min(1)),
});

/**
 * 64-bit FNV-1a hash rendered as lowercase hex. Deterministic and dependency
 * free so scheduled task ids are identical across every runtime that derives
 * them from the same identity.
 */
function fnv1a64Hex(input: string): string {
  const prime = 0x100000001b3n;
  const mask = 0xffffffffffffffffn;
  let hash = 0xcbf29ce484222325n;
  for (let index = 0; index < input.length; index += 1) {
    hash ^= BigInt(input.charCodeAt(index));
    hash = (hash * prime) & mask;
  }
  return hash.toString(16).padStart(16, "0");
}
