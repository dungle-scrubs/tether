import { sessionSummaryContentSchema } from "@dungle-scrubs/tether-protocol";
import type { SessionSummaryContent } from "@dungle-scrubs/tether-protocol";

/**
 * Owns deterministic Session Summary evaluation policy. It scores already
 * generated structured candidates and never performs inference or production
 * publication.
 */

/** Durable identity allowlists grounded by one sanitized evaluation case. */
export interface SessionSummaryEvalIdentities {
  readonly approval: readonly string[];
  readonly decision: readonly string[];
  readonly participant: readonly string[];
  readonly task: readonly string[];
}

/** Projection fields that candidate claims must not contradict. */
export interface SessionSummaryEvalProjection {
  readonly activity: "idle" | "queued" | "running" | "settled";
  readonly archived: boolean;
  readonly deleted: boolean;
  readonly title: string;
}

/** One required source-grounded fact used by the critical-fact scorer. */
export interface SessionSummaryEvalRequiredFact {
  readonly category: SessionSummaryContent["facts"][number]["category"];
  readonly sourceEventIds: readonly string[];
  readonly statements: readonly string[];
  readonly subjectIds: readonly string[];
}

/** Sanitized deterministic inputs and expectations for one evaluation case. */
export interface SessionSummaryEvalCase {
  readonly allowedIdentities: SessionSummaryEvalIdentities;
  readonly eventIds: readonly string[];
  readonly forbiddenClaims: readonly string[];
  readonly id: string;
  readonly maxOutputBytes: number;
  readonly projection: SessionSummaryEvalProjection;
  readonly projectionContradictions?: readonly string[];
  readonly requiredFacts: readonly SessionSummaryEvalRequiredFact[];
  readonly sensitiveValues: readonly string[];
}

/** Result of one deterministic hard gate. */
export interface SessionSummaryEvalGateResult {
  readonly failures: readonly string[];
  readonly passed: boolean;
}

/** Current deterministic evaluation result for one candidate and case. */
export interface SessionSummaryEvalResult {
  readonly gates: {
    readonly criticalFacts: SessionSummaryEvalGateResult;
    readonly forbiddenClaims: SessionSummaryEvalGateResult;
    readonly identity: SessionSummaryEvalGateResult;
    readonly outputSize: SessionSummaryEvalGateResult;
    readonly projection: SessionSummaryEvalGateResult;
    readonly schema: SessionSummaryEvalGateResult;
    readonly sensitivity: SessionSummaryEvalGateResult;
  };
  readonly passed: boolean;
}

/** Scores one structured candidate against one sanitized evaluation case. */
export function scoreSessionSummaryCase(
  evalCase: SessionSummaryEvalCase,
  candidate: unknown,
): SessionSummaryEvalResult {
  const parsed = sessionSummaryContentSchema.safeParse(candidate);
  const schema = gateResult(parsed.success ? [] : ["structured summary schema validation failed"]);
  if (!parsed.success) {
    const gates = {
      criticalFacts: gateResult([]),
      forbiddenClaims: gateResult([]),
      identity: gateResult([]),
      outputSize: gateResult([]),
      projection: gateResult([]),
      schema,
      sensitivity: gateResult([]),
    };
    return {
      gates,
      passed: false,
    };
  }
  const allowedEventIds = new Set(evalCase.eventIds);
  const failures = parsed.data.facts.flatMap((fact) => {
    const allowedSubjectIds = subjectIdentitiesForCategory(
      evalCase.allowedIdentities,
      fact.category,
    );
    return [
      ...fact.sourceEventIds
        .filter((eventId) => !allowedEventIds.has(eventId))
        .map((eventId) => `invented event identity: ${eventId}`),
      ...fact.subjectIds
        .filter((subjectId) => !allowedSubjectIds.has(subjectId))
        .map((subjectId) => `invented ${identityLabel(fact.category)} identity: ${subjectId}`),
    ];
  });
  const identity = gateResult(failures);
  const criticalFacts = gateResult(
    evalCase.requiredFacts
      .filter(
        (required) =>
          !parsed.data.facts.some(
            (fact) =>
              fact.category === required.category &&
              sameValues(fact.sourceEventIds, required.sourceEventIds) &&
              required.statements.includes(fact.statement) &&
              sameValues(fact.subjectIds, required.subjectIds),
          ),
      )
      .map(
        (required) =>
          `missing critical fact tuple: ${required.category}/${required.sourceEventIds.join(",")}`,
      ),
  );
  const serialized = JSON.stringify(parsed.data);
  const outputBytes = new TextEncoder().encode(serialized).byteLength;
  const outputSize = gateResult(
    outputBytes <= evalCase.maxOutputBytes
      ? []
      : [`structured summary output exceeds ${evalCase.maxOutputBytes} bytes`],
  );
  const sensitivity = gateResult(
    evalCase.sensitiveValues.flatMap((value, index) =>
      serialized.includes(value) ? [`designated sensitive value leaked: sensitive_${index}`] : [],
    ),
  );
  const forbiddenClaims = gateResult(
    evalCase.forbiddenClaims.flatMap((claim, index) =>
      serialized.includes(claim) ? [`forbidden exact claim emitted: forbidden_${index}`] : [],
    ),
  );
  const projection = gateResult(
    (evalCase.projectionContradictions ?? []).flatMap((claim, index) =>
      serialized.includes(claim)
        ? [`authoritative projection contradicted: projection_${index}`]
        : [],
    ),
  );
  const gates = {
    criticalFacts,
    forbiddenClaims,
    identity,
    outputSize,
    projection,
    schema,
    sensitivity,
  };
  return {
    gates,
    passed: Object.values(gates).every((gate) => gate.passed),
  };
}

function sameValues(left: readonly string[], right: readonly string[]): boolean {
  const sortedLeft = [...left].sort();
  const sortedRight = [...right].sort();
  return (
    sortedLeft.length === sortedRight.length &&
    sortedLeft.every((value, index) => value === sortedRight[index])
  );
}

function gateResult(failures: readonly string[]): SessionSummaryEvalGateResult {
  return {
    failures,
    passed: failures.length === 0,
  };
}

function identityLabel(category: SessionSummaryContent["facts"][number]["category"]): string {
  switch (category) {
    case "approval":
    case "decision":
    case "participant":
    case "task":
      return category;
    case "other":
    case "user_preference":
      return "durable";
  }
}

function subjectIdentitiesForCategory(
  identities: SessionSummaryEvalIdentities,
  category: SessionSummaryContent["facts"][number]["category"],
): ReadonlySet<string> {
  switch (category) {
    case "approval":
      return new Set([...identities.approval, ...identities.task]);
    case "decision":
      return new Set(identities.decision);
    case "participant":
      return new Set(identities.participant);
    case "task":
      return new Set(identities.task);
    case "other":
    case "user_preference":
      return new Set([
        ...identities.approval,
        ...identities.decision,
        ...identities.participant,
        ...identities.task,
      ]);
  }
}
