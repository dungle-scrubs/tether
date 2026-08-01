import { readFile, readdir } from "node:fs/promises";
import path from "node:path";

import { describe, expect, it } from "vitest";

import { sessionEventRetentionConfiguration } from "../src/session-event-retention.js";

describe("session event retention cutoff", () => {
  it("is statically disabled with every future safety gate unmet", () => {
    expect(sessionEventRetentionConfiguration).toEqual({
      boundaryAdvancementEnabled: false,
      deletionEnabled: false,
      reason: "future_safety_gates_unmet",
      status: "disabled",
      unmetGates: [
        "consumer_cursor_coverage",
        "atomic_boundary_advance",
        "replica_convergence",
        "backup_restore_validation",
        "recovery_contract",
      ],
    });
    expect(Object.isFrozen(sessionEventRetentionConfiguration)).toBe(true);
    expect(Object.isFrozen(sessionEventRetentionConfiguration.unmetGates)).toBe(true);
    expect(Object.keys(sessionEventRetentionConfiguration)).not.toContain("enabled");
  });

  it("has no raw event deletion or retention-boundary advancement implementation", async () => {
    const repositoryRoot = path.resolve(import.meta.dirname, "../../..");
    const implementationFiles = await collectFiles(repositoryRoot, [
      "apps/tether/drizzle",
      "apps/tether/src",
    ]);
    const sources = await Promise.all(
      implementationFiles.map(async (file) => ({
        file: path.relative(repositoryRoot, file),
        source: await readFile(file, "utf8"),
      })),
    );

    expect(
      sources.flatMap(({ file, source }) =>
        detectForbiddenRetentionImplementation(source).map((code) => `${file}:${code}`),
      ),
    ).toEqual([]);
    expect(collectDeleteCallInventory(sources)).toEqual([
      // Reviewed: bounded TTL cleanup for browser pairing abuse counters;
      // never touches session events or retention boundaries.
      "apps/tether/src/auth/browser-pairing-stores.ts:browserPairingExchangeFailures",
      "apps/tether/src/auth/grant-authority.ts:key",
      "apps/tether/src/auth/grant-authority.ts:key",
      "apps/tether/src/auth/grant-authority.ts:oldestKey",
      "apps/tether/src/auth/socket-registry.ts:entry.grantJti",
      "apps/tether/src/auth/socket-registry.ts:socket",
      "apps/tether/src/auth/socket-registry.ts:socket",
      // Reviewed: bounded fenced-generation history prune; never touches
      // session_events, and the current lease row is excluded by predicate.
      "apps/tether/src/db.ts:participantControlLeases",
      // Reviewed: fenced permanent session delete; cascades session-owned rows
      // but session_events removal happens only through this reviewed cascade.
      "apps/tether/src/db.ts:sessions",
      "apps/tether/src/host-presence.ts:instanceId",
      "apps/tether/src/host-presence.ts:listener",
      "apps/tether/src/host-presence.ts:sessionId",
      "apps/tether/src/host-presence.ts:sessionId",
      "apps/tether/src/hub.ts:nextSeq",
      "apps/tether/src/hub.ts:sessionId",
      "apps/tether/src/hub.ts:socket",
      "apps/tether/src/session-event-fanout.ts:notification.sessionId",
      "apps/tether/src/session-event-fanout.ts:sessionId",
      "apps/tether/src/session-scalability-runtime-state.ts:key",
      "apps/tether/src/session-scalability-runtime-state.ts:oldest",
      "apps/tether/src/session-service-core-effects.ts:sessionId, { hasLiveHost }",
      'apps/tether/src/websocket-participant-gateway.ts:"access_token"',
      'apps/tether/src/websocket-participant-gateway.ts:"ticket"',
      "apps/tether/src/websocket-participant-gateway.ts:key",
    ]);
  });

  it.each([
    ["Drizzle event-table delete", "database.delete(sessionEvents)", "event_table_delete"],
    [
      "interpolated event-table SQL",
      "sql`DELETE FROM $" + "{sessionEvents}`",
      "raw_destructive_sql",
    ],
    ["aliased event-table SQL", "sql`DELETE FROM $" + "{events}`", "raw_destructive_sql"],
    ["literal event-table SQL", "DELETE FROM session_events", "raw_destructive_sql"],
    ["event-table truncate", "TRUNCATE TABLE session_events", "raw_destructive_sql"],
    ["boundary writer", "setEventRetentionFloor(nextSeq)", "retention_boundary_write"],
    ["boundary column", "retainedFromSeq: bigint()", "retention_boundary_vocabulary"],
    ["boundary SQL column", "retention_boundary_seq bigint", "retention_boundary_vocabulary"],
    ["hidden enablement", "retentionEnabled: true", "retention_enablement"],
    ["service recovery emission", 'reason: "recovery_required"', "recovery_required_emission"],
  ])("detects forbidden %s fixtures", (_label, source, expectedCode) => {
    expect(detectForbiddenRetentionImplementation(source)).toContain(expectedCode);
  });
});

type RetentionContractViolation =
  | "event_table_delete"
  | "raw_destructive_sql"
  | "recovery_required_emission"
  | "retention_boundary_vocabulary"
  | "retention_boundary_write"
  | "retention_enablement";

/** Detects source forms that would violate the statically disabled cutoff. */
function detectForbiddenRetentionImplementation(source: string): RetentionContractViolation[] {
  const violations: RetentionContractViolation[] = [];
  if (/\.delete\s*\(\s*sessionEvents\s*\)/.test(source)) {
    violations.push("event_table_delete");
  }
  if (/\bDELETE\s+FROM\b|\bTRUNCATE(?:\s+TABLE)?\b/i.test(source)) {
    violations.push("raw_destructive_sql");
  }
  if (
    /\b(?:advance|move|persist|set|update|write)(?:Event|SessionEvent)?Retention(?:Boundary|Cursor|Floor)\b/.test(
      source,
    )
  ) {
    violations.push("retention_boundary_write");
  }
  if (
    /\b(?:oldestRetainedSeq|retainedFromSeq|retentionBoundary|retentionCursor|retentionFloor|minimumRetainedSeq|oldest_retained_seq|retained_from_seq|retention_boundar(?:y|ies)|retention_boundary_seq)\b/.test(
      source,
    )
  ) {
    violations.push("retention_boundary_vocabulary");
  }
  if (/\bretentionEnabled\s*:\s*true\b|\benableSessionEventRetention\b/.test(source)) {
    violations.push("retention_enablement");
  }
  if (/['"]recovery_required['"]/.test(source)) {
    violations.push("recovery_required_emission");
  }
  return violations;
}

/**
 * Inventories every method-style delete call. Any new target, including a
 * schema alias for `sessionEvents`, requires explicit safety review.
 */
function collectDeleteCallInventory(
  sources: readonly { readonly file: string; readonly source: string }[],
): string[] {
  return sources
    .flatMap(({ file, source }) =>
      Array.from(source.matchAll(/\.delete\s*\(\s*([^\n)]+?)\s*\)/g), (match) => {
        const target = match[1];
        if (target === undefined) {
          throw new Error("Delete call inventory matched without a target");
        }
        return `${file}:${target.trim()}`;
      }),
    )
    .sort();
}

/** Collects implementation files from a bounded repository-relative inventory. */
async function collectFiles(
  root: string,
  relativeDirectories: readonly string[],
): Promise<string[]> {
  const files: string[] = [];
  for (const relativeDirectory of relativeDirectories) {
    await visit(path.join(root, relativeDirectory), files);
  }
  return files;
}

/** Recursively visits one bounded implementation directory. */
async function visit(directory: string, files: string[]): Promise<void> {
  for (const entry of await readdir(directory, { withFileTypes: true })) {
    const absolutePath = path.join(directory, entry.name);
    if (entry.isDirectory()) {
      await visit(absolutePath, files);
    } else if (/\.(?:sql|ts)$/.test(entry.name)) {
      files.push(absolutePath);
    }
  }
}
