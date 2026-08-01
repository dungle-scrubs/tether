import { readdir, readFile } from "node:fs/promises";

import { describe, expect, it } from "vitest";

describe("recurring-work schedule migration", () => {
  it("versions and backfills existing scheduled rows behind an opaque scope key", async () => {
    const migrationDirectory = new URL("../drizzle/", import.meta.url);
    const migrationNames = (await readdir(migrationDirectory)).sort();
    const migrationName = migrationNames.find((name) => name.startsWith("0019_"));
    const migrationSql = await readFile(
      new URL(migrationName ?? "missing.sql", migrationDirectory),
      "utf8",
    );

    expect(migrationSql).toContain('ADD COLUMN "schedule_identity_version" integer');
    expect(migrationSql).toContain('ADD COLUMN "schedule_scope_key" text');
    expect(migrationSql).toContain('SET "schedule_identity_version" = 1');
    expect(migrationSql).toContain('"schedule_scope_key" =');
    expect(migrationSql).toContain('DROP INDEX "tasks_schedule_identity_idx"');
    expect(migrationSql).toContain(
      'CREATE UNIQUE INDEX "tasks_schedule_identity_idx" ON "tasks" USING btree ("session_id","kind","schedule_identity_version","schedule_scope_key","schedule_algorithm_version","schedule_interval_ms","schedule_window_start")',
    );
  });

  it("recognizes the versioned identity schema when baselining a journal-less database", async () => {
    const migrationModule = await readFile(
      new URL("../src/database-migration.ts", import.meta.url),
      "utf8",
    );

    expect(migrationModule).toContain('label: "0019 provider-neutral recurring-work identity"');
    expect(migrationModule).toContain('"schedule_identity_version"');
    expect(migrationModule).toContain('"schedule_scope_key"');
    expect(migrationModule).toMatch(
      /providerNeutralScheduleIdentityIndex[\s\S]*?columnNames: \[[\s\S]*?"session_id",[\s\S]*?"kind",[\s\S]*?"schedule_identity_version",[\s\S]*?"schedule_scope_key",/u,
    );
  });
});
