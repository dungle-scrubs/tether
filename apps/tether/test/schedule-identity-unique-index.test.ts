import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";

import { describe, expect, it } from "vitest";

/**
 * The schedule-identity index must be unique so duplicate deterministic
 * scheduled runs are impossible at the database level, and the legacy baseline
 * probe must require that uniqueness.
 */
describe("schedule identity uniqueness", () => {
  it("declares the schedule-identity index as UNIQUE in its migration", () => {
    const migration = readFileSync(
      fileURLToPath(new URL("../drizzle/0011_special_blue_marvel.sql", import.meta.url)),
      "utf8",
    );
    expect(migration).toContain('CREATE UNIQUE INDEX "tasks_schedule_identity_idx" ON "tasks"');
    expect(migration).not.toContain('CREATE INDEX "tasks_schedule_identity_idx"');
  });

  it("declares the schedule-identity index as unique in the Drizzle schema", () => {
    const schema = readFileSync(
      fileURLToPath(new URL("../src/schema.ts", import.meta.url)),
      "utf8",
    );
    expect(schema).toContain('uniqueIndex("tasks_schedule_identity_idx")');
  });

  it("requires a unique schedule-identity index in the 0011 legacy baseline probe", () => {
    const db = readFileSync(fileURLToPath(new URL("../src/db.ts", import.meta.url)), "utf8");
    expect(db).toContain('hasUniqueIndex(database, "tasks_schedule_identity_idx")');
  });
});
