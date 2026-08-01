import { readMigrationFiles } from "drizzle-orm/migrator";
import { describe, expect, it, vi } from "vitest";

import {
  isDatabaseMigrationCurrent,
  type MigrationDatabase,
  readDatabaseMigrationReadiness,
} from "../src/database-migration.js";

describe("database migration readiness", () => {
  it("accepts only the complete ordered generated journal", async () => {
    const migrations = readMigrationFiles({ migrationsFolder: "drizzle" });
    const rows = migrations.map((migration) => ({
      createdAt: String(migration.folderMillis),
      hash: migration.hash,
    }));

    await expect(isDatabaseMigrationCurrent(createMigrationDatabase(rows))).resolves.toBe(true);
  });

  it("rejects a corrupt prefix even when the final journal row matches the generated head", async () => {
    const migrations = readMigrationFiles({ migrationsFolder: "drizzle" });
    const rows = migrations.map((migration, index) => ({
      createdAt: String(migration.folderMillis),
      hash: index === 0 ? "corrupt-prefix" : migration.hash,
    }));

    await expect(isDatabaseMigrationCurrent(createMigrationDatabase(rows))).resolves.toBe(false);
  });

  it("revalidates durable journal state after a previously current result", async () => {
    const migrations = readMigrationFiles({ migrationsFolder: "drizzle" });
    const rows = migrations.map((migration) => ({
      createdAt: String(migration.folderMillis),
      hash: migration.hash,
    }));
    const database = createMigrationDatabase(rows);

    await expect(isDatabaseMigrationCurrent(database)).resolves.toBe(true);
    const first = rows[0];
    if (!first) throw new Error("Expected generated migrations");
    first.hash = "restored-older-journal";
    await expect(isDatabaseMigrationCurrent(database)).resolves.toBe(false);
  });

  it("distinguishes an absent journal from a query or transport outage", async () => {
    await expect(
      readDatabaseMigrationReadiness(createFailingMigrationDatabase({ code: "42P01" })),
    ).resolves.toBe("incomplete");
    await expect(
      readDatabaseMigrationReadiness(createFailingMigrationDatabase({ code: "57P01" })),
    ).resolves.toBe("unavailable");
  });
});

/** Builds the checked-out-client seam used by complete journal inspection. */
function createMigrationDatabase(
  rows: readonly { readonly createdAt: string; readonly hash: string }[],
): MigrationDatabase {
  const client = {
    query: vi.fn().mockResolvedValue({ rows }),
    release: vi.fn(),
  };
  return {
    pool: { connect: vi.fn().mockResolvedValue(client) },
  } as unknown as MigrationDatabase;
}

/** Builds a database whose checked-out client rejects journal inspection. */
function createFailingMigrationDatabase(error: unknown): MigrationDatabase {
  const client = {
    query: vi.fn().mockRejectedValue(error),
    release: vi.fn(),
  };
  return {
    pool: { connect: vi.fn().mockResolvedValue(client) },
  } as unknown as MigrationDatabase;
}
