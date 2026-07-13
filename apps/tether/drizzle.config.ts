import { defineConfig } from "drizzle-kit";

/** Reads the database URL for Drizzle without falling back to weak credentials. */
function readDatabaseUrl(): string {
  const databaseUrl = process.env.DATABASE_URL?.trim();
  if (databaseUrl === undefined || databaseUrl.length === 0) {
    throw new Error("DATABASE_URL is required");
  }
  return databaseUrl;
}

export default defineConfig({
  dbCredentials: {
    url: readDatabaseUrl(),
  },
  dialect: "postgresql",
  out: "./drizzle",
  schema: "./src/schema.ts",
});
