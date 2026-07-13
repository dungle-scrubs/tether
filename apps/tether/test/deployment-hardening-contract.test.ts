import { readFile } from "node:fs/promises";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";

import { parse } from "yaml";
import { describe, expect, it } from "vitest";

const testDir = dirname(fileURLToPath(import.meta.url));
const repoRoot = resolve(testDir, "../../..");
const appPortBinding = "$" + "{APP_HOST_BIND:-127.0.0.1}:$" + "{APP_HOST_PORT:-3025}:3025";
const databaseUrlRequirement = "$" + "{DATABASE_URL:?DATABASE_URL is required}";
const githubTokenExpression = "GITHUB_TOKEN: $" + "{{ secrets.GITHUB_TOKEN }}";
const postgresDbDefault = "$" + "{POSTGRES_DB:-tether}";
const postgresPasswordRequirement = "$" + "{POSTGRES_PASSWORD:?POSTGRES_PASSWORD is required}";
const postgresPortBinding =
  "$" + "{POSTGRES_HOST_BIND:-127.0.0.1}:$" + "{POSTGRES_HOST_PORT:-54329}:5432";
const postgresUserDefault = "$" + "{POSTGRES_USER:-tether}";

interface ComposeService {
  readonly environment?: Readonly<Record<string, string>>;
  readonly healthcheck?: {
    readonly test?: readonly string[];
  };
  readonly ports?: readonly string[];
}

interface ComposeFile {
  readonly services?: Readonly<Record<string, ComposeService>>;
}

/** Reads a repository file relative to the workspace root. */
async function readRepoFile(path: string): Promise<string> {
  return readFile(resolve(repoRoot, path), "utf8");
}

/** Parses the root Compose file into the small service shape used by these checks. */
async function readRootCompose(): Promise<ComposeFile> {
  const source = await readRepoFile("docker-compose.yml");
  const parsed: unknown = parse(source);
  if (!isComposeFile(parsed)) {
    throw new Error("docker-compose.yml did not parse to the expected Compose object");
  }
  return parsed;
}

/** Narrows parsed YAML to the service map needed by the deployment contract tests. */
function isComposeFile(value: unknown): value is ComposeFile {
  if (!isRecord(value)) {
    return false;
  }
  const services = value.services;
  return services === undefined || isRecord(services);
}

/** Narrows unknown values to readonly records. */
function isRecord(value: unknown): value is Readonly<Record<string, unknown>> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

/** Returns a required Compose service or throws a readable test failure. */
function getService(compose: ComposeFile, name: string): ComposeService {
  const service = compose.services?.[name];
  if (service === undefined) {
    throw new Error(`Missing ${name} service in docker-compose.yml`);
  }
  return service;
}

/** Reads a required environment value from a Compose service. */
function getEnvironmentValue(service: ComposeService, name: string): string {
  const value = service.environment?.[name];
  if (value === undefined) {
    throw new Error(`Missing ${name} environment value`);
  }
  return value;
}

/** Secret-like example values must be blank, placeholders, local test values, or op references. */
function isPlaceholderSecretValue(value: string): boolean {
  return (
    value.length === 0 ||
    value.startsWith("replace-with-") ||
    value.startsWith("PASTE_") ||
    value === "local" ||
    value.startsWith("op://")
  );
}

describe("deployment hardening contract", () => {
  it("binds the Postgres host port to loopback", async () => {
    const compose = await readRootCompose();
    const postgres = getService(compose, "postgres");

    expect(postgres.ports).toContain(postgresPortBinding);
  });

  it("binds the app host port to loopback", async () => {
    const compose = await readRootCompose();
    const app = getService(compose, "app");

    expect(app.ports).toContain(appPortBinding);
  });

  it("requires explicit Postgres credentials without an insecure default", async () => {
    const compose = await readRootCompose();
    const postgres = getService(compose, "postgres");

    expect(getEnvironmentValue(postgres, "POSTGRES_DB")).toBe(postgresDbDefault);
    expect(getEnvironmentValue(postgres, "POSTGRES_USER")).toBe(postgresUserDefault);
    expect(getEnvironmentValue(postgres, "POSTGRES_PASSWORD")).toBe(postgresPasswordRequirement);
    expect(postgres.healthcheck?.test?.join(" ")).toContain("$${POSTGRES_USER}");
    expect(postgres.healthcheck?.test?.join(" ")).toContain("$${POSTGRES_DB}");
  });

  it("requires an explicit app DATABASE_URL instead of the weak Compose URL", async () => {
    const compose = await readRootCompose();
    const app = getService(compose, "app");

    expect(getEnvironmentValue(app, "DATABASE_URL")).toBe(databaseUrlRequirement);
  });

  it("keeps Drizzle from silently using the weak database credential", async () => {
    const source = await readRepoFile("apps/tether/drizzle.config.ts");

    expect(source).not.toContain("postgres://tether:tether@localhost:54329/tether");
    expect(source).toContain("DATABASE_URL");
    expect(source).toContain("DATABASE_URL is required");
  });

  it("keeps env examples placeholders-only for secret-like fields", async () => {
    const envExamplePaths = [".env.example"] as const;

    for (const path of envExamplePaths) {
      const source = await readRepoFile(path);
      const secretLikeEntries = source
        .split("\n")
        .filter((line) => !line.startsWith("#"))
        .map((line) => line.split("=", 2))
        .filter(([name]) => name !== undefined && /(?:SECRET|TOKEN|PASSWORD|API_KEY)$/u.test(name));

      for (const [name, value = ""] of secretLikeEntries) {
        expect(isPlaceholderSecretValue(value), `${path} ${name ?? "unknown"}`).toBe(true);
      }
    }
  });

  it("keeps .gitignore env protections and declared safe exceptions", async () => {
    const lines = (await readRepoFile(".gitignore")).split("\n");

    expect(lines).toContain(".env");
    expect(lines).toContain(".env.*");
    expect(lines).not.toContain("!.env.op");
    expect(lines).toContain("!**/.env.example");
  });

  it("runs maintained secret scanning in CI on pushes and pull requests", async () => {
    const workflow = await readRepoFile(".github/workflows/ci.yml");

    expect(workflow).toContain("pull_request:");
    expect(workflow).toContain("branches: [main]");
    expect(workflow).toContain("gitleaks/gitleaks-action@v3");
    expect(workflow).toContain("fetch-depth: 0");
    expect(workflow).toContain(githubTokenExpression);
  });
});
