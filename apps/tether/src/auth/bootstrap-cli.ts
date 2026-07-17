import { pathToFileURL } from "node:url";

import { z } from "zod";

import { createPool, migrate } from "../db.js";
import { createAuthPersistenceStores } from "./db-grant-stores.js";
import { createAuthGrantLifecycle, maximumAuthGrantLifetimeSeconds } from "./grant-lifecycle.js";

const defaultSigningKid = "default";
const defaultTtlSeconds = 24 * 60 * 60;

export interface BootstrapAdminOptions {
  /** Postgres URL whose schema and grant authority will be updated. */
  readonly databaseUrl: string;
  /** Durable issuer embedded in the new grant. */
  readonly issuer: string;
  /** Explicit confirmation that deployed replicas can verify tgr2 grants. */
  readonly issuanceEnabled: boolean;
  /** Signing key id. */
  readonly kid: string;
  /** Signing secret kept only in memory. */
  readonly secret: string;
  /** Recovery administrator identity. */
  readonly subject: string;
  /** Grant lifetime in seconds. */
  readonly ttlSeconds: number;
}

/** Narrow environment accepted by the bootstrap command. */
export interface BootstrapAdminEnvironment {
  /** Durable grant issuer. */
  readonly AUTH_ISSUER?: string | undefined;
  /** Must be exactly `true` before bootstrap can issue a tgr2 bearer. */
  readonly AUTH_GRANT_BOOTSTRAP_COMPATIBILITY_CONFIRMED?: string | undefined;
  /** Optional signing key id. */
  readonly AUTH_SIGNING_KID?: string | undefined;
  /** Required active signing secret. */
  readonly AUTH_SIGNING_SECRET?: string | undefined;
  /** Required target Postgres URL. */
  readonly DATABASE_URL?: string | undefined;
}

/** Parses the narrow database-aware bootstrap contract without loading server-only config. */
export function parseBootstrapAdminOptions(
  args: readonly string[],
  env: BootstrapAdminEnvironment = process.env,
): BootstrapAdminOptions {
  const values = parseFlagValues(args);
  return {
    databaseUrl: readRequiredEnvironment(env.DATABASE_URL, "DATABASE_URL"),
    issuer: z
      .string()
      .min(1)
      .max(512)
      .parse(readRequiredEnvironment(env.AUTH_ISSUER, "AUTH_ISSUER")),
    issuanceEnabled: env.AUTH_GRANT_BOOTSTRAP_COMPATIBILITY_CONFIRMED === "true",
    kid: env.AUTH_SIGNING_KID?.trim() || defaultSigningKid,
    secret: readRequiredEnvironment(env.AUTH_SIGNING_SECRET, "AUTH_SIGNING_SECRET"),
    subject: readRequiredFlag(values, "subject"),
    ttlSeconds: parseTtlSeconds(values.get("ttl") ?? String(defaultTtlSeconds)),
  };
}

/** Migrates the target database and creates one audited recovery administrator grant. */
export async function bootstrapAdminGrant(
  options: BootstrapAdminOptions,
  onCommitted: (output: string) => void = () => undefined,
): Promise<string> {
  const database = createPool(options.databaseUrl, { max: 1 });
  return completeBootstrapOneTimeSecret({
    cleanup: () => database.end(),
    issue: async () => {
      await migrate(database);
      const lifecycle = createAuthGrantLifecycle({
        activeKid: options.kid,
        issuer: options.issuer,
        issuanceEnabled: options.issuanceEnabled,
        secrets: { [options.kid]: options.secret },
        stores: createAuthPersistenceStores(database),
      });
      return JSON.stringify(
        await lifecycle.create({
          actorSubject: "bootstrap-cli",
          reasonCode: "bootstrap",
          role: "admin",
          sessionScope: "*",
          source: "bootstrap",
          subject: options.subject,
          ttlSeconds: options.ttlSeconds,
        }),
      );
    },
    onCommitted,
  });
}

/** Runs the CLI with injectable streams while writing the bearer exactly once. */
export async function runBootstrapAdminCli(
  args: readonly string[] = process.argv.slice(2),
  env: BootstrapAdminEnvironment = process.env,
  writeOutput: (value: string) => void = (value) => process.stdout.write(value),
): Promise<void> {
  await bootstrapAdminGrant(parseBootstrapAdminOptions(args, env), (output) =>
    writeOutput(`${output}\n`),
  );
}

/** Emits a committed one-time secret before cleanup and preserves it across cleanup failure. */
export async function completeBootstrapOneTimeSecret(input: {
  readonly cleanup: () => Promise<void>;
  readonly issue: () => Promise<string>;
  readonly onCommitted: (output: string) => void;
}): Promise<string> {
  let output: string;
  try {
    output = await input.issue();
  } catch (error) {
    await ignoreCleanupFailure(input.cleanup);
    throw error;
  }
  try {
    input.onCommitted(output);
  } catch (error) {
    await ignoreCleanupFailure(input.cleanup);
    throw error;
  }
  await ignoreCleanupFailure(input.cleanup);
  return output;
}

async function ignoreCleanupFailure(cleanup: () => Promise<void>): Promise<void> {
  try {
    await cleanup();
  } catch {
    // Durable authority and its one-time output already committed.
  }
}

async function main(): Promise<void> {
  try {
    await runBootstrapAdminCli();
  } catch (error) {
    process.stderr.write(`${boundedErrorCode(error)}\n`);
    process.exitCode = 1;
  }
}

function parseFlagValues(args: readonly string[]): ReadonlyMap<string, string> {
  const values = new Map<string, string>();
  for (let index = 0; index < args.length; index += 2) {
    const flag = args[index];
    const value = args[index + 1];
    if (flag !== "--subject" && flag !== "--ttl")
      throw new Error("auth_bootstrap_invalid_arguments");
    if (value === undefined || value.startsWith("--"))
      throw new Error("auth_bootstrap_invalid_arguments");
    values.set(flag.slice(2), value);
  }
  return values;
}

function readRequiredFlag(values: ReadonlyMap<string, string>, name: string): string {
  const value = values.get(name)?.trim();
  if (!value) throw new Error(`auth_bootstrap_${name}_required`);
  return z.string().min(1).max(255).parse(value);
}

function readRequiredEnvironment(value: string | undefined, name: string): string {
  if (!value?.trim()) throw new Error(`auth_bootstrap_${name.toLowerCase()}_required`);
  return value.trim();
}

function parseTtlSeconds(value: string): number {
  const match = value.match(/^(\d+)([dh])?$/u);
  if (!match?.[1]) throw new Error("auth_bootstrap_ttl_invalid");
  const multiplier = match[2] === "d" ? 86_400 : match[2] === "h" ? 3_600 : 1;
  const ttlSeconds = Number.parseInt(match[1], 10) * multiplier;
  if (
    !Number.isSafeInteger(ttlSeconds) ||
    ttlSeconds <= 0 ||
    ttlSeconds > maximumAuthGrantLifetimeSeconds
  ) {
    throw new Error("auth_bootstrap_ttl_invalid");
  }
  return ttlSeconds;
}

function boundedErrorCode(error: unknown): string {
  return error instanceof Error && /^auth_[a-z0-9_]+$/u.test(error.message)
    ? error.message
    : "auth_bootstrap_failed";
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  void main();
}
