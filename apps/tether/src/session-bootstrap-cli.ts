import { pathToFileURL } from "node:url";

import { z } from "zod";

import {
  createPool,
  ensureBootstrapSession,
  migrate,
  SessionBootstrapIdentityConflictError,
} from "./db.js";

/** Narrow environment accepted by the host-local session bootstrap command. */
export interface SessionBootstrapCliEnvironment {
  readonly DATABASE_URL?: string | undefined;
}

/** Parsed durable bootstrap identity operation. */
export interface SessionBootstrapCliOptions {
  readonly databaseUrl: string;
  readonly identityKey: string;
  readonly sessionId: string;
}

const sessionIdSchema = z.string().trim().min(1).max(255);

/** Parses the exact loopback deployment bootstrap contract. */
export function parseSessionBootstrapCliOptions(
  args: readonly string[],
  env: SessionBootstrapCliEnvironment = process.env,
): SessionBootstrapCliOptions {
  const values = parseFlagValues(args);
  if (values.size !== 2) throw new Error("session_bootstrap_arguments_invalid");
  const identityKey = readRequired(values.get("identity"), "identity");
  if (Buffer.byteLength(identityKey, "utf8") > 512) {
    throw new Error("session_bootstrap_identity_invalid");
  }
  return {
    databaseUrl: readRequired(env.DATABASE_URL, "database_url"),
    identityKey,
    sessionId: sessionIdSchema.parse(readRequired(values.get("session"), "session")),
  };
}

/** Migrates and ensures one durable deployment session without remote authority. */
export async function executeSessionBootstrapCli(
  options: SessionBootstrapCliOptions,
): Promise<string> {
  const database = createPool(options.databaseUrl, { max: 1 });
  try {
    await migrate(database);
    const result = await ensureBootstrapSession(database, options);
    return JSON.stringify(result);
  } finally {
    await database.end();
  }
}

/** Runs the host-local bootstrap command with injectable streams. */
export async function runSessionBootstrapCli(
  args: readonly string[] = process.argv.slice(2),
  env: SessionBootstrapCliEnvironment = process.env,
  writeOutput: (value: string) => void = (value) => process.stdout.write(value),
): Promise<void> {
  const output = await executeSessionBootstrapCli(parseSessionBootstrapCliOptions(args, env));
  writeOutput(`${output}\n`);
}

/** Projects all command failures to bounded diagnostics without database details. */
export function projectSessionBootstrapCliError(error: unknown): string {
  if (error instanceof SessionBootstrapIdentityConflictError) return error.code;
  return error instanceof Error && /^session_bootstrap_[a-z0-9_]+$/u.test(error.message)
    ? error.message
    : "session_bootstrap_failed";
}

/** Parses strict `--identity value --session value` arguments. */
function parseFlagValues(args: readonly string[]): ReadonlyMap<string, string> {
  const values = new Map<string, string>();
  for (let index = 0; index < args.length; index += 2) {
    const flag = args[index];
    const value = args[index + 1];
    if (
      (flag !== "--identity" && flag !== "--session") ||
      value === undefined ||
      value.startsWith("--") ||
      values.has(flag.slice(2))
    ) {
      throw new Error("session_bootstrap_arguments_invalid");
    }
    values.set(flag.slice(2), value);
  }
  return values;
}

/** Reads one required nonempty CLI or environment value. */
function readRequired(value: string | undefined, name: string): string {
  const trimmed = value?.trim();
  if (!trimmed) throw new Error(`session_bootstrap_${name}_required`);
  return trimmed;
}

async function main(): Promise<void> {
  try {
    await runSessionBootstrapCli();
  } catch (error) {
    process.stderr.write(`${projectSessionBootstrapCliError(error)}\n`);
    process.exitCode = 1;
  }
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  void main();
}
