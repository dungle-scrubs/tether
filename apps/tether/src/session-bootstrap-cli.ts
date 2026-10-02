import { z } from "zod";

import {
  boundedCliErrorCode,
  isCliEntrypoint,
  parseCliFlagValues,
  readRequiredCliValue,
  runCliEntrypoint,
} from "./cli-flags.js";
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
const sessionBootstrapCliFlags = ["--identity", "--session"] as const;
const sessionBootstrapCliErrorPrefix = "session_bootstrap";

/** Parses the exact loopback deployment bootstrap contract. */
export function parseSessionBootstrapCliOptions(
  args: readonly string[],
  env: SessionBootstrapCliEnvironment = process.env,
): SessionBootstrapCliOptions {
  const values = parseCliFlagValues(args, {
    allowedFlags: sessionBootstrapCliFlags,
    invalidMessage: "session_bootstrap_arguments_invalid",
  });
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
  return boundedCliErrorCode(error, sessionBootstrapCliErrorPrefix);
}

/** Reads one required nonempty CLI or environment value. */
function readRequired(value: string | undefined, name: string): string {
  return readRequiredCliValue(value, name, sessionBootstrapCliErrorPrefix);
}

if (isCliEntrypoint(import.meta.url)) {
  void runCliEntrypoint(runSessionBootstrapCli, projectSessionBootstrapCliError);
}
