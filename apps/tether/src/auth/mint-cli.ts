import { pathToFileURL } from "node:url";

import { z } from "zod";

import { mintAuthToken, type AuthRole, authRoles } from "./token.js";

const defaultTokenTtlSeconds = 30 * 24 * 60 * 60;
const defaultSigningKid = "default";

export interface MintCliEnvironment {
  readonly AUTH_SIGNING_KID?: string | undefined;
  readonly AUTH_SIGNING_SECRET?: string | undefined;
}

export interface MintCliOptions {
  readonly kid: string;
  readonly participantId: string;
  readonly role: AuthRole;
  readonly secret: string;
  readonly sessionId: string;
  readonly ttlSeconds: number;
}

const cliRoleSchema = z.enum(authRoles);

/** Parses `tether-mint` arguments and environment into validated mint options. */
export function parseMintCliOptions(
  args: readonly string[],
  env: MintCliEnvironment = process.env,
): MintCliOptions {
  const values = parseFlagValues(args);
  const participantId = readRequiredFlag(values, "participant");
  const sessionId = readRequiredFlag(values, "session");
  const role = cliRoleSchema.parse(readRequiredFlag(values, "role"));
  const ttlSeconds = parseTtlSeconds(values.get("ttl") ?? String(defaultTokenTtlSeconds));
  const secret = env.AUTH_SIGNING_SECRET?.trim();
  if (!secret) {
    throw new Error("AUTH_SIGNING_SECRET is required to mint a token");
  }
  return {
    kid: values.get("kid")?.trim() || env.AUTH_SIGNING_KID?.trim() || defaultSigningKid,
    participantId,
    role,
    secret,
    sessionId,
    ttlSeconds,
  };
}

/** Mints a token for parsed CLI arguments using the provided clock. */
export function mintAuthTokenFromCli(
  args: readonly string[],
  env: MintCliEnvironment = process.env,
  now: Date = new Date(),
): string {
  const options = parseMintCliOptions(args, env);
  const exp = Math.floor(now.getTime() / 1_000) + options.ttlSeconds;
  return mintAuthToken(
    {
      exp,
      kid: options.kid,
      participantId: options.participantId,
      role: options.role,
      sessionId: options.sessionId,
    },
    { [options.kid]: options.secret },
  );
}

/** CLI entry point for operator token minting. */
async function main(): Promise<void> {
  try {
    process.stdout.write(`${mintAuthTokenFromCli(process.argv.slice(2))}\n`);
  } catch (error) {
    process.stderr.write(`${error instanceof Error ? error.message : "Failed to mint token"}\n`);
    process.exitCode = 1;
  }
}

/** Parses `--name value` pairs into a map and rejects unknown positional args. */
function parseFlagValues(args: readonly string[]): ReadonlyMap<string, string> {
  const values = new Map<string, string>();
  for (let index = 0; index < args.length; index += 2) {
    const flag = args[index];
    const value = args[index + 1];
    if (!flag?.startsWith("--") || value === undefined || value.startsWith("--")) {
      throw new Error("Expected --participant, --session, --role, and optional --ttl/--kid");
    }
    values.set(flag.slice(2), value);
  }
  return values;
}

/** Reads a required CLI flag value from the parsed map. */
function readRequiredFlag(values: ReadonlyMap<string, string>, name: string): string {
  const value = values.get(name)?.trim();
  if (!value) {
    throw new Error(`--${name} is required`);
  }
  return value;
}

/** Parses TTL values as seconds, with optional h/d suffixes for operator use. */
function parseTtlSeconds(value: string): number {
  const match = value.match(/^(\d+)([dh])?$/u);
  if (!match?.[1]) {
    throw new Error("--ttl must be seconds, or use h/d suffix");
  }
  const amount = Number.parseInt(match[1], 10);
  const suffix = match[2] ?? "";
  const multiplier = suffix === "d" ? 86_400 : suffix === "h" ? 3_600 : 1;
  const ttlSeconds = amount * multiplier;
  if (!Number.isSafeInteger(ttlSeconds) || ttlSeconds <= 0) {
    throw new Error("--ttl must be a positive safe integer duration");
  }
  return ttlSeconds;
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  void main();
}
