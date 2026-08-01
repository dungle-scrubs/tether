import { pathToFileURL } from "node:url";

import {
  browserPairingNonceSchema,
  createBrowserPairingRequestSchema,
  operatorGrantScopeSchema,
  publicBrowserPairingRequestSchema,
} from "@dungle-scrubs/tether-protocol";
import { z } from "zod";

import { createPool, migrate } from "../db.js";
import { createBrowserPairingRequest, toPublicPairingRequest } from "./browser-pairing.js";
import { createBrowserPairingStore } from "./browser-pairing-stores.js";

/** Narrow environment accepted by the loopback browser-pairing command. */
export interface BrowserPairingCliEnvironment {
  readonly DATABASE_URL?: string | undefined;
}

/** Parsed loopback admin operation. */
export type BrowserPairingCliOptions =
  | {
      readonly command: "create";
      readonly databaseUrl: string;
      readonly operatorSubject: string;
      readonly origin: string;
      readonly publicNonce: string;
      readonly requestedScope: ReturnType<typeof operatorGrantScopeSchema.parse>;
    }
  | {
      readonly command: "inspect";
      readonly databaseUrl: string;
      readonly requestId: string;
    }
  | {
      readonly actorSubject: string;
      readonly command: "confirm";
      readonly databaseUrl: string;
      readonly requestId: string;
      readonly verificationPhrase: string;
    };

const requestIdSchema = publicBrowserPairingRequestSchema.shape.requestId;
const actorSubjectSchema = z.string().min(1).max(255);
const operatorSubjectSchema = createBrowserPairingRequestSchema.shape.operatorSubject;
const verificationPhraseSchema = publicBrowserPairingRequestSchema.shape.verificationPhrase;
const originSchema = z
  .string()
  .max(512)
  .transform((value, context) => {
    try {
      const parsed = new URL(value);
      if (
        (parsed.protocol !== "http:" && parsed.protocol !== "https:") ||
        parsed.origin !== value ||
        parsed.username !== "" ||
        parsed.password !== ""
      ) {
        context.addIssue({ code: "custom", message: "Expected an exact HTTP origin" });
        return z.NEVER;
      }
      return parsed.origin;
    } catch {
      context.addIssue({ code: "custom", message: "Expected an exact HTTP origin" });
      return z.NEVER;
    }
  });

/** Parses one explicit inspect or confirm operation without loading server auth secrets. */
export function parseBrowserPairingCliOptions(
  args: readonly string[],
  env: BrowserPairingCliEnvironment = process.env,
): BrowserPairingCliOptions {
  const [command, ...flagArgs] = args;
  if (command !== "confirm" && command !== "create" && command !== "inspect") {
    throw new Error("browser_pairing_command_invalid");
  }
  const values = parseFlagValues(flagArgs);
  const databaseUrl = readRequiredValue(env.DATABASE_URL, "database_url");
  if (command === "create") {
    if (values.size !== 4) throw new Error("browser_pairing_arguments_invalid");
    return {
      command,
      databaseUrl,
      operatorSubject: operatorSubjectSchema.parse(
        readRequiredValue(values.get("operator"), "operator"),
      ),
      origin: originSchema.parse(readRequiredValue(values.get("origin"), "origin")),
      publicNonce: browserPairingNonceSchema.parse(readRequiredValue(values.get("nonce"), "nonce")),
      requestedScope: operatorGrantScopeSchema.parse(
        JSON.parse(readRequiredValue(values.get("scope"), "scope")) as unknown,
      ),
    };
  }
  const requestId = requestIdSchema.parse(readRequiredValue(values.get("request"), "request"));
  if (command === "inspect") {
    if (values.size !== 1) throw new Error("browser_pairing_arguments_invalid");
    return { command, databaseUrl, requestId };
  }
  if (values.size !== 3) throw new Error("browser_pairing_arguments_invalid");
  return {
    actorSubject: actorSubjectSchema.parse(readRequiredValue(values.get("actor"), "actor")),
    command,
    databaseUrl,
    requestId,
    verificationPhrase: verificationPhraseSchema.parse(
      readRequiredValue(values.get("phrase"), "phrase"),
    ),
  };
}

/** Executes one host-local pairing inspection or explicit confirmation. */
export async function executeBrowserPairingCli(options: BrowserPairingCliOptions): Promise<string> {
  const database = createPool(options.databaseUrl, { max: 1 });
  try {
    await migrate(database);
    const store = createBrowserPairingStore(database);
    if (options.command === "create") {
      return JSON.stringify(
        await createBrowserPairingRequest(
          { store },
          {
            operatorSubject: options.operatorSubject,
            origin: options.origin,
            publicNonce: options.publicNonce,
            requestedScope: options.requestedScope,
            sourceAddress: null,
          },
        ),
      );
    }
    const request = await store.inspect(options.requestId);
    if (request === null) throw new Error("browser_pairing_not_found");
    if (options.command === "inspect") {
      return JSON.stringify(toPublicPairingRequest(request));
    }
    if (request.verificationPhrase !== options.verificationPhrase) {
      throw new Error("browser_pairing_phrase_mismatch");
    }
    const result = await store.confirm({
      actorSubject: options.actorSubject,
      confirmedAt: new Date(),
      requestId: options.requestId,
    });
    if (result.status !== "confirmed" && result.status !== "already_confirmed") {
      throw new Error(`browser_pairing_${result.status}`);
    }
    return JSON.stringify({
      request: toPublicPairingRequest(result.request),
      status: result.status,
    });
  } finally {
    await database.end();
  }
}

/** Runs the host-local CLI with injectable arguments and output. */
export async function runBrowserPairingCli(
  args: readonly string[] = process.argv.slice(2),
  env: BrowserPairingCliEnvironment = process.env,
  writeOutput: (value: string) => void = (value) => process.stdout.write(value),
): Promise<void> {
  const output = await executeBrowserPairingCli(parseBrowserPairingCliOptions(args, env));
  writeOutput(`${output}\n`);
}

/** Parses strict `--name value` arguments. */
function parseFlagValues(args: readonly string[]): ReadonlyMap<string, string> {
  const values = new Map<string, string>();
  for (let index = 0; index < args.length; index += 2) {
    const flag = args[index];
    const value = args[index + 1];
    if (
      (flag !== "--actor" &&
        flag !== "--nonce" &&
        flag !== "--operator" &&
        flag !== "--origin" &&
        flag !== "--phrase" &&
        flag !== "--request" &&
        flag !== "--scope") ||
      value === undefined ||
      value.startsWith("--") ||
      values.has(flag.slice(2))
    ) {
      throw new Error("browser_pairing_arguments_invalid");
    }
    values.set(flag.slice(2), value);
  }
  return values;
}

/** Reads one required nonempty CLI or environment value. */
function readRequiredValue(value: string | undefined, name: string): string {
  const trimmed = value?.trim();
  if (!trimmed) throw new Error(`browser_pairing_${name}_required`);
  return trimmed;
}

/** Collapses unexpected details into a bounded CLI error code. */
function boundedErrorCode(error: unknown): string {
  return error instanceof Error && /^browser_pairing_[a-z0-9_]+$/u.test(error.message)
    ? error.message
    : "browser_pairing_failed";
}

async function main(): Promise<void> {
  try {
    await runBrowserPairingCli();
  } catch (error) {
    process.stderr.write(`${boundedErrorCode(error)}\n`);
    process.exitCode = 1;
  }
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  void main();
}
