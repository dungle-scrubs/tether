/**
 * Owns the strict `--name value` argument contract shared by every host-local
 * Tether CLI, plus the bounded error projection and entry-point guard those
 * commands repeat. It does not know what any individual flag means.
 */

import { pathToFileURL } from "node:url";

/** Parses strict `--name value` pairs, rejecting unknown, empty, and repeated flags. */
export function parseCliFlagValues(
  args: readonly string[],
  options: { readonly allowedFlags: readonly string[]; readonly invalidMessage: string },
): ReadonlyMap<string, string> {
  const values = new Map<string, string>();
  for (let index = 0; index < args.length; index += 2) {
    const flag = args[index];
    const value = args[index + 1];
    if (
      flag === undefined ||
      !options.allowedFlags.includes(flag) ||
      value === undefined ||
      value.startsWith("--") ||
      values.has(flag.slice(2))
    ) {
      throw new Error(options.invalidMessage);
    }
    values.set(flag.slice(2), value);
  }
  return values;
}

/** Reads one required nonempty CLI or environment value under a command's error prefix. */
export function readRequiredCliValue(
  value: string | undefined,
  name: string,
  errorPrefix: string,
): string {
  const trimmed = value?.trim();
  if (!trimmed) throw new Error(`${errorPrefix}_${name}_required`);
  return trimmed;
}

/** Collapses unexpected failure details into one bounded command error code. */
export function boundedCliErrorCode(error: unknown, errorPrefix: string): string {
  const pattern = new RegExp(`^${errorPrefix}_[a-z0-9_]+$`, "u");
  return error instanceof Error && pattern.test(error.message)
    ? error.message
    : `${errorPrefix}_failed`;
}

/** Returns whether the current module was started directly as a command. */
export function isCliEntrypoint(importMetaUrl: string): boolean {
  const entry = process.argv[1];
  return entry !== undefined && importMetaUrl === pathToFileURL(entry).href;
}

/** Runs one command entry point, reporting only bounded diagnostics on failure. */
export async function runCliEntrypoint(
  run: () => Promise<void>,
  projectError: (error: unknown) => string,
): Promise<void> {
  try {
    await run();
  } catch (error) {
    process.stderr.write(`${projectError(error)}\n`);
    process.exitCode = 1;
  }
}
