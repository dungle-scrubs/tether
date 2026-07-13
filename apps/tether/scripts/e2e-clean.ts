import { spawn as nodeSpawn } from "node:child_process";

import { cleanExpired, type SpawnResult } from "./e2e-harness.js";

/** Default expiry window for abandoned E2E resources. */
const DEFAULT_TTL_MINUTES = 120;

/** Runs a docker command and captures stdout/stderr, resolving with its status. */
function capture(command: string, args: readonly string[]): Promise<SpawnResult> {
  const child = nodeSpawn(command, [...args], {
    env: process.env,
    stdio: ["inherit", "pipe", "inherit"],
  });
  let stdout = "";
  if (child.stdout !== null) {
    child.stdout.setEncoding("utf8");
    child.stdout.on("data", (chunk: string) => {
      stdout += chunk;
    });
  }
  return new Promise<SpawnResult>((resolve) => {
    child.on("error", (error) => {
      process.stderr.write(`${command} failed to start: ${error.message}\n`);
      resolve({ status: 1, signal: null, stdout });
    });
    child.on("close", (status, signal) => {
      resolve({ status, signal, stdout });
    });
  });
}

/** Parses `--apply` and `--ttl-minutes N` from argv. */
function parseArgs(argv: readonly string[]): { apply: boolean; ttlMinutes: number } {
  let apply = false;
  let ttlMinutes = DEFAULT_TTL_MINUTES;
  for (let index = 0; index < argv.length; index += 1) {
    const arg = argv[index];
    if (arg === "--apply") {
      apply = true;
    } else if (arg === "--ttl-minutes") {
      const value = Number(argv[index + 1]);
      if (!Number.isFinite(value) || value < 0) {
        throw new Error("--ttl-minutes requires a non-negative number");
      }
      ttlMinutes = value;
      index += 1;
    }
  }
  return { apply, ttlMinutes };
}

const { apply, ttlMinutes } = parseArgs(process.argv.slice(2));

const result = await cleanExpired({
  capture,
  log: (message) => {
    process.stdout.write(`${message}\n`);
  },
  now: () => Date.now(),
  ttlMs: ttlMinutes * 60_000,
  apply,
});

process.exit(result.exitCode);
