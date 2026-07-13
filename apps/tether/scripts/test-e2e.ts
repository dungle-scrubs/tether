import { spawn as nodeSpawn } from "node:child_process";
import { randomUUID } from "node:crypto";
import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { stringify as stringifyYaml } from "yaml";

import {
  type ChildProcess,
  type ResourceLabels,
  runE2E,
  type SpawnOptions,
  type SpawnResult,
  type TerminationSignal,
} from "./e2e-harness.js";

/** Sanitizes the parent environment into a defined string map. */
function currentEnv(): Record<string, string> {
  const env: Record<string, string> = {};
  for (const [key, value] of Object.entries(process.env)) {
    if (value !== undefined) {
      env[key] = value;
    }
  }
  return env;
}

/** Spawns a child process, streaming or capturing stdout per options. */
function spawn(command: string, args: readonly string[], options: SpawnOptions): ChildProcess {
  const capture = options.capture === true;
  const child = nodeSpawn(command, [...args], {
    env: options.env,
    stdio: capture ? ["inherit", "pipe", "inherit"] : "inherit",
  });

  let stdout = "";
  if (capture && child.stdout !== null) {
    child.stdout.setEncoding("utf8");
    child.stdout.on("data", (chunk: string) => {
      stdout += chunk;
    });
  }

  const result = new Promise<SpawnResult>((resolve) => {
    child.on("error", (error) => {
      process.stderr.write(`${command} failed to start: ${error.message}\n`);
      resolve({ status: 1, signal: null, stdout });
    });
    child.on("close", (status, signal) => {
      resolve({ status, signal, stdout });
    });
  });

  return {
    kill: (signal) => {
      child.kill(signal);
    },
    result,
  };
}

const overrideDir = mkdtempSync(join(tmpdir(), "tether-e2e-"));

/** Writes a compose override that stamps E2E labels onto containers and volumes. */
function writeComposeOverride(labels: ResourceLabels): string {
  const overridePath = join(overrideDir, "labels.compose.yml");
  const document = {
    services: {
      postgres: { labels },
    },
    volumes: {
      "tether-postgres": { labels },
    },
  };
  writeFileSync(overridePath, stringifyYaml(document), "utf8");
  return overridePath;
}

function removeComposeOverride(path: string): void {
  rmSync(path, { force: true });
}

function registerSignalHandler(handler: (signal: TerminationSignal) => void): void {
  process.on("SIGINT", () => {
    handler("SIGINT");
  });
  process.on("SIGTERM", () => {
    handler("SIGTERM");
  });
}

const exitCode = await runE2E({
  spawn,
  env: currentEnv(),
  registerSignalHandler,
  writeComposeOverride,
  removeComposeOverride,
  log: (message) => {
    process.stderr.write(`${message}\n`);
  },
  now: () => Date.now(),
  runId: randomUUID(),
  composeFile: "../../docker-compose.yml",
  databasePassword: "e2e-local-postgres-password",
});

rmSync(overrideDir, { force: true, recursive: true });
process.exit(exitCode);
