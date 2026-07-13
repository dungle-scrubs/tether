import { constants as osConstants } from "node:os";

/**
 * Signal-safe E2E orchestration harness.
 *
 * All process and Docker interaction is injected through the dependency
 * interfaces below so the pure lifecycle logic (label building, expiry
 * selection, and exit-status preservation) can be unit tested without a
 * container runtime. The real wiring lives in `test-e2e.ts` and `e2e-clean.ts`.
 */

/** Termination signals the runner forwards to its children. */
export type TerminationSignal = "SIGINT" | "SIGTERM";

/** Docker label keys applied to every Tether E2E resource. */
export const E2E_LABEL = "com.tether.e2e";
export const E2E_RUN_ID_LABEL = "com.tether.e2e.run-id";
export const E2E_CREATED_AT_LABEL = "com.tether.e2e.created-at";

/** Label map applied to E2E containers and volumes. */
export interface ResourceLabels {
  [E2E_LABEL]: "true";
  [E2E_RUN_ID_LABEL]: string;
  [E2E_CREATED_AT_LABEL]: string;
}

/**
 * Builds the label set that scopes an E2E run: a fixed marker, the unique run
 * ID, and the creation timestamp used later for expiry selection.
 */
export function buildResourceLabels(runId: string, createdAtMs: number): ResourceLabels {
  return {
    [E2E_LABEL]: "true",
    [E2E_RUN_ID_LABEL]: runId,
    [E2E_CREATED_AT_LABEL]: String(createdAtMs),
  };
}

/** A labeled Docker resource discovered during expired-resource cleanup. */
export interface LabeledResource {
  /** Creation timestamp in epoch milliseconds, or null when unparseable. */
  createdAt: number | null;
  runId: string;
  name: string;
}

/**
 * Parses `created-at|run-id|name` lines emitted by `docker ps`/`docker volume
 * ls` Go templates. Blank lines are ignored. A missing or non-numeric
 * timestamp yields `createdAt: null` so the resource is never treated as
 * expired by default.
 */
export function parseDockerLabelLines(output: string): LabeledResource[] {
  const resources: LabeledResource[] = [];
  for (const rawLine of output.split("\n")) {
    const line = rawLine.trim();
    if (line === "") {
      continue;
    }
    const parts = line.split("|");
    const createdAtRaw = (parts[0] ?? "").trim();
    const runId = (parts[1] ?? "").trim();
    const name = (parts[2] ?? "").trim();
    if (name === "") {
      continue;
    }
    const createdAtNum = Number(createdAtRaw);
    const createdAt = createdAtRaw !== "" && Number.isFinite(createdAtNum) ? createdAtNum : null;
    resources.push({ createdAt, runId, name });
  }
  return resources;
}

/**
 * Selects resources whose creation time is at least `ttlMs` in the past.
 * Resources with an unknown creation time are never selected, so a missing or
 * malformed label can never cause removal.
 */
export function selectExpired(
  resources: readonly LabeledResource[],
  nowMs: number,
  ttlMs: number,
): LabeledResource[] {
  const cutoff = nowMs - ttlMs;
  return resources.filter(
    (resource) => resource.createdAt !== null && resource.createdAt <= cutoff,
  );
}

/**
 * Preserves the original test outcome through cleanup: a nonzero test status
 * always wins, and a passing run surfaces any cleanup failure.
 */
export function resolveExitStatus(testStatus: number, cleanupStatus: number): number {
  return testStatus === 0 ? cleanupStatus : testStatus;
}

/** Conventional shell exit status (128 + signal number) for a termination signal. */
export function signalExitStatus(signal: TerminationSignal): number {
  const number = osConstants.signals[signal];
  return 128 + number;
}

/** Result of a completed child process. */
export interface SpawnResult {
  status: number | null;
  signal: NodeJS.Signals | null;
  stdout: string;
}

/** A running child process the orchestrator can await and signal. */
export interface ChildProcess {
  kill(signal: NodeJS.Signals): void;
  readonly result: Promise<SpawnResult>;
}

/** Options for spawning a child. */
export interface SpawnOptions {
  env: Record<string, string>;
  /** When true, capture stdout instead of inheriting the parent stream. */
  capture?: boolean;
}

/** Spawns a child process. */
export type Spawn = (
  command: string,
  args: readonly string[],
  options: SpawnOptions,
) => ChildProcess;

/** Maps a completed child result to a shell exit status. */
export function exitStatusFromResult(result: SpawnResult): number {
  if (result.status !== null) {
    return result.status;
  }
  if (result.signal !== null) {
    const number = osConstants.signals[result.signal];
    return typeof number === "number" ? 128 + number : 1;
  }
  return 1;
}

/**
 * Extracts the published host port from a `docker compose port` address such as
 * `127.0.0.1:54321`.
 */
export function parsePublishedPort(address: string): string {
  const trimmed = address.trim();
  return trimmed.slice(trimmed.lastIndexOf(":") + 1);
}

/** Dependencies for a full E2E run. */
export interface RunE2EDeps {
  spawn: Spawn;
  /** Parent process environment. */
  env: Record<string, string>;
  /** Registers a handler invoked once per received termination signal. */
  registerSignalHandler: (handler: (signal: TerminationSignal) => void) => void;
  /** Writes the label override compose file and returns its path. */
  writeComposeOverride: (labels: ResourceLabels) => string;
  /** Removes the override compose file written above. */
  removeComposeOverride: (path: string) => void;
  log: (message: string) => void;
  now: () => number;
  runId: string;
  composeFile: string;
  databasePassword: string;
}

/**
 * Runs the E2E suite against a freshly provisioned, uniquely labeled Postgres
 * stack. Children run asynchronously and receive forwarded termination
 * signals; cleanup runs exactly once across every exit path; and the original
 * test (or signal) status is preserved through cleanup. Returns the final
 * process exit status.
 */
export async function runE2E(deps: RunE2EDeps): Promise<number> {
  const createdAtMs = deps.now();
  const labels = buildResourceLabels(deps.runId, createdAtMs);
  const overridePath = deps.writeComposeOverride(labels);

  const composeProjectName = `tether-e2e-${deps.runId}`;
  const composeDatabaseUrl = `postgres://tether:${deps.databasePassword}@postgres:5432/tether`;
  const composeEnv: Record<string, string> = {
    ...deps.env,
    DATABASE_URL: composeDatabaseUrl,
    POSTGRES_HOST_PORT: "0",
    POSTGRES_PASSWORD: deps.databasePassword,
  };
  const composeArgs = [
    "compose",
    "-p",
    composeProjectName,
    "-f",
    deps.composeFile,
    "-f",
    overridePath,
  ];

  let activeChild: ChildProcess | null = null;
  let interruptStatus: number | null = null;
  let cleaned = false;
  let cleanupStatus = 0;

  deps.registerSignalHandler((signal) => {
    if (interruptStatus === null) {
      interruptStatus = signalExitStatus(signal);
      deps.log(`Received ${signal}; forwarding to E2E children and cleaning up.`);
    }
    // Only the active test/startup child is signalled. The cleanup child is
    // intentionally never registered as active so cleanup runs to completion.
    activeChild?.kill(signal);
  });

  // Runs a child that participates in signal forwarding. If a signal already
  // arrived, it short-circuits so the flow proceeds straight to cleanup.
  const runForwarded = async (
    command: string,
    args: readonly string[],
    options: SpawnOptions,
  ): Promise<SpawnResult> => {
    if (interruptStatus !== null) {
      return { status: interruptStatus, signal: null, stdout: "" };
    }
    const child = deps.spawn(command, args, options);
    activeChild = child;
    try {
      return await child.result;
    } finally {
      activeChild = null;
    }
  };

  // Cleanup runs exactly once. The down child is not signal-forwarded so an
  // in-flight interrupt cannot abort resource teardown.
  const cleanup = async (): Promise<number> => {
    if (cleaned) {
      return cleanupStatus;
    }
    cleaned = true;
    deps.log(`Cleaning up E2E resources for project ${composeProjectName}.`);
    const child = deps.spawn("docker", [...composeArgs, "down", "-v", "--remove-orphans"], {
      env: composeEnv,
    });
    const result = await child.result;
    cleanupStatus = exitStatusFromResult(result);
    return cleanupStatus;
  };

  let testStatus = 1;
  try {
    const up = await runForwarded("docker", [...composeArgs, "up", "-d", "--wait", "postgres"], {
      env: composeEnv,
    });
    const upStatus = exitStatusFromResult(up);
    if (upStatus !== 0) {
      testStatus = upStatus;
    } else {
      const port = await runForwarded("docker", [...composeArgs, "port", "postgres", "5432"], {
        env: composeEnv,
        capture: true,
      });
      const portStatus = exitStatusFromResult(port);
      if (portStatus !== 0) {
        testStatus = portStatus;
      } else {
        const publishedPort = parsePublishedPort(port.stdout);
        const testEnv: Record<string, string> = {
          ...deps.env,
          E2E: "true",
          E2E_ADMIN_DATABASE_URL: `postgres://tether:${deps.databasePassword}@127.0.0.1:${publishedPort}/postgres`,
        };
        const test = await runForwarded("vitest", ["run", "test/e2e.test.ts"], {
          env: testEnv,
        });
        testStatus = exitStatusFromResult(test);
      }
    }
  } catch (error) {
    testStatus = 1;
    deps.log(
      `E2E run failed: ${error instanceof Error ? (error.stack ?? error.message) : String(error)}`,
    );
  }

  const finalCleanupStatus = await cleanup();
  deps.removeComposeOverride(overridePath);

  if (interruptStatus !== null) {
    testStatus = interruptStatus;
  }
  return resolveExitStatus(testStatus, finalCleanupStatus);
}

/** A removal plan scoped strictly to Tether E2E labeled resources. */
export interface CleanupPlan {
  containers: string[];
  volumes: string[];
}

/**
 * Builds the removal plan from labeled container and volume listings. Only
 * resources past their TTL are included, and only ones carrying a parseable
 * creation timestamp, so a missing label can never widen the scope.
 */
export function planExpiredCleanup(
  containers: readonly LabeledResource[],
  volumes: readonly LabeledResource[],
  nowMs: number,
  ttlMs: number,
): CleanupPlan {
  return {
    containers: selectExpired(containers, nowMs, ttlMs).map((resource) => resource.name),
    volumes: selectExpired(volumes, nowMs, ttlMs).map((resource) => resource.name),
  };
}

/** Go templates used to list labeled resources with their timestamps. */
export const CONTAINER_LABEL_FORMAT = `{{.Label "${E2E_CREATED_AT_LABEL}"}}|{{.Label "${E2E_RUN_ID_LABEL}"}}|{{.Names}}`;
export const VOLUME_LABEL_FORMAT = `{{.Label "${E2E_CREATED_AT_LABEL}"}}|{{.Label "${E2E_RUN_ID_LABEL}"}}|{{.Name}}`;

/** Dependencies for the expired-resource cleanup command. */
export interface CleanExpiredDeps {
  /** Runs a docker command and returns its captured stdout. */
  capture: (command: string, args: readonly string[]) => Promise<SpawnResult>;
  log: (message: string) => void;
  now: () => number;
  ttlMs: number;
  /** When false (the default), only report the plan without removing anything. */
  apply: boolean;
}

/** Outcome of the expired-resource cleanup command. */
export interface CleanExpiredResult {
  plan: CleanupPlan;
  applied: boolean;
  exitCode: number;
}

/**
 * Discovers Tether E2E resources by label, plans removal of expired ones, and
 * either reports the plan (dry run, the default) or removes exactly those
 * labeled resources. Resources without the `com.tether.e2e` label - including
 * the developer's running stack - are never listed and never removed.
 */
export async function cleanExpired(deps: CleanExpiredDeps): Promise<CleanExpiredResult> {
  const containerListing = await deps.capture("docker", [
    "ps",
    "-a",
    "--filter",
    `label=${E2E_LABEL}=true`,
    "--format",
    CONTAINER_LABEL_FORMAT,
  ]);
  const volumeListing = await deps.capture("docker", [
    "volume",
    "ls",
    "--filter",
    `label=${E2E_LABEL}=true`,
    "--format",
    VOLUME_LABEL_FORMAT,
  ]);

  const containers = parseDockerLabelLines(containerListing.stdout);
  const volumes = parseDockerLabelLines(volumeListing.stdout);
  const plan = planExpiredCleanup(containers, volumes, deps.now(), deps.ttlMs);

  const total = plan.containers.length + plan.volumes.length;
  if (total === 0) {
    deps.log("No expired Tether E2E resources found.");
    return { plan, applied: false, exitCode: 0 };
  }

  if (!deps.apply) {
    deps.log(`Dry run: ${total} expired Tether E2E resource(s) would be removed.`);
    for (const name of plan.containers) {
      deps.log(`  container ${name}`);
    }
    for (const name of plan.volumes) {
      deps.log(`  volume ${name}`);
    }
    deps.log("Re-run with --apply to remove them.");
    return { plan, applied: false, exitCode: 0 };
  }

  let exitCode = 0;
  if (plan.containers.length > 0) {
    const removed = await deps.capture("docker", ["rm", "-f", ...plan.containers]);
    exitCode = Math.max(exitCode, exitStatusFromResult(removed));
  }
  if (plan.volumes.length > 0) {
    const removed = await deps.capture("docker", ["volume", "rm", ...plan.volumes]);
    exitCode = Math.max(exitCode, exitStatusFromResult(removed));
  }
  deps.log(`Removed ${total} expired Tether E2E resource(s).`);
  return { plan, applied: true, exitCode };
}
