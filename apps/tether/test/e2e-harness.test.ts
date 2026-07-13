import { describe, expect, it } from "vitest";

import {
  buildResourceLabels,
  type ChildProcess,
  cleanExpired,
  E2E_CREATED_AT_LABEL,
  E2E_LABEL,
  E2E_RUN_ID_LABEL,
  exitStatusFromResult,
  parseDockerLabelLines,
  parsePublishedPort,
  planExpiredCleanup,
  type RunE2EDeps,
  resolveExitStatus,
  runE2E,
  type Spawn,
  type SpawnOptions,
  type SpawnResult,
  selectExpired,
  signalExitStatus,
  type TerminationSignal,
} from "../scripts/e2e-harness.js";

describe("buildResourceLabels", () => {
  it("stamps the marker, run ID, and creation timestamp", () => {
    const labels = buildResourceLabels("run-123", 1_700_000_000_000);
    expect(labels).toEqual({
      [E2E_LABEL]: "true",
      [E2E_RUN_ID_LABEL]: "run-123",
      [E2E_CREATED_AT_LABEL]: "1700000000000",
    });
  });
});

describe("parseDockerLabelLines", () => {
  it("parses created-at, run-id, and name and skips blank lines", () => {
    const output =
      "\n1700000000000|run-a|tether-e2e-a-postgres-1\n  1700000005000|run-b|vol-b  \n\n";
    expect(parseDockerLabelLines(output)).toEqual([
      { createdAt: 1_700_000_000_000, runId: "run-a", name: "tether-e2e-a-postgres-1" },
      { createdAt: 1_700_000_005_000, runId: "run-b", name: "vol-b" },
    ]);
  });

  it("treats a missing or non-numeric timestamp as unknown, never expired", () => {
    const output = "|run-c|resource-c\nnot-a-number|run-d|resource-d\n";
    const parsed = parseDockerLabelLines(output);
    expect(parsed).toEqual([
      { createdAt: null, runId: "run-c", name: "resource-c" },
      { createdAt: null, runId: "run-d", name: "resource-d" },
    ]);
  });

  it("drops lines that lack a resource name", () => {
    expect(parseDockerLabelLines("1700000000000|run-e|\n")).toEqual([]);
  });
});

describe("selectExpired", () => {
  const now = 10_000;
  const ttl = 1_000;
  const resources = [
    { createdAt: 8_000, runId: "old", name: "old" },
    { createdAt: 9_000, runId: "boundary", name: "boundary" },
    { createdAt: 9_500, runId: "fresh", name: "fresh" },
    { createdAt: null, runId: "unknown", name: "unknown" },
  ];

  it("selects only resources at or past the TTL cutoff", () => {
    expect(selectExpired(resources, now, ttl).map((r) => r.name)).toEqual(["old", "boundary"]);
  });

  it("never selects resources with an unknown creation time", () => {
    const onlyUnknown = [{ createdAt: null, runId: "x", name: "x" }];
    expect(selectExpired(onlyUnknown, now, ttl)).toEqual([]);
  });
});

describe("resolveExitStatus", () => {
  it("preserves a nonzero test status through a clean teardown", () => {
    expect(resolveExitStatus(3, 0)).toBe(3);
  });

  it("preserves the test status even when cleanup fails", () => {
    expect(resolveExitStatus(1, 7)).toBe(1);
  });

  it("surfaces a cleanup failure when the test passed", () => {
    expect(resolveExitStatus(0, 5)).toBe(5);
  });

  it("returns success when both the test and cleanup pass", () => {
    expect(resolveExitStatus(0, 0)).toBe(0);
  });
});

describe("signalExitStatus and exitStatusFromResult", () => {
  it("maps signals to conventional 128 + signal-number exit codes", () => {
    expect(signalExitStatus("SIGINT")).toBe(130);
    expect(signalExitStatus("SIGTERM")).toBe(143);
  });

  it("prefers an explicit status over a signal", () => {
    expect(exitStatusFromResult({ status: 2, signal: "SIGTERM", stdout: "" })).toBe(2);
  });

  it("derives an exit code from the terminating signal", () => {
    expect(exitStatusFromResult({ status: null, signal: "SIGINT", stdout: "" })).toBe(130);
  });

  it("falls back to 1 when neither status nor signal is present", () => {
    expect(exitStatusFromResult({ status: null, signal: null, stdout: "" })).toBe(1);
  });
});

describe("parsePublishedPort", () => {
  it("extracts the port from an address", () => {
    expect(parsePublishedPort("127.0.0.1:54321\n")).toBe("54321");
  });
});

describe("planExpiredCleanup", () => {
  it("scopes removal to expired resources only", () => {
    const containers = [
      { createdAt: 1_000, runId: "old", name: "c-old" },
      { createdAt: 9_500, runId: "fresh", name: "c-fresh" },
    ];
    const volumes = [
      { createdAt: 1_000, runId: "old", name: "v-old" },
      { createdAt: null, runId: "unknown", name: "v-unknown" },
    ];
    expect(planExpiredCleanup(containers, volumes, 10_000, 1_000)).toEqual({
      containers: ["c-old"],
      volumes: ["v-old"],
    });
  });
});

// --- Orchestration harness with an injected spawner ---

class FakeChild implements ChildProcess {
  readonly killed: NodeJS.Signals[] = [];
  resolveOnKill: SpawnResult | null = null;
  private settle!: (result: SpawnResult) => void;
  readonly result: Promise<SpawnResult>;

  constructor() {
    this.result = new Promise<SpawnResult>((resolve) => {
      this.settle = resolve;
    });
  }

  kill(signal: NodeJS.Signals): void {
    this.killed.push(signal);
    if (this.resolveOnKill !== null) {
      this.resolve({ ...this.resolveOnKill, signal });
    }
  }

  resolve(result: SpawnResult): void {
    this.settle(result);
  }
}

interface SpawnCall {
  command: string;
  args: string[];
  options: SpawnOptions;
  child: FakeChild;
}

function harness(program: (call: SpawnCall) => void) {
  const calls: SpawnCall[] = [];
  const logs: string[] = [];
  let signalHandler: ((signal: TerminationSignal) => void) | null = null;
  const overrides: Array<Record<string, string>> = [];

  const spawn: Spawn = (command, args, options) => {
    const child = new FakeChild();
    const call: SpawnCall = { command, args: [...args], options, child };
    calls.push(call);
    program(call);
    return child;
  };

  const deps: RunE2EDeps = {
    spawn,
    env: { PATH: "/usr/bin" },
    registerSignalHandler: (handler) => {
      signalHandler = handler;
    },
    writeComposeOverride: (labels) => {
      overrides.push({ ...labels });
      return "/tmp/override.yml";
    },
    removeComposeOverride: () => {},
    log: (message) => {
      logs.push(message);
    },
    now: () => 1_700_000_000_000,
    runId: "run-fixed",
    composeFile: "../../docker-compose.yml",
    databasePassword: "pw",
  };

  return {
    deps,
    calls,
    logs,
    overrides,
    fireSignal: (signal: TerminationSignal) => signalHandler?.(signal),
  };
}

const isCommand = (call: SpawnCall, ...tokens: string[]) =>
  tokens.every((token) => call.command === token || call.args.includes(token));

const downCalls = (calls: SpawnCall[]) => calls.filter((call) => call.args.includes("down"));

async function waitFor(predicate: () => boolean): Promise<void> {
  for (let attempt = 0; attempt < 100; attempt += 1) {
    if (predicate()) {
      return;
    }
    await new Promise((resolve) => setTimeout(resolve, 0));
  }
  throw new Error("waitFor timed out");
}

describe("runE2E lifecycle", () => {
  it("labels resources with the run ID and creation time and isolates the project", () => {
    const { deps, calls, overrides } = harness((call) => {
      if (isCommand(call, "port")) {
        call.child.resolve({ status: 0, signal: null, stdout: "127.0.0.1:54321" });
      } else {
        call.child.resolve({ status: 0, signal: null, stdout: "" });
      }
    });
    return runE2E(deps).then((code) => {
      expect(code).toBe(0);
      expect(overrides[0]).toEqual({
        [E2E_LABEL]: "true",
        [E2E_RUN_ID_LABEL]: "run-fixed",
        [E2E_CREATED_AT_LABEL]: "1700000000000",
      });
      // Every compose invocation is scoped to the unique per-run project and
      // never to the developer's default `tether` project.
      const composeCalls = calls.filter((call) => call.args.includes("compose"));
      expect(composeCalls.length).toBeGreaterThan(0);
      for (const call of composeCalls) {
        expect(call.args).toContain("tether-e2e-run-fixed");
        expect(call.args).not.toContain("tether");
      }
    });
  });

  it("runs children asynchronously and cleans up exactly once on success", async () => {
    const { deps, calls } = harness((call) => {
      const stdout = isCommand(call, "port") ? "127.0.0.1:54321" : "";
      call.child.resolve({ status: 0, signal: null, stdout });
    });
    const code = await runE2E(deps);
    expect(code).toBe(0);
    expect(downCalls(calls)).toHaveLength(1);
    expect(calls.some((call) => call.command === "vitest")).toBe(true);
  });

  it("preserves a test failure status through cleanup", async () => {
    const { deps, calls } = harness((call) => {
      if (call.command === "vitest") {
        call.child.resolve({ status: 1, signal: null, stdout: "" });
      } else {
        const stdout = isCommand(call, "port") ? "127.0.0.1:54321" : "";
        call.child.resolve({ status: 0, signal: null, stdout });
      }
    });
    const code = await runE2E(deps);
    expect(code).toBe(1);
    expect(downCalls(calls)).toHaveLength(1);
  });

  it("preserves the test failure status even when cleanup also fails", async () => {
    const { deps, calls } = harness((call) => {
      if (call.command === "vitest") {
        call.child.resolve({ status: 1, signal: null, stdout: "" });
      } else if (call.args.includes("down")) {
        call.child.resolve({ status: 137, signal: null, stdout: "" });
      } else {
        const stdout = isCommand(call, "port") ? "127.0.0.1:54321" : "";
        call.child.resolve({ status: 0, signal: null, stdout });
      }
    });
    const code = await runE2E(deps);
    expect(code).toBe(1);
    expect(downCalls(calls)).toHaveLength(1);
  });

  it("surfaces a cleanup failure when the test passed", async () => {
    const { deps } = harness((call) => {
      if (call.args.includes("down")) {
        call.child.resolve({ status: 5, signal: null, stdout: "" });
      } else {
        const stdout = isCommand(call, "port") ? "127.0.0.1:54321" : "";
        call.child.resolve({ status: 0, signal: null, stdout });
      }
    });
    const code = await runE2E(deps);
    expect(code).toBe(5);
  });

  it("cleans up after a startup failure without running the test", async () => {
    const { deps, calls } = harness((call) => {
      if (isCommand(call, "up")) {
        call.child.resolve({ status: 1, signal: null, stdout: "" });
      } else {
        call.child.resolve({ status: 0, signal: null, stdout: "" });
      }
    });
    const code = await runE2E(deps);
    expect(code).toBe(1);
    expect(calls.some((call) => call.command === "vitest")).toBe(false);
    expect(calls.some((call) => call.args.includes("port"))).toBe(false);
    expect(downCalls(calls)).toHaveLength(1);
  });

  for (const [signal, expected] of [
    ["SIGINT", 130],
    ["SIGTERM", 143],
  ] as const) {
    it(`forwards ${signal} to the active child, cleans up once, and preserves the signal status`, async () => {
      const { deps, calls, fireSignal } = harness((call) => {
        if (call.command === "vitest") {
          // The test child only settles once the forwarded signal reaches it.
          call.child.resolveOnKill = { status: null, signal: null, stdout: "" };
        } else {
          const stdout = isCommand(call, "port") ? "127.0.0.1:54321" : "";
          call.child.resolve({ status: 0, signal: null, stdout });
        }
      });
      const run = runE2E(deps);
      await waitFor(() => calls.some((call) => call.command === "vitest"));
      fireSignal(signal);
      const code = await run;
      expect(code).toBe(expected);
      const vitestCall = calls.find((call) => call.command === "vitest");
      expect(vitestCall?.child.killed).toEqual([signal]);
      // Cleanup ran exactly once and was never itself signalled.
      const down = downCalls(calls);
      expect(down).toHaveLength(1);
      expect(down[0]?.child.killed).toEqual([]);
    });
  }
});

// --- Expired-resource cleanup command ---

interface CaptureCall {
  command: string;
  args: string[];
}

function cleanupHarness(responder: (call: CaptureCall) => SpawnResult) {
  const calls: CaptureCall[] = [];
  const logs: string[] = [];
  const capture = (command: string, args: readonly string[]): Promise<SpawnResult> => {
    const call: CaptureCall = { command, args: [...args] };
    calls.push(call);
    return Promise.resolve(responder(call));
  };
  return { capture, calls, logs, log: (message: string) => logs.push(message) };
}

describe("cleanExpired", () => {
  const containerListing = "1000|old|c-old\n9999|fresh|c-fresh\n";
  const volumeListing = "1000|old|v-old\n";

  const respond = (call: CaptureCall): SpawnResult => {
    if (call.args.includes("ps")) {
      return { status: 0, signal: null, stdout: containerListing };
    }
    if (call.args.includes("volume") && call.args.includes("ls")) {
      return { status: 0, signal: null, stdout: volumeListing };
    }
    return { status: 0, signal: null, stdout: "" };
  };

  it("queries only Tether E2E labeled resources", async () => {
    const { capture, calls, log } = cleanupHarness(respond);
    await cleanExpired({ capture, log, now: () => 10_000, ttlMs: 1_000, apply: false });
    const listCalls = calls.filter((call) => call.args.includes("ps") || call.args.includes("ls"));
    expect(listCalls.length).toBeGreaterThan(0);
    for (const call of listCalls) {
      expect(call.args).toContain(`label=${E2E_LABEL}=true`);
    }
  });

  it("reports a dry run by default and removes nothing", async () => {
    const { capture, calls, logs } = cleanupHarness(respond);
    const result = await cleanExpired({
      capture,
      log: (m) => logs.push(m),
      now: () => 10_000,
      ttlMs: 1_000,
      apply: false,
    });
    expect(result.applied).toBe(false);
    expect(result.plan).toEqual({ containers: ["c-old"], volumes: ["v-old"] });
    expect(calls.some((call) => call.args.includes("rm"))).toBe(false);
    expect(logs.some((line) => line.includes("--apply"))).toBe(true);
  });

  it("removes exactly the expired labeled resources when applied", async () => {
    const { capture, calls } = cleanupHarness(respond);
    const result = await cleanExpired({
      capture,
      log: () => {},
      now: () => 10_000,
      ttlMs: 1_000,
      apply: true,
    });
    expect(result.applied).toBe(true);
    const rmCall = calls.find((call) => call.args[0] === "rm");
    expect(rmCall?.args).toEqual(["rm", "-f", "c-old"]);
    const volumeRm = calls.find((call) => call.args[0] === "volume" && call.args[1] === "rm");
    expect(volumeRm?.args).toEqual(["volume", "rm", "v-old"]);
  });

  it("does nothing when no resource is past its TTL", async () => {
    const { capture, calls } = cleanupHarness(respond);
    const result = await cleanExpired({
      capture,
      log: () => {},
      now: () => 1_500,
      ttlMs: 1_000,
      apply: true,
    });
    expect(result.plan).toEqual({ containers: [], volumes: [] });
    expect(calls.some((call) => call.args.includes("rm"))).toBe(false);
  });
});
