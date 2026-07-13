import { readFile } from "node:fs/promises";

import ts from "typescript";
import { describe, expect, it } from "vitest";

import { ModuleObservability as PackageModuleObservability } from "@dungle-scrubs/tether-client/observability";
import {
  ModuleObservability,
  readModuleObservabilityOptions,
  TetherInvariantError,
  type StructuredLogEntry,
} from "../src/observability.js";

describe("module observability", () => {
  it("consumes the package-owned helper through the service compatibility module", async () => {
    const source = await readFile(new URL("../src/observability.ts", import.meta.url), "utf8");
    const sourceFile = ts.createSourceFile(
      "observability.ts",
      source,
      ts.ScriptTarget.Latest,
      true,
    );

    expect(ModuleObservability).toBe(PackageModuleObservability);
    expect(
      sourceFile.statements.every(
        (statement) =>
          ts.isExportDeclaration(statement) &&
          statement.moduleSpecifier !== undefined &&
          ts.isStringLiteral(statement.moduleSpecifier) &&
          statement.moduleSpecifier.text === "@dungle-scrubs/tether-client/observability",
      ),
    ).toBe(true);
  });

  it("records boundary logs and debug counters", async () => {
    const logs: StructuredLogEntry[] = [];
    const observability = new ModuleObservability({
      boundaryLogsEnabled: true,
      debugEnabled: true,
      logger: { log: (entry) => logs.push(entry) },
      moduleName: "TestModule",
    });

    const result = await observability.traceBoundary(
      "doThing",
      { id: "item_1" },
      async () => "done",
      (value) => ({ value }),
    );

    expect(result).toBe("done");
    expect(logs.map((entry) => entry.message)).toEqual(["boundary.enter", "boundary.exit"]);
    expect(observability.debugInfo()).toMatchObject({
      activeOperations: 0,
      boundaryCalls: 1,
      boundaryFailures: 0,
      lastOperation: "doThing",
      moduleName: "TestModule",
    });
  });

  it("records failed boundaries", async () => {
    const logs: StructuredLogEntry[] = [];
    const observability = new ModuleObservability({
      boundaryLogsEnabled: true,
      logger: { log: (entry) => logs.push(entry) },
      moduleName: "TestModule",
    });

    await expect(
      observability.traceBoundary("failThing", {}, async () => {
        throw new Error("boom");
      }),
    ).rejects.toThrow("boom");

    expect(logs.map((entry) => entry.message)).toEqual(["boundary.enter", "boundary.error"]);
    expect(observability.debugInfo()).toMatchObject({
      boundaryFailures: 1,
      lastError: { message: "boom", name: "Error" },
    });
  });

  it("throws typed invariant errors", () => {
    const observability = new ModuleObservability({ moduleName: "TestModule" });

    expect(() =>
      observability.assertInvariant(false, "checkThing", "broken invariant", {
        id: "item_1",
      }),
    ).toThrow(TetherInvariantError);
  });

  it("reads current observability environment names", () => {
    expect(
      readModuleObservabilityOptions("TestModule", {
        DEBUG: "true",
        OBSERVABILITY: "1",
      }),
    ).toMatchObject({
      boundaryLogsEnabled: true,
      debugEnabled: true,
      moduleName: "TestModule",
    });
  });
});
