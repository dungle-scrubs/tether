import { readFile } from "node:fs/promises";

import ts from "typescript";
import { describe, expect, it } from "vitest";

import {
  ConsoleStructuredLogger,
  ModuleObservability,
  readModuleObservabilityOptions,
  TetherInvariantError,
  type BoundaryDebugInfo,
  type ModuleObservabilityOptions,
  type StructuredErrorInfo,
  type StructuredLogEntry,
  type StructuredLogger,
} from "@dungle-scrubs/tether-client/observability";

interface ExportedObservabilityContract {
  readonly BoundaryDebugInfo: BoundaryDebugInfo;
  readonly ConsoleStructuredLogger: typeof ConsoleStructuredLogger;
  readonly ModuleObservability: typeof ModuleObservability;
  readonly ModuleObservabilityOptions: ModuleObservabilityOptions;
  readonly readModuleObservabilityOptions: typeof readModuleObservabilityOptions;
  readonly StructuredErrorInfo: StructuredErrorInfo;
  readonly StructuredLogEntry: StructuredLogEntry;
  readonly StructuredLogger: StructuredLogger;
  readonly TetherInvariantError: typeof TetherInvariantError;
}

describe("@dungle-scrubs/tether-client/observability public API", () => {
  it("exposes the package-owned observability helper and types through the subpath", () => {
    const entry: StructuredLogEntry = {
      at: new Date(0).toISOString(),
      data: {},
      level: "info",
      message: "boundary.exit",
      moduleName: "ContractModule",
      operation: "check",
      traceId: "trace_contract",
    };

    const contract = {
      BoundaryDebugInfo: {
        activeOperations: 0,
        boundaryCalls: 0,
        boundaryFailures: 0,
        boundaryLogsEnabled: false,
        debugEnabled: false,
        lastError: null,
        lastOperation: null,
        moduleName: "ContractModule",
      },
      ConsoleStructuredLogger,
      ModuleObservability,
      ModuleObservabilityOptions: {
        moduleName: "ContractModule",
      },
      readModuleObservabilityOptions,
      StructuredErrorInfo: {
        message: "boom",
        name: "Error",
      },
      StructuredLogEntry: entry,
      StructuredLogger: {
        log: (_entry) => undefined,
      },
      TetherInvariantError,
    } satisfies ExportedObservabilityContract;

    expect(contract.ModuleObservability).toBe(ModuleObservability);
    expect(contract.readModuleObservabilityOptions("ContractModule").moduleName).toBe(
      "ContractModule",
    );
  });

  it("keeps exported observability interfaces and public fields documented", async () => {
    const source = await readFile(new URL("../src/observability.ts", import.meta.url), "utf8");
    const sourceFile = ts.createSourceFile(
      "observability.ts",
      source,
      ts.ScriptTarget.Latest,
      true,
    );
    const exportedInterfaces = getExportedInterfaces(sourceFile);

    expect(exportedInterfaces.map((node) => node.name.text).sort()).toEqual([
      "BoundaryDebugInfo",
      "ModuleObservabilityOptions",
      "StructuredErrorInfo",
      "StructuredLogEntry",
      "StructuredLogger",
    ]);

    for (const interfaceNode of exportedInterfaces) {
      expect(hasJsdoc(source, interfaceNode)).toBe(true);
      for (const member of interfaceNode.members) {
        expect(hasJsdoc(source, member)).toBe(true);
      }
    }

    expect(getPropertyJsdoc(source, exportedInterfaces, "StructuredLogEntry", "at")).toContain(
      "ISO timestamp",
    );
    expect(getPropertyJsdoc(source, exportedInterfaces, "StructuredLogEntry", "traceId")).toContain(
      "this is not an OpenTelemetry trace id",
    );
  });
});

/**
 * Finds exported interface declarations in source order.
 */
function getExportedInterfaces(sourceFile: ts.SourceFile): readonly ts.InterfaceDeclaration[] {
  const interfaces: ts.InterfaceDeclaration[] = [];
  for (const statement of sourceFile.statements) {
    if (ts.isInterfaceDeclaration(statement) && hasExportModifier(statement)) {
      interfaces.push(statement);
    }
  }
  return interfaces;
}

/**
 * Checks whether a declaration has an adjacent JSDoc comment.
 */
function hasJsdoc(source: string, node: ts.Node): boolean {
  return getLeadingJsdoc(source, node).length > 0;
}

/**
 * Reads JSDoc text for one exported interface property.
 */
function getPropertyJsdoc(
  source: string,
  interfaces: readonly ts.InterfaceDeclaration[],
  interfaceName: string,
  propertyName: string,
): string {
  const interfaceNode = interfaces.find((node) => node.name.text === interfaceName);
  const member = interfaceNode?.members.find(
    (node) => ts.isPropertySignature(node) && node.name.getText() === propertyName,
  );

  if (member === undefined) {
    return "";
  }

  return getLeadingJsdoc(source, member).join("\n");
}

/**
 * Finds leading JSDoc comments attached to a node.
 */
function getLeadingJsdoc(source: string, node: ts.Node): readonly string[] {
  return (
    ts
      .getLeadingCommentRanges(source, node.getFullStart())
      ?.map((range) => source.slice(range.pos, range.end))
      .filter((comment) => comment.startsWith("/**")) ?? []
  );
}

/**
 * Checks for an export modifier without relying on declaration name strings.
 */
function hasExportModifier(node: ts.Node): boolean {
  return ts.canHaveModifiers(node)
    ? (ts.getModifiers(node)?.some((modifier) => modifier.kind === ts.SyntaxKind.ExportKeyword) ??
        false)
    : false;
}
