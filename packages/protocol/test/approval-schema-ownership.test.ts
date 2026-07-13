import { readFileSync } from "node:fs";
import { execFileSync } from "node:child_process";
import { relative, resolve } from "node:path";

import ts from "typescript";
import { describe, expect, it } from "vitest";

const repoRoot = resolve(import.meta.dirname, "../../..");
const protocolOwnerFile = "packages/protocol/src/rest-schemas.ts";
const approvalSchemaConsumerFiles = ["packages/client-bridge/src/schemas.ts"] as const;

describe("approval schema ownership", () => {
  it("keeps durable approval decision Zod validators owned by @dungle-scrubs/tether-protocol", () => {
    const duplicates = productionTypeScriptFiles()
      .filter((file) => file !== protocolOwnerFile)
      .flatMap((file) =>
        localApprovalDecisionValidators(file).map((expression) => `${file}: ${expression}`),
      );

    expect(duplicates).toEqual([]);
  });

  it("keeps affected consumers deriving approval validation from protocol schemas", () => {
    const missingImports = approvalSchemaConsumerFiles.filter(
      (file) => !importsProtocolApprovalSchema(file),
    );

    expect(missingImports).toEqual([]);
  });

  it("allows the protocol owner to declare the durable approval decision validator", () => {
    expect(localApprovalDecisionValidators(protocolOwnerFile)).toEqual([
      'z.union([z.literal("approved"), z.literal("rejected")])',
    ]);
  });
});

/** Lists production TypeScript sources that can carry runtime validators. */
function productionTypeScriptFiles(): readonly string[] {
  return execFileSync("git", ["ls-files", "apps/**/*.ts", "packages/**/*.ts"], {
    cwd: repoRoot,
    encoding: "utf8",
  })
    .split("\n")
    .filter((file) => file.endsWith(".ts"))
    .filter((file) => !file.endsWith(".d.ts"))
    .filter((file) => !file.includes("/test/"))
    .filter((file) => !file.includes("/tests/"))
    .filter((file) => !file.endsWith(".test.ts"));
}

/** Finds local Zod approved/rejected validators in one production source file. */
function localApprovalDecisionValidators(file: string): readonly string[] {
  const sourceFile = readSourceFile(file);
  const validators: string[] = [];

  const visit = (node: ts.Node): void => {
    if (isApprovalDecisionValidator(node)) {
      validators.push(node.getText(sourceFile));
    }
    ts.forEachChild(node, visit);
  };

  visit(sourceFile);
  return validators;
}

/** Reads one TypeScript source file from the repository root. */
function readSourceFile(file: string): ts.SourceFile {
  const path = resolve(repoRoot, file);
  return ts.createSourceFile(
    relative(repoRoot, path),
    readFileSync(path, "utf8"),
    ts.ScriptTarget.Latest,
    true,
    ts.ScriptKind.TS,
  );
}

/** Returns true when a source file imports or re-exports protocol approval schemas. */
function importsProtocolApprovalSchema(file: string): boolean {
  const source = readFileSync(resolve(repoRoot, file), "utf8");
  return (
    source.includes("approvalDecisionSchema") ||
    source.includes("taskApprovalRecordedPayloadSchema")
  );
}

/** Detects both local z.union and z.enum spellings for the durable approval decision. */
function isApprovalDecisionValidator(node: ts.Node): boolean {
  if (!ts.isCallExpression(node)) {
    return false;
  }
  const expression = node.expression.getText();
  if (expression === "z.enum") {
    return hasApprovedRejectedArray(node.arguments[0]);
  }
  return expression === "z.union" && hasApprovedRejectedLiteralUnion(node.arguments[0]);
}

/** Returns true for ["approved", "rejected"] or ["rejected", "approved"]. */
function hasApprovedRejectedArray(node: ts.Node | undefined): boolean {
  if (!node || !ts.isArrayLiteralExpression(node)) {
    return false;
  }
  const values = node.elements.map((element) =>
    ts.isStringLiteralLike(element) ? element.text : null,
  );
  return values.length === 2 && values.includes("approved") && values.includes("rejected");
}

/** Returns true for arrays of z.literal("approved") and z.literal("rejected"). */
function hasApprovedRejectedLiteralUnion(node: ts.Node | undefined): boolean {
  if (!node || !ts.isArrayLiteralExpression(node)) {
    return false;
  }
  const values = node.elements.map((element) => literalArgumentText(element));
  return values.length === 2 && values.includes("approved") && values.includes("rejected");
}

/** Extracts the string argument from z.literal("...") expressions. */
function literalArgumentText(node: ts.Node): string | null {
  if (!ts.isCallExpression(node) || node.expression.getText() !== "z.literal") {
    return null;
  }
  const [argument] = node.arguments;
  return argument && ts.isStringLiteralLike(argument) ? argument.text : null;
}
