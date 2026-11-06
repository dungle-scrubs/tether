import { fileURLToPath } from "node:url";
import ts from "typescript";
import { describe, expect, it } from "vitest";

const modulePath = fileURLToPath(new URL("../src/participant-rest-replay.ts", import.meta.url));
const program = ts.createProgram([modulePath], { noLib: true, noResolve: true });
const source = program.getSourceFile(modulePath);
if (!source) throw new Error("REST replay source was not loaded");
const moduleFile = source;
const checker = program.getTypeChecker();
const positions = new Map<ts.Node, number>(
  moduleFile.statements.map((statement, index): [ts.Node, number] => [statement, index]),
);

/** Finds the module-level declaration containing a node. */
function moduleStatement(node: ts.Node): ts.Node {
  let current = node;
  while (current.parent && current.parent !== moduleFile) current = current.parent;
  return current;
}

/** Visits syntax without treating comments or strings as identifier references. */
function visit(node: ts.Node, check: (node: ts.Node) => void): void {
  check(node);
  ts.forEachChild(node, (child) => visit(child, check));
}

/** Checks the existing module declaration-order rule with lexical symbol resolution. */
function declarationFindings(): readonly string[] {
  const findings = new Set<string>();
  visit(moduleFile, (node) => {
    if (!ts.isIdentifier(node)) return;
    const declaration = checker
      .getSymbolAtLocation(node)
      ?.declarations?.find((candidate) => candidate.getSourceFile() === moduleFile);
    if (!declaration) return;
    const declaredAt = positions.get(moduleStatement(declaration));
    const usedAt = positions.get(moduleStatement(node));
    if (declaredAt !== undefined && usedAt !== undefined && declaredAt > usedAt) {
      findings.add(`Declare ${node.text} before the module declaration that uses it`);
    }
  });
  return [...findings];
}

/** Checks JSDoc on named functions and methods, leaving inline callbacks alone. */
function documentationFindings(): readonly string[] {
  const findings: string[] = [];
  visit(moduleFile, (node) => {
    if (
      (ts.isFunctionDeclaration(node) ||
        ts.isMethodDeclaration(node) ||
        ts.isConstructorDeclaration(node) ||
        ts.isGetAccessorDeclaration(node) ||
        ts.isSetAccessorDeclaration(node)) &&
      node.body &&
      ts.getJSDocCommentsAndTags(node).length === 0
    ) {
      findings.push(`Add JSDoc to ${node.name?.getText(moduleFile) ?? "constructor"}`);
    }
  });
  return findings;
}

describe("REST replay module conventions", () => {
  it("declares module dependencies before use", () => {
    expect(declarationFindings()).toEqual([]);
  });

  it("documents named functions and methods", () => {
    expect(documentationFindings()).toEqual([]);
  });
});
