import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import ts from "typescript";
import { describe, expect, it } from "vitest";

const checkedModules = [
  { path: "../src/participant-rest-replay.ts", declarationBaseline: 0, documentationBaseline: 0 },
  // Incremental baseline measured at pre-feature commit 085c182, not a waiver for new helpers.
  {
    path: "./participant-runtime-client.test.ts",
    declarationBaseline: 27,
    documentationBaseline: 8,
  },
];

/** Finds the module-level declaration containing a node. */
function moduleStatement(node: ts.Node, source: ts.SourceFile): ts.Node {
  let current = node;
  while (current.parent && current.parent !== source) current = current.parent;
  return current;
}

/** Visits syntax without treating comments or strings as identifier references. */
function visit(node: ts.Node, check: (node: ts.Node) => void): void {
  check(node);
  ts.forEachChild(node, (child) => visit(child, check));
}

/** Resolves symbols in one source file, including archived source supplied by correction proofs. */
function analyzeModule(path: string, text = readFileSync(path, "utf8")) {
  const options = { noLib: true, noResolve: true };
  const host = ts.createCompilerHost(options);
  host.getSourceFile = (filename, languageVersion) =>
    filename === path ? ts.createSourceFile(filename, text, languageVersion, true) : undefined;
  const program = ts.createProgram([path], options, host);
  const source = program.getSourceFile(path);
  if (!source) throw new Error(`Convention source was not loaded: ${path}`);
  const checker = program.getTypeChecker();
  const positions = new Map<ts.Node, number>(
    source.statements.map((statement, index) => [statement, index]),
  );
  const declarations = new Set<string>();
  const documentation: string[] = [];
  visit(source, (node) => {
    if (ts.isIdentifier(node)) {
      const declaration = checker
        .getSymbolAtLocation(node)
        ?.declarations?.find((candidate) => candidate.getSourceFile() === source);
      if (declaration) {
        const declaredAt = positions.get(moduleStatement(declaration, source));
        const usedAt = positions.get(moduleStatement(node, source));
        if (declaredAt !== undefined && usedAt !== undefined && declaredAt > usedAt) {
          declarations.add(`Declare ${node.text} before the module declaration that uses it`);
        }
      }
    }
    if (
      (ts.isFunctionDeclaration(node) ||
        ts.isMethodDeclaration(node) ||
        ts.isConstructorDeclaration(node) ||
        ts.isGetAccessorDeclaration(node) ||
        ts.isSetAccessorDeclaration(node)) &&
      node.body &&
      ts.getJSDocCommentsAndTags(node).length === 0
    ) {
      documentation.push(`Add JSDoc to ${node.name?.getText(source) ?? "constructor"}`);
    }
  });
  return { declarations: [...declarations], documentation };
}

describe("REST replay module conventions", () => {
  for (const module of checkedModules) {
    const path = fileURLToPath(new URL(module.path, import.meta.url));
    it(`${module.path} declares module dependencies before use`, () => {
      const findings = analyzeModule(path).declarations;
      expect(
        findings.length,
        `${findings.join("\n")}\nMove dependencies above first use; do not increase the pre-feature baseline.`,
      ).toBeLessThanOrEqual(module.declarationBaseline);
    });
    it(`${module.path} documents named functions and methods`, () => {
      const findings = analyzeModule(path).documentation;
      expect(
        findings.length,
        `${findings.join("\n")}\nAdd JSDoc to new named declarations; do not increase the pre-feature baseline.`,
      ).toBeLessThanOrEqual(module.documentationBaseline);
    });
  }
});
