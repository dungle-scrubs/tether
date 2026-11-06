import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import ts from "typescript";
import { describe, expect, it } from "vitest";

/** Files whose convention identities are ratcheted by the client test command. */
const checkedModules = [
  "../src/participant-rest-replay.ts",
  "../src/participant-runtime-client.ts",
  "../src/participant-claimable-task-runner.ts",
  "../src/rest-participant-task-client.ts",
  "./participant-runtime-client.test.ts",
  "./rest-participant-task-client.test.ts",
  "./participant-rest-replay-style.test.ts",
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

/** Identifies enclosing declarations and literal test titles without unstable line numbers. */
function findingScope(node: ts.Node, source: ts.SourceFile): string {
  const owners: string[] = [];
  let current: ts.Node | undefined = node;
  while (current && current !== source) {
    if ("name" in current && current.name && ts.isIdentifier(current.name as ts.Node)) {
      owners.unshift(`${ts.SyntaxKind[current.kind]}:${(current.name as ts.Identifier).text}`);
    } else if (
      ts.isCallExpression(current) &&
      current.arguments[0] &&
      (ts.isStringLiteral(current.arguments[0]) ||
        ts.isNoSubstitutionTemplateLiteral(current.arguments[0]))
    ) {
      owners.unshift(`call:${current.arguments[0].text}`);
    }
    current = current.parent;
  }
  return owners.join("/");
}

/** Recognizes named functions and fields on exported interface contracts. */
function documentationTarget(node: ts.Node): ts.Node | null {
  if (
    (ts.isFunctionDeclaration(node) ||
      ts.isMethodDeclaration(node) ||
      ts.isConstructorDeclaration(node) ||
      ts.isGetAccessorDeclaration(node) ||
      ts.isSetAccessorDeclaration(node)) &&
    node.body
  )
    return node;
  if (
    ts.isVariableDeclaration(node) &&
    node.initializer &&
    (ts.isArrowFunction(node.initializer) || ts.isFunctionExpression(node.initializer))
  )
    return node.parent.parent;
  if (ts.isPropertySignature(node)) {
    let owner: ts.Node | undefined = node.parent;
    while (owner && !ts.isSourceFile(owner)) {
      if (ts.isInterfaceDeclaration(owner)) {
        return owner.modifiers?.some((modifier) => modifier.kind === ts.SyntaxKind.ExportKeyword)
          ? node
          : null;
      }
      owner = owner.parent;
    }
  }
  return null;
}

/** Resolves exact finding identities, preserving every reference to an already-listed name. */
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
  const file = path.slice(path.lastIndexOf("packages/client/"));
  const declarations: string[] = [];
  const documentation: string[] = [];
  const occurrences = new Map<string, number>();
  visit(source, (node) => {
    if (ts.isIdentifier(node)) {
      const declaration = checker
        .getSymbolAtLocation(node)
        ?.declarations?.find((candidate) => candidate.getSourceFile() === source);
      if (declaration) {
        const declaredAt = positions.get(moduleStatement(declaration, source));
        const usedAt = positions.get(moduleStatement(node, source));
        if (declaredAt !== undefined && usedAt !== undefined && declaredAt > usedAt) {
          const site = JSON.stringify([
            file,
            "declaration-order",
            ts.SyntaxKind[declaration.kind],
            node.text,
            findingScope(node.parent, source),
            node.parent.getText(source).replace(/\s+/gu, " "),
          ]);
          const occurrence = (occurrences.get(site) ?? 0) + 1;
          occurrences.set(site, occurrence);
          declarations.push(`${site}#${occurrence}`);
        }
      }
    }
    const documented = documentationTarget(node);
    if (documented && ts.getJSDocCommentsAndTags(documented).length === 0) {
      documentation.push(
        JSON.stringify([
          file,
          "jsdoc",
          ts.SyntaxKind[node.kind],
          (node as ts.NamedDeclaration).name?.getText(source) ?? "constructor",
          findingScope(node, source),
        ]),
      );
    }
  });
  return { declarations: declarations.sort(), documentation: documentation.sort() };
}

/** Exact legacy identities only; fixing a violation must remove its baseline entry. */
const baseline = JSON.parse(
  readFileSync(new URL("./participant-conventions.baseline.json", import.meta.url), "utf8"),
) as Record<
  string,
  { readonly declarations: readonly string[]; readonly documentation: readonly string[] }
>;

describe("REST replay module conventions", () => {
  for (const module of checkedModules) {
    const path = fileURLToPath(new URL(module, import.meta.url));
    it(`${module} declares module dependencies before use`, () => {
      expect(
        analyzeModule(path).declarations,
        "Move each dependency above every referencing declaration. Remove fixed identities from the baseline; never replace them with new violations.",
      ).toEqual(baseline[module]?.declarations ?? []);
    });
  }
  for (const module of checkedModules) {
    it(`${module} documents named functions and methods`, () => {
      const path = fileURLToPath(new URL(module, import.meta.url));
      expect(
        analyzeModule(path).documentation,
        "Add JSDoc to named declarations and exported contract fields. Remove fixed identities from the baseline; never admit new undocumented declarations.",
      ).toEqual(baseline[module]?.documentation ?? []);
    });
  }
});
