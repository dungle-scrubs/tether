import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import ts from "typescript";
import { describe, expect, it } from "vitest";
import { analyzeDocumentation, findingScope, visit } from "../../../tooling/conventions/jsdoc.js";

/** Files whose convention identities are ratcheted by the client test command. */
const checkedModules = [
  "../src/participant-rest-replay.ts",
  "../src/participant-runtime-client.ts",
  "../src/participant-claimable-task-runner.ts",
  "../src/participant-recovery-task-state.ts",
  "../src/rest-participant-task-client.ts",
  "./participant-runtime-client.test.ts",
  "./participant-rest-replay.test.ts",
  "./rest-participant-task-client.test.ts",
  "./public-api.test.ts",
  "./participant-rest-replay-style.test.ts",
];

/** Finds the module-level declaration containing a node. */
function moduleStatement(node: ts.Node, source: ts.SourceFile): ts.Node {
  let current = node;
  while (current.parent && current.parent !== source) current = current.parent;
  return current;
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
  });
  return { declarations: declarations.sort(), documentation: analyzeDocumentation(source, file) };
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
    let findings: ReturnType<typeof analyzeModule> | undefined;
    /** Loads and analyzes this module only when its first convention assertion runs. */
    const moduleFindings = () => (findings ??= analyzeModule(path));
    it(`${module} declares module dependencies before use`, () => {
      expect(
        moduleFindings().declarations,
        "Move each dependency above every referencing declaration. Remove fixed identities from the baseline; never replace them with new violations.",
      ).toEqual(baseline[module]?.declarations ?? []);
    });
    it(`${module} documents named functions and methods`, () => {
      expect(
        moduleFindings().documentation,
        "Add JSDoc to named declarations and exported contract fields. Remove fixed identities from the baseline; never admit new undocumented declarations.",
      ).toEqual(baseline[module]?.documentation ?? []);
    });
  }
});
