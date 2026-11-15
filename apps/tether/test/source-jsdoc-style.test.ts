import { readdirSync, readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import ts from "typescript";
import { describe, expect, it } from "vitest";
import { analyzeDocumentation } from "../../../tooling/conventions/jsdoc.js";

/** Exact legacy identities from origin/main at cb8b6a51362ffc4837b9ebb54d7556d4c0196f9c. */
const baseline = JSON.parse(
  readFileSync(new URL("./source-jsdoc-conventions.baseline.json", import.meta.url), "utf8"),
) as Record<string, readonly string[]>;

/** Discover source files recursively so new modules start with an empty baseline. */
const sourceRoot = new URL("../src/", import.meta.url);

/** Source paths relative to the app's src directory; there are currently no generated files. */
const checkedModules = readdirSync(sourceRoot, { encoding: "utf8", recursive: true })
  .filter((file) => file.endsWith(".ts") && !file.endsWith(".d.ts"))
  .sort();

/** Parses source with parents so the shared analyzer can identify enclosing declarations. */
function moduleFindings(module: string): string[] {
  const path = fileURLToPath(new URL(module, sourceRoot));
  const source = ts.createSourceFile(
    path,
    readFileSync(path, "utf8"),
    ts.ScriptTarget.Latest,
    true,
  );
  return analyzeDocumentation(source, `apps/tether/src/${module}`);
}

describe("Tether source JSDoc conventions", () => {
  it("contains no baseline entries for removed source files", () => {
    expect(
      Object.keys(baseline).filter((file) => !checkedModules.includes(file)),
      "Remove baseline entries for deleted source files; never grow the baseline.",
    ).toEqual([]);
  });

  for (const module of checkedModules) {
    it(`${module} documents named functions and methods`, () => {
      expect(
        moduleFindings(module),
        "Add JSDoc to named declarations and contract fields. Remove fixed identities from the baseline; never grow the baseline to admit new undocumented declarations.",
      ).toEqual(baseline[module] ?? []);
    });
  }
});
