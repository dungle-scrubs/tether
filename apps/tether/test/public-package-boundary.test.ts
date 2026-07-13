import { existsSync, readdirSync, readFileSync } from "node:fs";
import { resolve } from "node:path";

import { describe, expect, it } from "vitest";
import { parse } from "yaml";
import { z } from "zod";

const repoRoot = resolve(import.meta.dirname, "../../..");
const packageManifestSchema = z.object({
  dependencies: z.record(z.string(), z.string()).optional(),
  devDependencies: z.record(z.string(), z.string()).optional(),
});
const workspaceSchema = z.object({
  packages: z.array(z.string()),
});
const privatePackageNames = [
  ["@tether", "commands"].join("/"),
  ["@tether", ["email", "protocol"].join("-")].join("/"),
  ["@tether", ["smoke", "client"].join("-")].join("/"),
  ["agent", "coordinator"].join("-"),
  ["agent", "email"].join("-"),
  ["agent", "media"].join("-"),
  ["agent", "openai"].join("-"),
  ["web", "search"].join("-"),
] as const;
const privatePublicTreePaths = [
  "agents",
  ["packages", "commands"].join("/"),
  ["packages", ["email", "protocol"].join("-")].join("/"),
  ["packages", ["smoke", "client"].join("-")].join("/"),
  ["packages", ["web", "search"].join("-")].join("/"),
] as const;

describe("public package boundary", () => {
  it("keeps public manifests free of private workspace packages", () => {
    const manifests = ["package.json", ...readWorkspacePackageManifestPaths()];

    for (const manifestPath of manifests) {
      const manifest = readPackageManifest(manifestPath);
      const dependencyNames = new Set([
        ...Object.keys(manifest.dependencies ?? {}),
        ...Object.keys(manifest.devDependencies ?? {}),
      ]);

      expect(privatePackageNames.filter((packageName) => dependencyNames.has(packageName))).toEqual(
        [],
      );
    }
  });

  it("keeps private package source directories out of the public tree", () => {
    expect(
      privatePublicTreePaths.filter((treePath) => existsSync(resolve(repoRoot, treePath))),
    ).toEqual([]);
  });
});

/** Reads and validates one package manifest from the repository root. */
function readPackageManifest(path: string): z.infer<typeof packageManifestSchema> {
  return packageManifestSchema.parse(JSON.parse(readFileSync(resolve(repoRoot, path), "utf8")));
}

/** Reads public workspace package manifest paths from pnpm workspace membership. */
function readWorkspacePackageManifestPaths(): readonly string[] {
  const workspace = workspaceSchema.parse(
    parse(readFileSync(resolve(repoRoot, "pnpm-workspace.yaml"), "utf8")),
  );
  return workspace.packages.flatMap((workspacePath) => {
    if (!workspacePath.endsWith("/*")) {
      return [`${workspacePath}/package.json`];
    }
    const parentPath = workspacePath.slice(0, -2);
    return readdirSync(resolve(repoRoot, parentPath), { withFileTypes: true })
      .filter((entry) => entry.isDirectory())
      .map((entry) => `${parentPath}/${entry.name}/package.json`)
      .filter((manifestPath) => existsSync(resolve(repoRoot, manifestPath)));
  });
}
