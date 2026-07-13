import { readdir, readFile } from "node:fs/promises";
import { dirname, join, relative, resolve } from "node:path";
import { fileURLToPath } from "node:url";

import { describe, expect, it } from "vitest";

const testDir = dirname(fileURLToPath(import.meta.url));
const appRoot = resolve(testDir, "..");
const scanRoots = ["src", "test"] as const;
const scanFiles = ["package.json", "Dockerfile"] as const;
const forbiddenReferences = [
  ["@tether", ["email", "protocol"].join("-")].join("/"),
  ["packages", ["email", "protocol"].join("-")].join("/"),
  ["agents", "email"].join("/"),
  ["agent", "email"].join("-"),
] as const;

describe("email domain boundary", () => {
  it("keeps the core app free of email-owned packages and agent paths", async () => {
    const files = [
      ...(await Promise.all(scanRoots.map((root) => listFiles(resolve(appRoot, root))))).flat(),
      ...scanFiles.map((file) => resolve(appRoot, file)),
    ];
    const violations: string[] = [];

    for (const file of files) {
      const source = await readFile(file, "utf8");
      for (const forbiddenReference of forbiddenReferences) {
        if (source.includes(forbiddenReference)) {
          violations.push(`${relative(appRoot, file)} contains ${forbiddenReference}`);
        }
      }
    }

    expect(violations).toEqual([]);
  });
});

async function listFiles(root: string): Promise<readonly string[]> {
  const entries = await readdir(root, { withFileTypes: true });
  const files = await Promise.all(
    entries.map(async (entry) => {
      const path = join(root, entry.name);
      if (entry.isDirectory()) {
        return listFiles(path);
      }
      return entry.isFile() ? [path] : [];
    }),
  );
  return files.flat();
}
