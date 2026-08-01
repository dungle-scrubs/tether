#!/usr/bin/env node

/**
 * Packs the browser client, inspects its allowlist, and compiles a clean
 * consumer against the packed browser and protocol declarations.
 */

import { execFileSync } from "node:child_process";
import { mkdtempSync, mkdirSync, readFileSync, readdirSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join, relative, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const scriptDirectory = dirname(fileURLToPath(import.meta.url));
const browserDirectory = resolve(scriptDirectory, "..");
const repositoryRoot = resolve(browserDirectory, "../..");
const protocolDirectory = join(repositoryRoot, "packages/protocol");
const temporaryDirectory = mkdtempSync(join(tmpdir(), "tether-browser-package-"));
const consumerDirectory = join(temporaryDirectory, "consumer");

try {
  const protocolTarball = pack(protocolDirectory, temporaryDirectory);
  const browserTarball = pack(browserDirectory, temporaryDirectory);
  verifyArchive(browserTarball);
  verifyBrowserOnlySources(browserDirectory);
  mkdirSync(consumerDirectory);
  writeFileSync(
    join(consumerDirectory, "package.json"),
    `${JSON.stringify(
      {
        name: "tether-browser-clean-consumer",
        packageManager: "pnpm@11.1.3",
        private: true,
        type: "module",
        dependencies: {
          "@dungle-scrubs/tether-browser": fileDependency(consumerDirectory, browserTarball),
        },
        devDependencies: {
          typescript: "6.0.3",
        },
      },
      null,
      2,
    )}\n`,
  );
  writeFileSync(
    join(consumerDirectory, "pnpm-workspace.yaml"),
    [
      "packages:",
      '  - "."',
      "overrides:",
      `  "@dungle-scrubs/tether-protocol": "${fileDependency(consumerDirectory, protocolTarball)}"`,
      "",
    ].join("\n"),
  );
  writeFileSync(
    join(consumerDirectory, "tsconfig.json"),
    `${JSON.stringify(
      {
        compilerOptions: {
          lib: ["DOM", "ES2022"],
          module: "NodeNext",
          moduleResolution: "NodeNext",
          noEmit: true,
          strict: true,
          target: "ES2022",
        },
        include: ["index.ts"],
      },
      null,
      2,
    )}\n`,
  );
  writeFileSync(
    join(consumerDirectory, "index.ts"),
    [
      'import { BrowserOperatorClient, type BrowserSessionStreamDebugInfo } from "@dungle-scrubs/tether-browser";',
      "",
      'const client = new BrowserOperatorClient({ csrfToken: "c".repeat(43), serviceUrl: "https://hub.example.test" });',
      "const debug: BrowserSessionStreamDebugInfo | null = null;",
      "void client;",
      "void debug;",
      "",
    ].join("\n"),
  );
  execFileSync("pnpm", ["install", "--prefer-offline", "--ignore-scripts"], {
    cwd: consumerDirectory,
    stdio: "inherit",
  });
  execFileSync("pnpm", ["exec", "tsc", "--project", "tsconfig.json"], {
    cwd: consumerDirectory,
    stdio: "pipe",
  });
  execFileSync(
    "node",
    ["--input-type=module", "--eval", 'await import("@dungle-scrubs/tether-browser");'],
    { cwd: consumerDirectory, stdio: "pipe" },
  );
  console.log("Verified packed browser client and clean consumer declarations.");
} finally {
  rmSync(temporaryDirectory, { force: true, recursive: true });
}

/** Packs one workspace package and returns its newly created tarball. */
function pack(packageDirectory, destination) {
  const before = new Set(readdirSync(destination));
  execFileSync("pnpm", ["pack", "--pack-destination", destination], {
    cwd: packageDirectory,
    stdio: "pipe",
  });
  const tarball = readdirSync(destination).find(
    (entry) => entry.endsWith(".tgz") && !before.has(entry),
  );
  if (tarball === undefined) {
    throw new Error(`No tarball produced for ${packageDirectory}`);
  }
  return join(destination, tarball);
}

/** Allows only published package metadata and compiled runtime artifacts. */
function verifyArchive(tarball) {
  const entries = execFileSync("tar", ["-tzf", tarball], { encoding: "utf8" })
    .trim()
    .split("\n")
    .filter(Boolean);
  const unexpected = entries.filter(
    (entry) =>
      entry !== "package/LICENSE" &&
      entry !== "package/package.json" &&
      !entry.startsWith("package/dist/"),
  );
  if (unexpected.length > 0) {
    throw new Error(`Unexpected packed browser files: ${unexpected.join(", ")}`);
  }
  for (const required of ["package/dist/index.js", "package/dist/index.d.ts"]) {
    if (!entries.includes(required)) {
      throw new Error(`Packed browser client is missing ${required}`);
    }
  }
}

/** Rejects Node transport imports and private domain language in published sources. */
function verifyBrowserOnlySources(packageDirectory) {
  const files = [
    ...sourceFiles(join(packageDirectory, "src")),
    ...sourceFiles(join(packageDirectory, "dist")),
  ];
  for (const path of files) {
    const source = readFileSync(path, "utf8");
    if (/from\s+["']ws["']|require\(["']ws["']\)|node:/u.test(source)) {
      throw new Error(`Node runtime dependency found in ${path}`);
    }
    if (/\bemail\b/iu.test(source)) {
      throw new Error(`Private domain semantics found in ${path}`);
    }
  }
}

/** Recursively lists TypeScript, JavaScript, and declaration sources. */
function sourceFiles(directory) {
  return readdirSync(directory, { withFileTypes: true }).flatMap((entry) => {
    const path = join(directory, entry.name);
    if (entry.isDirectory()) {
      return sourceFiles(path);
    }
    return /\.(?:d\.ts|js|ts)$/u.test(entry.name) ? [path] : [];
  });
}

/** Creates a portable relative file dependency for the scratch consumer. */
function fileDependency(fromDirectory, tarball) {
  return `file:${relative(fromDirectory, tarball).replaceAll("\\", "/")}`;
}
