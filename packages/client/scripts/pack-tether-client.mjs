#!/usr/bin/env node

/**
 * Builds the vendorable Tether client package consumed by external repos.
 *
 * The workspace package is `@dungle-scrubs/tether-client`, but downstream repos vendor a
 * self-contained package named `tether` with a `tether/client` export. The
 * JavaScript bundle inlines workspace code such as `@dungle-scrubs/tether-protocol`; only
 * third-party runtime dependencies remain external.
 */

import { execFileSync } from "node:child_process";
import { copyFileSync, mkdirSync, readdirSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { basename, dirname, join, relative, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { build } from "esbuild";

const scriptDir = dirname(fileURLToPath(import.meta.url));
const clientDir = resolve(scriptDir, "..");
const repoRoot = resolve(clientDir, "../..");
const protocolDistDir = join(repoRoot, "packages/protocol/dist");
const clientDistDir = join(clientDir, "dist");
const packDir = join(clientDir, ".pack/tether");
const packDistDir = join(packDir, "dist");
const packageVersion = JSON.parse(readFileSync(join(clientDir, "package.json"), "utf8")).version;
const tarballName = `tether-${packageVersion}.tgz`;
const tarballPath = join(clientDir, tarballName);

rmSync(packDir, { force: true, recursive: true });
rmSync(tarballPath, { force: true });
mkdirSync(packDistDir, { recursive: true });

await build({
  bundle: true,
  entryPoints: [join(clientDir, "src/index.ts")],
  external: ["@opentelemetry/api", "effect", "ws", "zod"],
  format: "esm",
  logLevel: "info",
  outfile: join(packDistDir, "client.js"),
  platform: "node",
  sourcemap: false,
  target: "node22",
});

for (const fileName of readdirSync(clientDistDir)) {
  if (!fileName.endsWith(".d.ts")) {
    continue;
  }

  const sourcePath = join(clientDistDir, fileName);
  const targetPath = join(packDistDir, fileName === "index.d.ts" ? "client.d.ts" : fileName);
  const declaration = readFileSync(sourcePath, "utf8").replaceAll(
    'from "@dungle-scrubs/tether-protocol"',
    'from "./protocol-types.js"',
  );
  writeFileSync(targetPath, declaration);
}

for (const fileName of readdirSync(protocolDistDir)) {
  if (!fileName.endsWith(".d.ts")) {
    continue;
  }

  const sourcePath = join(protocolDistDir, fileName);
  const targetName = fileName === "index.d.ts" ? "protocol-types.d.ts" : `protocol-${fileName}`;
  copyFileSync(sourcePath, join(packDistDir, targetName));
}

patchProtocolTypeBarrels();

writeFileSync(
  join(packDir, "package.json"),
  `${JSON.stringify(
    {
      name: "tether",
      version: packageVersion,
      type: "module",
      exports: {
        "./client": {
          import: "./dist/client.js",
          types: "./dist/client.d.ts",
        },
      },
      dependencies: {
        "@opentelemetry/api": "^1.9.1",
        effect: "^3.21.2",
        ws: "^8.20.1",
        zod: "^4.4.3",
      },
    },
    null,
    2,
  )}\n`,
);

execFileSync("pnpm", ["pack", "--pack-destination", clientDir], {
  cwd: packDir,
  stdio: "inherit",
});

console.log(`Wrote ${relative(repoRoot, tarballPath)}`);

function patchProtocolTypeBarrels() {
  for (const fileName of readdirSync(packDistDir)) {
    if (!fileName.startsWith("protocol-") || !fileName.endsWith(".d.ts")) {
      continue;
    }

    const path = join(packDistDir, fileName);
    let declaration = readFileSync(path, "utf8");

    for (const targetFileName of readdirSync(packDistDir)) {
      if (!targetFileName.startsWith("protocol-") || !targetFileName.endsWith(".d.ts")) {
        continue;
      }

      const originalSpecifier = `./${targetFileName.replace(/^protocol-/, "").replace(/\.d\.ts$/, ".js")}`;
      const stagedSpecifier = `./${basename(targetFileName, ".d.ts")}.js`;
      declaration = declaration.replaceAll(
        `from "${originalSpecifier}"`,
        `from "${stagedSpecifier}"`,
      );
      declaration = declaration.replaceAll(
        `export * from "${originalSpecifier}"`,
        `export * from "${stagedSpecifier}"`,
      );
    }

    writeFileSync(path, declaration);
  }
}
