#!/usr/bin/env node

/** Builds compiled browser fixtures for standalone tests outside the Turbo task graph. */

import { execFileSync } from "node:child_process";

if (process.env.TURBO_HASH === undefined) {
  execFileSync("pnpm", ["--filter", "@dungle-scrubs/tether-protocol", "build"], {
    stdio: "inherit",
  });
  execFileSync("pnpm", ["run", "build"], { stdio: "inherit" });
}
