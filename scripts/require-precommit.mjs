import { readFile, rm } from "node:fs/promises";
import { join } from "node:path";

const markerPath = join(".git", "tether-precommit-ok");
const maxMarkerAgeMs = 2 * 60 * 1000;

let marker;
try {
  marker = JSON.parse(await readFile(markerPath, "utf8"));
} catch {
  console.error("Commit blocked: pre-commit verification did not run. Do not use --no-verify.");
  process.exit(1);
}

const createdAt =
  typeof marker === "object" && marker !== null && typeof marker.createdAt === "string"
    ? Date.parse(marker.createdAt)
    : Number.NaN;

await rm(markerPath, { force: true });

if (!Number.isFinite(createdAt) || Date.now() - createdAt > maxMarkerAgeMs) {
  console.error(
    "Commit blocked: pre-commit verification marker is stale. Run a normal commit without --no-verify.",
  );
  process.exit(1);
}
