import { mkdir, writeFile } from "node:fs/promises";
import { dirname } from "node:path";

import { resolvePrecommitMarkerPath } from "./precommit-marker.mjs";

const markerPath = resolvePrecommitMarkerPath();
const payload = JSON.stringify({
  createdAt: new Date().toISOString(),
  pid: process.pid,
});

await mkdir(dirname(markerPath), { recursive: true });
await writeFile(markerPath, payload, "utf8");
