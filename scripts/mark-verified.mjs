import { mkdir, writeFile } from "node:fs/promises";
import { dirname, join } from "node:path";

const markerPath = join(".git", "tether-precommit-ok");
const payload = JSON.stringify({
  createdAt: new Date().toISOString(),
  pid: process.pid,
});

await mkdir(dirname(markerPath), { recursive: true });
await writeFile(markerPath, payload, "utf8");
