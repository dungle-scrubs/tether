import { productionStartupStatus } from "./production-selection.js";

const status = productionStartupStatus();
process.stderr.write(`${JSON.stringify({ module: "session-summary-worker", ...status })}\n`);
