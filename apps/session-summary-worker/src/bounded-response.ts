import { SessionSummaryWorkerError } from "./errors.js";

/** Reads a response body without ever retaining more than the configured byte limit. */
export async function readBoundedResponseText(
  response: Response,
  maximumBytes: number,
  failureCode: "generation_unavailable" | "invalid_output",
): Promise<string> {
  if (!response.body) {
    throw new SessionSummaryWorkerError(failureCode, "HTTP response body was missing");
  }
  const reader = response.body.getReader();
  const chunks: Uint8Array[] = [];
  let totalBytes = 0;
  try {
    while (true) {
      const chunk = await reader.read();
      if (chunk.done) {
        break;
      }
      totalBytes += chunk.value.byteLength;
      if (totalBytes > maximumBytes) {
        await reader.cancel();
        throw new SessionSummaryWorkerError(
          failureCode,
          "HTTP response exceeded its raw byte limit",
        );
      }
      chunks.push(chunk.value);
    }
  } finally {
    reader.releaseLock();
  }
  const body = new Uint8Array(totalBytes);
  let offset = 0;
  for (const chunk of chunks) {
    body.set(chunk, offset);
    offset += chunk.byteLength;
  }
  return new TextDecoder("utf-8", { fatal: true }).decode(body);
}
