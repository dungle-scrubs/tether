import { createHash, randomBytes, timingSafeEqual } from "node:crypto";

/** Produces one 256-bit unpadded base64url credential. */
export function randomOpaqueCredential(): string {
  return randomBytes(32).toString("base64url");
}

/** Hashes an opaque credential before it crosses a persistence boundary. */
export function hashOpaqueCredential(value: string): string {
  return createHash("sha256").update(value).digest("hex");
}

/** Compares fixed-width SHA-256 hex values without leaking a matching prefix. */
export function opaqueCredentialHashesEqual(actual: string, expected: string): boolean {
  const actualBuffer = Buffer.from(actual, "hex");
  const expectedBuffer = Buffer.from(expected, "hex");
  return (
    actualBuffer.length === 32 &&
    expectedBuffer.length === 32 &&
    timingSafeEqual(actualBuffer, expectedBuffer)
  );
}

/** Aligns durable authentication timestamps with signed-token second precision. */
export function wholeAuthSecond(value: Date): Date {
  return new Date(Math.floor(value.getTime() / 1_000) * 1_000);
}
