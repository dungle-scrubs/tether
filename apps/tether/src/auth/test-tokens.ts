import type { AuthRole } from "./token.js";
import { mintAuthToken } from "./token.js";

export const testAuthSigningKid = "test";
export const testAuthSigningSecret = "test-auth-secret";

export interface MintTestAuthTokenInput {
  /** Unix expiration timestamp in seconds. Defaults far enough out for tests. */
  readonly exp?: number;
  /** Authenticated participant identity. */
  readonly participantId?: string;
  /** Authenticated role. */
  readonly role?: AuthRole;
  /** Scoped session id, or `*` for service-wide tests. */
  readonly sessionId?: string;
}

/** Mints deterministic auth tokens for local and e2e tests without exposing live secrets. */
export function mintTestAuthToken(input: MintTestAuthTokenInput = {}): string {
  return mintAuthToken(
    {
      exp: input.exp ?? 4_102_444_800,
      kid: testAuthSigningKid,
      participantId: input.participantId ?? "part_test",
      role: input.role ?? "admin",
      sessionId: input.sessionId ?? "*",
    },
    { [testAuthSigningKid]: testAuthSigningSecret },
  );
}
