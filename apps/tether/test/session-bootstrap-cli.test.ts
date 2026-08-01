import { describe, expect, it } from "vitest";

import { SessionBootstrapIdentityConflictError } from "../src/db.js";
import {
  parseSessionBootstrapCliOptions,
  projectSessionBootstrapCliError,
} from "../src/session-bootstrap-cli.js";

describe("session bootstrap CLI", () => {
  it("parses one exact durable identity mapping", () => {
    expect(
      parseSessionBootstrapCliOptions(
        ["--identity", "email-primary", "--session", "sess_email_primary"],
        { DATABASE_URL: "postgres://local.test/tether" },
      ),
    ).toEqual({
      databaseUrl: "postgres://local.test/tether",
      identityKey: "email-primary",
      sessionId: "sess_email_primary",
    });
  });

  it.each([
    [[], "session_bootstrap_arguments_invalid"],
    [
      ["--identity", "email-primary", "--identity", "duplicate"],
      "session_bootstrap_arguments_invalid",
    ],
    [
      ["--identity", "email-primary", "--session", "sess_email_primary"],
      "session_bootstrap_database_url_required",
    ],
  ] as const)("rejects invalid authority input", (args, reason) => {
    expect(() => parseSessionBootstrapCliOptions(args, {})).toThrow(reason);
  });

  it("redacts conflicts and unexpected failures to stable codes", () => {
    expect(projectSessionBootstrapCliError(new SessionBootstrapIdentityConflictError())).toBe(
      "session_bootstrap_identity_conflict",
    );
    expect(projectSessionBootstrapCliError(new Error("postgres://secret@database"))).toBe(
      "session_bootstrap_failed",
    );
  });
});
