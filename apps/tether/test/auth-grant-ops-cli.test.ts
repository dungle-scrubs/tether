import { access, constants, mkdtemp, readFile, stat, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { describe, expect, it, vi } from "vitest";

import {
  inventorySessionGrants,
  issueGrantOpsGrant,
  parseGrantOpsCliOptions,
  projectGrantOpsCliError,
  revokeGrantOpsGrant,
  runGrantOpsCli,
} from "../src/auth/grant-ops-cli.js";
import { createAuthPersistenceStores } from "../src/auth/db-grant-stores.js";
import type { AuthGrantRecord, AuthPersistenceStores } from "../src/auth/grant-stores.js";
import { serviceWideAuthGrantScope } from "../src/auth/grant-stores.js";
import { verifyAuthGrantToken } from "../src/auth/grant-token.js";
import type { DatabasePool } from "../src/db.js";

const secret = "grant-ops-test-secret-value";
const issuer = "https://auth.tether.test";
const kid = "default";
const issueEnvironment = {
  AUTH_GRANT_BOOTSTRAP_COMPATIBILITY_CONFIRMED: "true",
  AUTH_ISSUER: issuer,
  AUTH_SIGNING_KID: kid,
  AUTH_SIGNING_SECRET: secret,
  DATABASE_URL: "postgres://tether:test@127.0.0.1/tether",
};

/** Injectable persistence behaviors the focused tests replace per case. */
interface FakeStoreOverrides {
  readonly createGrantWithAudit?: AuthPersistenceStores["createGrantWithAudit"];
  readonly listForSessionInventory?: AuthPersistenceStores["grants"]["listForSessionInventory"];
  readonly revokeGrantWithAudit?: AuthPersistenceStores["revokeGrantWithAudit"];
}

/** Builds a transaction-shaped in-memory persistence seam for CLI unit tests. */
function fakeStores(overrides: FakeStoreOverrides = {}): AuthPersistenceStores & {
  readonly records: Map<string, AuthGrantRecord>;
} {
  const records = new Map<string, AuthGrantRecord>();
  return {
    audits: { listForGrant: async () => [] },
    createGrantWithAudit:
      overrides.createGrantWithAudit ??
      vi.fn(async ({ grant }: { readonly grant: AuthGrantRecord }) => {
        records.set(grant.jti, grant);
      }),
    createTaskGrantWithAudit: vi.fn(async () => undefined),
    grants: {
      findManyByJti: async () => [],
      findByJti: async (jti: string) => records.get(jti) ?? null,
      list: async (limit: number) => [...records.values()].slice(0, limit),
      listForSessionInventory:
        overrides.listForSessionInventory ??
        (async (input: { readonly sessionScope: string; readonly subject: string | null }) =>
          [...records.values()].filter(
            (record) =>
              (record.sessionScope === input.sessionScope ||
                record.sessionScope === serviceWideAuthGrantScope) &&
              (input.subject === null || record.subject === input.subject),
          )),
    },
    records,
    revokeGrantWithAudit:
      overrides.revokeGrantWithAudit ??
      vi.fn(async ({ jti, revokedAt }: { readonly jti: string; readonly revokedAt: Date }) => {
        const record = records.get(jti);
        if (!record) return { grant: null, status: "not_found" } as const;
        if (record.revokedAt) return { grant: record, status: "already_revoked" } as const;
        const revoked = { ...record, revokedAt };
        records.set(jti, revoked);
        return { grant: revoked, status: "revoked" } as const;
      }),
    revokeTaskGrantWithAudit: vi.fn(async () => ({ grant: null, status: "not_found" }) as const),
    taskGrantAudits: { listForTaskGrant: async () => [] },
    taskGrants: {
      findByJti: async () => null,
      list: async () => [],
      listLiveForSubject: async () => [],
    },
    tickets: {
      consume: async () => null,
      create: async () => undefined,
      findByHash: async () => null,
    },
  };
}

/** Builds one durable-shaped grant record for inventory fixtures. */
function grantRecord(
  overrides: Partial<AuthGrantRecord> & { readonly jti: string },
): AuthGrantRecord {
  const issuedAt = overrides.issuedAt ?? new Date("2026-10-01T00:00:00.000Z");
  return {
    audience: "tether-rest",
    expiresAt: overrides.expiresAt ?? new Date(issuedAt.getTime() + 3_600 * 1_000),
    issuedAt,
    issuer: overrides.issuer ?? issuer,
    jti: overrides.jti,
    kid: overrides.kid ?? kid,
    metadata: overrides.metadata ?? { requestId: null, source: "admin" },
    revokedAt: overrides.revokedAt ?? null,
    role: overrides.role ?? "observer",
    sessionScope: overrides.sessionScope ?? "sess_ops",
    subject: overrides.subject ?? "part_ops",
  };
}

async function expectMissing(path: string): Promise<void> {
  await expect(access(path, constants.F_OK)).rejects.toThrow();
}

async function temporaryDirectory(): Promise<string> {
  return mkdtemp(join(tmpdir(), "grant-ops-test-"));
}

describe("grant-ops CLI parsing", () => {
  it("parses one strict observer issue with bounded ttl and gate", () => {
    expect(
      parseGrantOpsCliOptions(
        [
          "issue",
          "--session",
          "sess_ops",
          "--subject",
          "part_ops",
          "--role",
          "observer",
          "--out",
          "/run/tether/grant.txt",
          "--ttl",
          "2d",
        ],
        issueEnvironment,
      ),
    ).toEqual({
      command: "issue",
      compatibilityConfirmed: true,
      databaseUrl: "postgres://tether:test@127.0.0.1/tether",
      issuer,
      kid,
      outputPath: "/run/tether/grant.txt",
      role: "observer",
      secret,
      sessionScope: "sess_ops",
      subject: "part_ops",
      ttlSeconds: 172_800,
    });
  });

  it("defaults ttl to 24 hours and accepts participant", () => {
    expect(
      parseGrantOpsCliOptions(
        [
          "issue",
          "--session",
          "sess_ops",
          "--subject",
          "part_ops",
          "--role",
          "participant",
          "--out",
          "/run/tether/grant.txt",
        ],
        issueEnvironment,
      ),
    ).toMatchObject({ role: "participant", ttlSeconds: 86_400 });
  });

  it("requires the host-local compatibility confirmation gate", () => {
    expect(() =>
      parseGrantOpsCliOptions(
        [
          "issue",
          "--session",
          "sess_ops",
          "--subject",
          "part_ops",
          "--role",
          "observer",
          "--out",
          "/run/tether/grant.txt",
        ],
        { ...issueEnvironment, AUTH_GRANT_BOOTSTRAP_COMPATIBILITY_CONFIRMED: "false" },
      ),
    ).toThrow("grant_ops_compatibility_unconfirmed");
  });

  it("refuses admin and unknown roles", () => {
    for (const role of ["admin", "root"]) {
      expect(() =>
        parseGrantOpsCliOptions(
          [
            "issue",
            "--session",
            "sess_ops",
            "--subject",
            "part_ops",
            "--role",
            role,
            "--out",
            "/run/tether/grant.txt",
          ],
          issueEnvironment,
        ),
      ).toThrow("grant_ops_role_forbidden");
    }
    expect(() =>
      parseGrantOpsCliOptions(
        [
          "issue",
          "--session",
          "sess_ops",
          "--subject",
          "part_ops",
          "--role",
          " ",
          "--out",
          "/run/tether/grant.txt",
        ],
        issueEnvironment,
      ),
    ).toThrow("grant_ops_role_required");
  });

  it("refuses service-wide and malformed session scopes", () => {
    expect(() =>
      parseGrantOpsCliOptions(
        [
          "issue",
          "--session",
          serviceWideAuthGrantScope,
          "--subject",
          "part_ops",
          "--role",
          "observer",
          "--out",
          "/run/tether/grant.txt",
        ],
        issueEnvironment,
      ),
    ).toThrow("grant_ops_session_scope_invalid");
    expect(() =>
      parseGrantOpsCliOptions(
        [
          "issue",
          "--session",
          " ",
          "--subject",
          "part_ops",
          "--role",
          "observer",
          "--out",
          "/run/tether/grant.txt",
        ],
        issueEnvironment,
      ),
    ).toThrow("grant_ops_session_required");
  });

  it("bounds the ttl to seven days", () => {
    const parseIssueWithTtl = (ttl: string) => {
      const options = parseGrantOpsCliOptions(
        [
          "issue",
          "--session",
          "sess_ops",
          "--subject",
          "part_ops",
          "--role",
          "observer",
          "--out",
          "/run/tether/grant.txt",
          "--ttl",
          ttl,
        ],
        issueEnvironment,
      );
      if (options.command !== "issue") {
        throw new Error("expected an issue command");
      }
      return options;
    };
    expect(parseIssueWithTtl("604800").ttlSeconds).toBe(604_800);
    expect(parseIssueWithTtl("7d").ttlSeconds).toBe(604_800);
    for (const invalid of ["604801", "8d", "0", "-1", "1x"]) {
      expect(() => parseIssueWithTtl(invalid)).toThrow("grant_ops_ttl_invalid");
    }
  });

  it("requires the signing secret and issuer for issue", () => {
    const args = [
      "issue",
      "--session",
      "sess_ops",
      "--subject",
      "part_ops",
      "--role",
      "observer",
      "--out",
      "/run/tether/grant.txt",
    ];
    expect(() =>
      parseGrantOpsCliOptions(args, { ...issueEnvironment, AUTH_SIGNING_SECRET: "" }),
    ).toThrow("grant_ops_auth_signing_secret_required");
    expect(() => parseGrantOpsCliOptions(args, { ...issueEnvironment, AUTH_ISSUER: " " })).toThrow(
      "grant_ops_auth_issuer_required",
    );
  });

  it("rejects unknown, duplicate, and valueless flags", () => {
    for (const badArgs of [
      [
        "issue",
        "--session",
        "sess_ops",
        "--subject",
        "part_ops",
        "--role",
        "observer",
        "--out",
        "/run/tether/grant.txt",
        "--extra",
        "1",
      ],
      [
        "issue",
        "--session",
        "sess_ops",
        "--subject",
        "part_ops",
        "--role",
        "observer",
        "--out",
        "/run/tether/grant.txt",
        "--out",
        "/other.txt",
      ],
      ["issue", "--session", "sess_ops", "--subject", "part_ops", "--role", "observer", "--out"],
      ["issue", "--session", "sess_ops", "--subject", "part_ops", "--role", "observer"],
    ]) {
      expect(() => parseGrantOpsCliOptions(badArgs, issueEnvironment)).toThrow(
        "grant_ops_arguments_invalid",
      );
    }
  });

  it("parses inventory with an optional subject filter", () => {
    expect(
      parseGrantOpsCliOptions(["inventory", "--session", "sess_ops"], {
        DATABASE_URL: issueEnvironment.DATABASE_URL,
      }),
    ).toEqual({
      acceptedKids: [],
      command: "inventory",
      databaseUrl: "postgres://tether:test@127.0.0.1/tether",
      issuer: null,
      sessionScope: "sess_ops",
      subject: null,
    });
    expect(
      parseGrantOpsCliOptions(["inventory", "--session", "sess_ops", "--subject", "part_ops"], {
        AUTH_SIGNING_KID: "rotated",
        AUTH_SIGNING_SECRET: secret,
        AUTH_ISSUER: issuer,
        DATABASE_URL: issueEnvironment.DATABASE_URL,
      }),
    ).toMatchObject({ acceptedKids: ["rotated"], issuer, subject: "part_ops" });
  });

  it("refuses service-wide inventory scope", () => {
    expect(() =>
      parseGrantOpsCliOptions(["inventory", "--session", "*"], {
        DATABASE_URL: issueEnvironment.DATABASE_URL,
      }),
    ).toThrow("grant_ops_session_scope_invalid");
  });

  it("reads accepted rotation key names and never exposes their values", () => {
    const parsed = parseGrantOpsCliOptions(["inventory", "--session", "sess_ops"], {
      AUTH_ACCEPTED_SIGNING_SECRETS: JSON.stringify({ retired: "rotation-secret" }),
      AUTH_ISSUER: issuer,
      AUTH_SIGNING_KID: kid,
      AUTH_SIGNING_SECRET: secret,
      DATABASE_URL: issueEnvironment.DATABASE_URL,
    });
    expect(parsed).toMatchObject({ acceptedKids: [kid, "retired"], issuer });
    expect(JSON.stringify(parsed)).not.toContain("rotation-secret");
    expect(() =>
      parseGrantOpsCliOptions(["inventory", "--session", "sess_ops"], {
        AUTH_ACCEPTED_SIGNING_SECRETS: "{private-invalid-json",
        DATABASE_URL: issueEnvironment.DATABASE_URL,
      }),
    ).toThrow("grant_ops_accepted_keys_invalid");
  });

  it("parses revoke with a bounded reason and validates the jti", () => {
    expect(
      parseGrantOpsCliOptions(
        ["revoke", "--jti", "grant_abcd1234", "--reason", "security-response"],
        { DATABASE_URL: issueEnvironment.DATABASE_URL },
      ),
    ).toEqual({
      command: "revoke",
      databaseUrl: "postgres://tether:test@127.0.0.1/tether",
      jti: "grant_abcd1234",
      reasonCode: "security-response",
    });
    expect(() =>
      parseGrantOpsCliOptions(["revoke", "--jti", "tgrant_abcd1234"], {
        DATABASE_URL: issueEnvironment.DATABASE_URL,
      }),
    ).toThrow("grant_ops_jti_invalid");
    expect(() =>
      parseGrantOpsCliOptions(["revoke", "--jti", "grant_abcd1234", "--reason", "bootstrap"], {
        DATABASE_URL: issueEnvironment.DATABASE_URL,
      }),
    ).toThrow("grant_ops_reason_invalid");
  });

  it("rejects unknown commands and offers help", () => {
    expect(() =>
      parseGrantOpsCliOptions(["rotate"], { DATABASE_URL: issueEnvironment.DATABASE_URL }),
    ).toThrow("grant_ops_command_invalid");
    expect(parseGrantOpsCliOptions([], issueEnvironment)).toEqual({ command: "help" });
    expect(parseGrantOpsCliOptions(["help"], issueEnvironment)).toEqual({ command: "help" });
  });

  it("prints usage, including the crash gap, without opening a database", async () => {
    const output: string[] = [];
    await runGrantOpsCli([], issueEnvironment, (value) => output.push(value));
    const text = output.join("");
    expect(text).toContain("Crash gap");
    expect(text).toContain("grant_ops_output_failed_revoked");
    expect(text).not.toContain("tgr2.");
  });
});

describe("grant-ops issue", () => {
  it("writes the bearer only to a new 0600 file and returns metadata only", async () => {
    const directory = await temporaryDirectory();
    const outputPath = join(directory, "grant.txt");
    const createGrantWithAudit = vi.fn(async ({ grant }: { readonly grant: AuthGrantRecord }) => {
      expect(JSON.stringify(grant)).not.toMatch(/tgr2\./u);
    });
    const stores = fakeStores({ createGrantWithAudit });

    const result = await issueGrantOpsGrant({
      activeKid: kid,
      issuer,
      outputPath,
      role: "observer",
      secrets: { [kid]: secret },
      sessionExists: async () => true,
      sessionScope: "sess_ops",
      stores,
      subject: "part_ops",
      ttlSeconds: 3_600,
    });

    const fileContent = await readFile(outputPath, "utf8");
    expect(fileContent).toMatch(/^tgr2\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\n$/u);
    const bearer = fileContent.trim();
    expect(
      verifyAuthGrantToken(bearer, { audience: "tether-rest", issuer, secrets: { [kid]: secret } })
        .sub,
    ).toBe("part_ops");
    expect((await stat(outputPath)).mode & 0o777).toBe(0o600);
    expect(JSON.stringify(result)).not.toContain(bearer);
    expect(JSON.stringify(result)).not.toContain("tgr2.");
    expect(result).toMatchObject({
      command: "issue",
      grant: { role: "observer", sessionScope: "sess_ops", subject: "part_ops" },
      outputPath,
    });
  });

  it("reserves the output exclusively and never overwrites an existing path", async () => {
    const directory = await temporaryDirectory();
    const outputPath = join(directory, "grant.txt");
    await writeFile(outputPath, "operator-content", { mode: 0o600 });
    const createGrantWithAudit = vi.fn(async () => undefined);
    const stores = fakeStores({ createGrantWithAudit });

    await expect(
      issueGrantOpsGrant({
        activeKid: kid,
        issuer,
        outputPath,
        role: "participant",
        secrets: { [kid]: secret },
        sessionExists: async () => true,
        sessionScope: "sess_ops",
        stores,
        subject: "part_ops",
        ttlSeconds: 3_600,
      }),
    ).rejects.toThrow("grant_ops_output_unavailable");
    expect(await readFile(outputPath, "utf8")).toBe("operator-content");
    expect(createGrantWithAudit).not.toHaveBeenCalled();
  });

  it("requires the session to already exist before reserving any output", async () => {
    const directory = await temporaryDirectory();
    const outputPath = join(directory, "grant.txt");
    const createGrantWithAudit = vi.fn(async () => undefined);
    const stores = fakeStores({ createGrantWithAudit });

    await expect(
      issueGrantOpsGrant({
        activeKid: kid,
        issuer,
        outputPath,
        role: "observer",
        secrets: { [kid]: secret },
        sessionExists: async () => false,
        sessionScope: "sess_ops",
        stores,
        subject: "part_ops",
        ttlSeconds: 3_600,
      }),
    ).rejects.toThrow("grant_ops_session_not_found");
    await expectMissing(outputPath);
    expect(createGrantWithAudit).not.toHaveBeenCalled();
  });

  it("removes the empty reserved file when the durable create fails", async () => {
    const directory = await temporaryDirectory();
    const outputPath = join(directory, "grant.txt");
    const stores = fakeStores({
      createGrantWithAudit: vi.fn(async () => {
        throw new Error("auth_grant_create_failed");
      }),
    });

    await expect(
      issueGrantOpsGrant({
        activeKid: kid,
        issuer,
        outputPath,
        role: "observer",
        secrets: { [kid]: secret },
        sessionExists: async () => true,
        sessionScope: "sess_ops",
        stores,
        subject: "part_ops",
        ttlSeconds: 3_600,
      }),
    ).rejects.toThrow("auth_grant_create_failed");
    await expectMissing(outputPath);
  });

  it("closes and removes the reservation when lifecycle setup throws", async () => {
    const directory = await temporaryDirectory();
    const outputPath = join(directory, "grant.txt");
    const stores = fakeStores();
    const input = {
      activeKid: kid,
      issuer,
      outputPath,
      role: "observer" as const,
      secrets: { [kid]: secret },
      sessionExists: async () => true,
      sessionScope: "sess_ops",
      stores,
      subject: "part_ops",
      ttlSeconds: 3_600,
      get now(): () => Date {
        throw new Error("lifecycle setup failure");
      },
    };
    await expect(issueGrantOpsGrant(input)).rejects.toThrow("lifecycle setup failure");
    await expectMissing(outputPath);
    expect(stores.createGrantWithAudit).not.toHaveBeenCalled();
  });

  it("revokes the committed grant by jti when output delivery fails", async () => {
    const directory = await temporaryDirectory();
    const outputPath = join(directory, "grant.txt");
    const stores = fakeStores();
    const revokeGrantWithAudit = vi.mocked(stores.revokeGrantWithAudit);

    await expect(
      issueGrantOpsGrant({
        activeKid: kid,
        deliver: async () => {
          throw new Error("EIO");
        },
        issuer,
        outputPath,
        role: "observer",
        secrets: { [kid]: secret },
        sessionExists: async () => true,
        sessionScope: "sess_ops",
        stores,
        subject: "part_ops",
        ttlSeconds: 3_600,
      }),
    ).rejects.toThrow(/^grant_ops_output_failed_revoked grant_[A-Za-z0-9_-]+$/u);

    expect(revokeGrantWithAudit).toHaveBeenCalledTimes(1);
    const revocation = revokeGrantWithAudit.mock.calls[0]?.[0];
    expect(revocation?.audit).toMatchObject({
      action: "grant.revoked",
      actorSubject: "grant-ops-cli",
      reasonCode: "security-response",
    });
    expect(revocation?.jti).toMatch(/^grant_[A-Za-z0-9_-]+$/u);
    await expectMissing(outputPath);
  });

  it("attempts audited revoke when the actual file sync fails", async () => {
    const directory = await temporaryDirectory();
    const outputPath = join(directory, "grant.txt");
    const stores = fakeStores();
    const syncFile = vi.fn(async () => {
      throw new Error(`fsync failed with ${secret}`);
    });

    await expect(
      issueGrantOpsGrant({
        activeKid: kid,
        issuer,
        outputPath,
        role: "observer",
        secrets: { [kid]: secret },
        sessionExists: async () => true,
        sessionScope: "sess_ops",
        stores,
        subject: "part_ops",
        syncFile,
        ttlSeconds: 3_600,
      }),
    ).rejects.toThrow(/^grant_ops_output_failed_revoked grant_[A-Za-z0-9_-]+$/u);
    expect(syncFile).toHaveBeenCalledTimes(1);
    expect(stores.revokeGrantWithAudit).toHaveBeenCalledTimes(1);
    await expectMissing(outputPath);
  });

  it("attempts audited revoke when the parent directory sync fails", async () => {
    const directory = await temporaryDirectory();
    const outputPath = join(directory, "grant.txt");
    const stores = fakeStores();
    const syncDirectory = vi.fn(async () => {
      throw new Error(`directory fsync failed with ${secret}`);
    });
    await expect(
      issueGrantOpsGrant({
        activeKid: kid,
        issuer,
        outputPath,
        role: "observer",
        secrets: { [kid]: secret },
        sessionExists: async () => true,
        sessionScope: "sess_ops",
        stores,
        subject: "part_ops",
        syncDirectory,
        ttlSeconds: 3_600,
      }),
    ).rejects.toThrow(/^grant_ops_output_failed_revoked grant_[A-Za-z0-9_-]+$/u);
    expect(syncDirectory).toHaveBeenCalledWith(outputPath);
    expect(stores.revokeGrantWithAudit).toHaveBeenCalledTimes(1);
    await expectMissing(outputPath);
  });

  it("reports a distinct bounded rollback failure when the revoke also fails", async () => {
    const directory = await temporaryDirectory();
    const outputPath = join(directory, "grant.txt");
    const stores = fakeStores({
      revokeGrantWithAudit: vi.fn(async () => {
        throw new Error("auth_grant_revoke_failed");
      }),
    });

    await expect(
      issueGrantOpsGrant({
        activeKid: kid,
        deliver: async () => {
          throw new Error("EIO");
        },
        issuer,
        outputPath,
        role: "observer",
        secrets: { [kid]: secret },
        sessionExists: async () => true,
        sessionScope: "sess_ops",
        stores,
        subject: "part_ops",
        ttlSeconds: 3_600,
      }),
    ).rejects.toThrow(/^grant_ops_output_rollback_failed grant_[A-Za-z0-9_-]+$/u);
    await expectMissing(outputPath);
  });

  it("does not report a missing revoke target as successfully revoked", async () => {
    const directory = await temporaryDirectory();
    const outputPath = join(directory, "grant.txt");
    const stores = fakeStores({
      revokeGrantWithAudit: vi.fn(async () => ({ grant: null, status: "not_found" }) as const),
    });
    await expect(
      issueGrantOpsGrant({
        activeKid: kid,
        deliver: async () => {
          throw new Error("delivery failed");
        },
        issuer,
        outputPath,
        role: "observer",
        secrets: { [kid]: secret },
        sessionExists: async () => true,
        sessionScope: "sess_ops",
        stores,
        subject: "part_ops",
        ttlSeconds: 3_600,
      }),
    ).rejects.toThrow(/^grant_ops_output_rollback_failed grant_[A-Za-z0-9_-]+$/u);
    await expectMissing(outputPath);
  });

  it("keeps an active orphan discoverable when output and rollback both fail", async () => {
    const directory = await temporaryDirectory();
    const outputPath = join(directory, "grant.txt");
    const stores = fakeStores({
      revokeGrantWithAudit: vi.fn(async () => {
        throw new Error("private persistence detail");
      }),
    });
    let failure = "";
    try {
      await issueGrantOpsGrant({
        activeKid: kid,
        deliver: async () => {
          throw new Error(`private delivery detail ${secret}`);
        },
        issuer,
        now: () => new Date("2026-10-01T00:00:00.000Z"),
        outputPath,
        role: "observer",
        secrets: { [kid]: secret },
        sessionExists: async () => true,
        sessionScope: "sess_ops",
        stores,
        subject: "part_ops",
        ttlSeconds: 3_600,
      });
    } catch (error) {
      failure = projectGrantOpsCliError(error);
    }
    const jti = failure.split(" ")[1];
    expect(failure).toMatch(/^grant_ops_output_rollback_failed grant_[A-Za-z0-9_-]+$/u);
    expect(failure).not.toContain(secret);
    const inventory = await inventorySessionGrants({
      acceptedKids: [kid],
      issuer,
      now: () => new Date("2026-10-01T00:10:00.000Z"),
      sessionScope: "sess_ops",
      stores,
      subject: null,
    });
    expect(inventory.grants).toContainEqual(
      expect.objectContaining({ jti, state: "active", restBearerEligibility: "eligible" }),
    );
    expect(JSON.stringify(inventory)).not.toContain(secret);
    await expectMissing(outputPath);
  });
});

describe("grant-ops inventory", () => {
  const inventoryNow = new Date("2026-10-01T00:30:00.000Z");

  function inventoryStores(records: readonly AuthGrantRecord[]) {
    const listForSessionInventory = vi.fn(
      async (input: { readonly sessionScope: string; readonly subject: string | null }) =>
        records.filter(
          (record) =>
            (record.sessionScope === input.sessionScope ||
              record.sessionScope === serviceWideAuthGrantScope) &&
            (input.subject === null || record.subject === input.subject),
        ),
    );
    const stores = fakeStores({ listForSessionInventory });
    return { listForSessionInventory, stores };
  }

  it("includes service-wide rows and exceeds the bounded 100-row list cap", async () => {
    const records = [
      grantRecord({ jti: "grant_service_wide", role: "admin", sessionScope: "*" }),
      ...Array.from({ length: 150 }, (_value, index) =>
        grantRecord({ jti: `grant_row_${index}`, sessionScope: "sess_ops" }),
      ),
    ];
    const { listForSessionInventory, stores } = inventoryStores(records);

    const result = await inventorySessionGrants({
      acceptedKids: [kid],
      issuer,
      now: () => inventoryNow,
      sessionScope: "sess_ops",
      stores,
      subject: null,
    });

    expect(listForSessionInventory).toHaveBeenCalledWith({
      sessionScope: "sess_ops",
      subject: null,
    });
    expect(result.grants).toHaveLength(151);
    expect(result.grants.map((grant) => grant.jti)).toContain("grant_service_wide");
    const serialized = JSON.stringify(result);
    expect(serialized).not.toContain("tgr2.");
    expect(serialized).not.toMatch(/"bearer"/u);
  });

  it("labels browser-source, issuer, kid, and eligible grants without claiming validity", async () => {
    const records = [
      grantRecord({ jti: "grant_browser", metadata: { requestId: null, source: "browser" } }),
      grantRecord({ jti: "grant_rotated", kid: "retired" }),
      grantRecord({ jti: "grant_old_issuer", issuer: "https://old.tether.test" }),
      grantRecord({ jti: "grant_current" }),
    ];
    const { stores } = inventoryStores(records);

    const result = await inventorySessionGrants({
      acceptedKids: [kid],
      issuer,
      now: () => inventoryNow,
      sessionScope: "sess_ops",
      stores,
      subject: null,
    });

    const byJti = new Map(result.grants.map((grant) => [grant.jti, grant]));
    expect(byJti.get("grant_browser")?.restBearerEligibility).toBe("browser_source");
    expect(byJti.get("grant_rotated")?.restBearerEligibility).toBe("unknown_kid");
    expect(byJti.get("grant_old_issuer")?.restBearerEligibility).toBe("unknown_issuer");
    expect(byJti.get("grant_current")?.restBearerEligibility).toBe("eligible");
    expect(result.note).toContain("no bearer token is read or verified");
  });

  it("classifies revoked, expired, and active durable state", async () => {
    const records = [
      grantRecord({ jti: "grant_revoked", revokedAt: new Date("2026-10-01T00:10:00.000Z") }),
      grantRecord({ jti: "grant_expired", expiresAt: new Date("2026-10-01T00:20:00.000Z") }),
      grantRecord({ jti: "grant_active" }),
    ];
    const { stores } = inventoryStores(records);

    const result = await inventorySessionGrants({
      acceptedKids: [kid],
      issuer,
      now: () => inventoryNow,
      sessionScope: "sess_ops",
      stores,
      subject: null,
    });

    const byJti = new Map(result.grants.map((grant) => [grant.jti, grant]));
    expect(byJti.get("grant_revoked")).toMatchObject({
      revokedAt: "2026-10-01T00:10:00.000Z",
      state: "revoked",
    });
    expect(byJti.get("grant_expired")).toMatchObject({ state: "expired" });
    expect(byJti.get("grant_active")).toMatchObject({ state: "active" });
    expect(byJti.get("grant_active")?.issuer).toBe(issuer);
  });

  it("forwards the subject filter and reports it back", async () => {
    const records = [
      grantRecord({ jti: "grant_one", subject: "part_one" }),
      grantRecord({ jti: "grant_two", subject: "part_two" }),
    ];
    const { listForSessionInventory, stores } = inventoryStores(records);

    const result = await inventorySessionGrants({
      acceptedKids: [kid],
      issuer,
      now: () => inventoryNow,
      sessionScope: "sess_ops",
      stores,
      subject: "part_two",
    });

    expect(listForSessionInventory).toHaveBeenCalledWith({
      sessionScope: "sess_ops",
      subject: "part_two",
    });
    expect(result.grants.map((grant) => grant.jti)).toEqual(["grant_two"]);
    expect(result.subjectFilter).toBe("part_two");
  });
});

describe("grant-ops revoke", () => {
  it("revokes by jti through the audited lifecycle without any bearer", async () => {
    const stores = fakeStores();
    stores.records.set("grant_revoke_me", grantRecord({ jti: "grant_revoke_me" }));

    const result = await revokeGrantOpsGrant({
      jti: "grant_revoke_me",
      reasonCode: "operator-request",
      stores,
    });

    expect(result).toMatchObject({
      command: "revoke",
      grant: { jti: "grant_revoke_me", revokedAt: expect.any(String) },
      jti: "grant_revoke_me",
      status: "revoked",
    });
    expect(stores.revokeGrantWithAudit).toHaveBeenCalledWith(
      expect.objectContaining({
        audit: expect.objectContaining({
          action: "grant.revoked",
          actorSubject: "grant-ops-cli",
          reasonCode: "operator-request",
        }),
        jti: "grant_revoke_me",
      }),
    );
  });

  it("reports already revoked and missing grants idempotently", async () => {
    const stores = fakeStores();
    stores.records.set("grant_twice", grantRecord({ jti: "grant_twice" }));

    expect(
      await revokeGrantOpsGrant({ jti: "grant_twice", reasonCode: "operator-request", stores }),
    ).toMatchObject({ status: "revoked" });
    expect(
      await revokeGrantOpsGrant({ jti: "grant_twice", reasonCode: "operator-request", stores }),
    ).toMatchObject({ status: "already_revoked" });
    expect(
      await revokeGrantOpsGrant({ jti: "grant_absent", reasonCode: "operator-request", stores }),
    ).toMatchObject({ grant: null, status: "not_found" });
  });
});

describe("grant-ops store inventory read", () => {
  const inventoryRow = (jti: string, sessionScope: string, subject: string) => ({
    audience: "tether-rest",
    expiresAt: new Date("2026-10-02T00:00:00.000Z"),
    issuedAt: new Date("2026-10-01T00:00:00.000Z"),
    issuer,
    jti,
    kid,
    metadata: { requestId: null, source: "admin" },
    revokedAt: null,
    role: "observer",
    sessionScope,
    subject,
  });

  /** Query shape produced by the grant inventory read. */
  type InventoryQueryConfig = {
    readonly text: string;
    readonly values: readonly unknown[];
  };
  type InventoryQuery = (
    config: InventoryQueryConfig,
  ) => Promise<{ readonly rows: readonly unknown[] }>;

  /** Builds a DatabasePool whose only live method is the parameterized query. */
  function fakeDatabasePool(query: InventoryQuery) {
    const database = {
      db: {} as DatabasePool["db"],
      end: vi.fn(async () => undefined),
      pool: { query } as unknown as DatabasePool["pool"],
    };
    return database as DatabasePool;
  }

  it("runs one parameterized uncapped query in deterministic order", async () => {
    const query = vi.fn(async (_config: InventoryQueryConfig) => ({
      rows: [
        inventoryRow("grant_b", "sess_ops", "part_ops"),
        inventoryRow("grant_a", "*", "part_admin"),
      ],
    }));
    const store = createAuthPersistenceStores(fakeDatabasePool(query)).grants;

    const grants = await store.listForSessionInventory({
      sessionScope: "sess_ops",
      subject: null,
    });

    expect(grants.map((grant) => grant.jti)).toEqual(["grant_b", "grant_a"]);
    const call = query.mock.calls[0]?.[0];
    expect(call?.values).toEqual([["sess_ops", "*"], null]);
    expect(call?.text).toContain("session_scope = ANY($1::text[])");
    expect(call?.text).toContain("$2::text IS NULL OR subject = $2::text");
    expect(call?.text).toContain("ORDER BY issued_at DESC, jti DESC");
    expect(call?.text).not.toMatch(/LIMIT/iu);
  });

  it("binds the exact subject filter when supplied", async () => {
    const query = vi.fn(async (_config: InventoryQueryConfig) => ({
      rows: [],
    }));
    const store = createAuthPersistenceStores(fakeDatabasePool(query)).grants;

    await store.listForSessionInventory({ sessionScope: "sess_ops", subject: "part_ops" });

    const call = query.mock.calls[0]?.[0];
    expect(call?.values).toEqual([["sess_ops", "*"], "part_ops"]);
  });

  it("rejects malformed scope and subject inputs before any query", async () => {
    const query = vi.fn(async (_config: InventoryQueryConfig) => ({
      rows: [],
    }));
    const store = createAuthPersistenceStores(fakeDatabasePool(query)).grants;

    await expect(
      store.listForSessionInventory({ sessionScope: "", subject: null }),
    ).rejects.toThrow("auth_grant_scope_invalid");
    await expect(
      store.listForSessionInventory({ sessionScope: "sess_ops", subject: "" }),
    ).rejects.toThrow("auth_grant_scope_invalid");
    await expect(
      store.listForSessionInventory({ sessionScope: "x".repeat(256), subject: null }),
    ).rejects.toThrow("auth_grant_scope_invalid");
    expect(query).not.toHaveBeenCalled();
  });

  it("collapses database failures into the bounded list error", async () => {
    const query = vi.fn(async (_config: InventoryQueryConfig) => {
      throw new Error("connection refused");
    });
    const store = createAuthPersistenceStores(fakeDatabasePool(query)).grants;

    await expect(
      store.listForSessionInventory({ sessionScope: "sess_ops", subject: null }),
    ).rejects.toThrow("auth_grant_list_failed");
  });
});

describe("grant-ops bounded error projection", () => {
  it("keeps bounded codes with a recovery jti and hides everything else", () => {
    expect(projectGrantOpsCliError(new Error("grant_ops_output_failed_revoked grant_abcd"))).toBe(
      "grant_ops_output_failed_revoked grant_abcd",
    );
    expect(projectGrantOpsCliError(new Error("grant_ops_output_rollback_failed grant_abcd"))).toBe(
      "grant_ops_output_rollback_failed grant_abcd",
    );
    expect(projectGrantOpsCliError(new Error("auth_grant_revoke_failed"))).toBe(
      "auth_grant_revoke_failed",
    );
    expect(projectGrantOpsCliError(new Error(`EIO writing ${"x".repeat(9_000)}`))).toBe(
      "grant_ops_failed",
    );
    expect(projectGrantOpsCliError(new Error("tgr2.leaked.signature"))).toBe("grant_ops_failed");
  });
});
