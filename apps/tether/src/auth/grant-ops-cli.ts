import { open, unlink, type FileHandle } from "node:fs/promises";
import { dirname } from "node:path";
import { pathToFileURL } from "node:url";

import { createPool, readSession, type DatabasePool } from "../db.js";
import { createAuthPersistenceStores } from "./db-grant-stores.js";
import {
  createAuthGrantLifecycle,
  maximumAuthGrantLifetimeSeconds,
  type CreatedAuthGrant,
  type PublicAuthGrant,
  toPublicAuthGrant,
} from "./grant-lifecycle.js";
import type { AuthPersistenceStores } from "./grant-stores.js";
import { serviceWideAuthGrantScope } from "./grant-stores.js";
import type { AuthSigningSecrets } from "./token.js";

/**
 * Host-local durable auth grant operations (`pnpm --filter tether grant-ops`).
 *
 * Commands:
 * - `issue` mints exactly one audited grant scoped to one existing durable
 *   session with role `observer` or `participant` (never `*` scope, never
 *   `admin`), bounded to a TTL of at most seven days, and writes the bearer
 *   only to an operator-supplied output file reserved with an exclusive
 *   0600 create before the durable grant exists.
 * - `inventory` lists every durable grant relevant to one session, including
 *   service-wide `*` rows plus revoked and expired grants, as metadata only.
 * - `revoke` idempotently revokes one grant by its public token id through the
 *   audited lifecycle; no bearer is needed.
 *
 * Safety contract:
 * - The bearer never appears in argv, stdout, stderr, logs, error messages, or
 *   audit metadata. It is written exactly once to the reserved file; the file
 *   and parent directory are fsynced before success; stdout carries grant
 *   metadata only.
 * - The schema must already be migrated (start the service or run
 *   `db:push`); this CLI never runs migrations itself.
 * - Crash gap: the audited durable grant commits before the bearer file is
 *   complete. If the process dies in between, the grant exists but its bearer
 *   is unrecoverable; run `inventory` to find the orphaned jti and `revoke` it.
 *   The bounded TTL limits how long any undelivered grant can stay valid.
 * - `completeBootstrapOneTimeSecret` from bootstrap-cli is deliberately not
 *   reused here: it does not revoke the committed grant when the one-time
 *   output write fails, which this CLI must do.
 */

const defaultSigningKid = "default";
const defaultTtlSeconds = 86_400;
const grantOpsActorSubject = "grant-ops-cli";
const maximumOutputPathLength = 4_096;
const grantJtiPattern = /^grant_[A-Za-z0-9_-]{1,120}$/u;

/** Roles this CLI is allowed to issue; admin issuance is refused outright. */
export type GrantOpsIssuableRole = "observer" | "participant";

/** Bounded audit reasons accepted for an explicit revocation. */
export type GrantOpsRevokeReasonCode = "key-rotation" | "operator-request" | "security-response";

/** Narrow environment accepted by the host-local grant operations command. */
export interface GrantOpsCliEnvironment {
  /** Must be exactly `true` before `issue` can mint a tgr2 bearer. */
  readonly AUTH_GRANT_BOOTSTRAP_COMPATIBILITY_CONFIRMED?: string | undefined;
  /** Durable grant issuer required by `issue`. */
  readonly AUTH_ISSUER?: string | undefined;
  /** JSON map of additional verification keys; values are never displayed. */
  readonly AUTH_ACCEPTED_SIGNING_SECRETS?: string | undefined;
  /** Optional signing key id; also the accepted kid for inventory labels. */
  readonly AUTH_SIGNING_KID?: string | undefined;
  /** Required active signing secret for `issue`. */
  readonly AUTH_SIGNING_SECRET?: string | undefined;
  /** Required target Postgres URL. */
  readonly DATABASE_URL?: string | undefined;
}

/** Parsed host-local grant operation. */
export type GrantOpsCliOptions =
  | { readonly command: "help" }
  | {
      readonly command: "issue";
      readonly compatibilityConfirmed: boolean;
      readonly databaseUrl: string;
      readonly issuer: string;
      readonly kid: string;
      readonly outputPath: string;
      readonly role: GrantOpsIssuableRole;
      readonly secret: string;
      readonly sessionScope: string;
      readonly subject: string;
      readonly ttlSeconds: number;
    }
  | {
      readonly acceptedKids: readonly string[];
      readonly command: "inventory";
      readonly databaseUrl: string;
      readonly issuer: string | null;
      readonly sessionScope: string;
      readonly subject: string | null;
    }
  | {
      readonly command: "revoke";
      readonly databaseUrl: string;
      readonly jti: string;
      readonly reasonCode: GrantOpsRevokeReasonCode;
    };

/** Parses the strict host-local contract without loading server-only config. */
export function parseGrantOpsCliOptions(
  args: readonly string[],
  env: GrantOpsCliEnvironment = process.env,
): GrantOpsCliOptions {
  const [command, ...flagArgs] = args;
  if (command === undefined || command === "help") {
    return { command: "help" };
  }
  if (command !== "issue" && command !== "inventory" && command !== "revoke") {
    throw new Error("grant_ops_command_invalid");
  }
  const databaseUrl = readRequiredEnvironment(env.DATABASE_URL, "database_url");
  const kid = env.AUTH_SIGNING_KID?.trim() || defaultSigningKid;
  if (kid.length === 0 || kid.length > 128) {
    throw new Error("grant_ops_kid_invalid");
  }
  if (command === "issue") {
    if (env.AUTH_GRANT_BOOTSTRAP_COMPATIBILITY_CONFIRMED !== "true") {
      throw new Error("grant_ops_compatibility_unconfirmed");
    }
    const values = parseFlagValues(flagArgs, ["out", "role", "session", "subject", "ttl"]);
    if (values.size !== 4 && values.size !== 5) {
      throw new Error("grant_ops_arguments_invalid");
    }
    const role = readRequiredFlag(values, "role");
    if (role !== "observer" && role !== "participant") {
      throw new Error("grant_ops_role_forbidden");
    }
    const sessionScope = parseSessionScope(readRequiredFlag(values, "session"));
    const subject = parseBoundedText(readRequiredFlag(values, "subject"), 255, () => {
      throw new Error("grant_ops_subject_invalid");
    });
    const outputPath = parseBoundedText(
      readRequiredFlag(values, "out"),
      maximumOutputPathLength,
      () => {
        throw new Error("grant_ops_output_invalid");
      },
    );
    const ttlSeconds = parseTtlSeconds(values.get("ttl") ?? String(defaultTtlSeconds));
    const issuer = readRequiredEnvironment(env.AUTH_ISSUER, "auth_issuer");
    if (issuer.length > 512) {
      throw new Error("grant_ops_auth_issuer_invalid");
    }
    const secret = readRequiredEnvironment(env.AUTH_SIGNING_SECRET, "auth_signing_secret");
    return {
      command,
      compatibilityConfirmed: true,
      databaseUrl,
      issuer,
      kid,
      outputPath,
      role,
      secret,
      sessionScope,
      subject,
      ttlSeconds,
    };
  }
  if (command === "inventory") {
    const values = parseFlagValues(flagArgs, ["session", "subject"]);
    if (values.size !== 1 && values.size !== 2) {
      throw new Error("grant_ops_arguments_invalid");
    }
    const sessionScope = parseSessionScope(readRequiredFlag(values, "session"));
    const subjectValue = values.get("subject");
    const subject =
      subjectValue === undefined
        ? null
        : parseBoundedText(subjectValue, 255, () => {
            throw new Error("grant_ops_subject_invalid");
          });
    return {
      acceptedKids: acceptedSigningKids(env, kid),
      command,
      databaseUrl,
      issuer: env.AUTH_ISSUER?.trim() || null,
      sessionScope,
      subject,
    };
  }
  const values = parseFlagValues(flagArgs, ["jti", "reason"]);
  if (values.size !== 1 && values.size !== 2) {
    throw new Error("grant_ops_arguments_invalid");
  }
  const jti = readRequiredFlag(values, "jti");
  if (!grantJtiPattern.test(jti)) {
    throw new Error("grant_ops_jti_invalid");
  }
  const reasonCode = values.get("reason") ?? "operator-request";
  if (
    reasonCode !== "key-rotation" &&
    reasonCode !== "operator-request" &&
    reasonCode !== "security-response"
  ) {
    throw new Error("grant_ops_reason_invalid");
  }
  return { command, databaseUrl, jti, reasonCode };
}

/** Dependencies for the testable issue core; no pool is opened here. */
export interface GrantOpsIssueInput {
  /** Signing key id for the new grant. */
  readonly activeKid: string;
  /** Injectable bearer delivery; defaults to write, fsync, and close. */
  readonly deliver?: (handle: FileHandle, bearer: string) => Promise<void>;
  /** Injectable sync seam for a real file-sync failure regression. */
  readonly syncFile?: (handle: FileHandle) => Promise<void>;
  /** Injectable directory-sync seam for durable output entry verification. */
  readonly syncDirectory?: (path: string) => Promise<void>;
  /** Durable issuer embedded in the new grant. */
  readonly issuer: string;
  /** Deterministic lifecycle clock. */
  readonly now?: () => Date;
  /** Explicit operator-chosen bearer output path; must not already exist. */
  readonly outputPath: string;
  /** Only `observer` or `participant` is accepted. */
  readonly role: GrantOpsIssuableRole;
  /** Accepted signing secrets keyed by key id. */
  readonly secrets: AuthSigningSecrets;
  /** Confirms the target session already exists before any write. */
  readonly sessionExists: (sessionId: string) => Promise<boolean>;
  /** Existing durable session id; never `*`. */
  readonly sessionScope: string;
  /** Transaction-owning persistence interface. */
  readonly stores: AuthPersistenceStores;
  /** Durable identity receiving the grant. */
  readonly subject: string;
  /** Bounded lifetime in seconds, at most seven days. */
  readonly ttlSeconds: number;
}

/** Metadata-only issue result; the bearer is never part of it. */
export interface GrantOpsIssueResult {
  readonly command: "issue";
  readonly grant: PublicAuthGrant;
  readonly outputPath: string;
}

/**
 * Issues one audited session-scoped grant and delivers its bearer only to the
 * reserved output file. If delivery fails after the durable commit, the grant
 * is immediately revoked by jti through the audited lifecycle and a bounded
 * error carrying only the jti is thrown.
 */
export async function issueGrantOpsGrant(input: GrantOpsIssueInput): Promise<GrantOpsIssueResult> {
  if (
    (input.role !== "observer" && input.role !== "participant") ||
    input.sessionScope === serviceWideAuthGrantScope ||
    input.sessionScope.trim() === ""
  ) {
    throw new Error("grant_ops_issue_scope_invalid");
  }
  if (!(await input.sessionExists(input.sessionScope))) {
    throw new Error("grant_ops_session_not_found");
  }
  let handle: FileHandle;
  try {
    // Reserve before create: exclusive create refuses an existing path and the
    // 0600 mode keeps the not-yet-written bearer file operator-only.
    handle = await open(input.outputPath, "wx", 0o600);
  } catch {
    throw new Error("grant_ops_output_unavailable");
  }
  try {
    await handle.chmod(0o600);
  } catch {
    // The exclusive create already restricted the file to the owner; a umask
    // fight here must not block issuance.
  }
  let lifecycle: ReturnType<typeof createAuthGrantLifecycle>;
  let created: CreatedAuthGrant;
  try {
    lifecycle = createAuthGrantLifecycle({
      activeKid: input.activeKid,
      issuer: input.issuer,
      issuanceEnabled: true,
      ...(input.now === undefined ? {} : { now: input.now }),
      secrets: input.secrets,
      stores: input.stores,
    });
    created = await lifecycle.create({
      actorSubject: grantOpsActorSubject,
      reasonCode: "operator-request",
      role: input.role,
      sessionScope: input.sessionScope,
      source: "admin",
      subject: input.subject,
      ttlSeconds: input.ttlSeconds,
    });
  } catch (error) {
    // Nothing committed: remove this run's empty reserved file and surface
    // the bounded persistence or token error unchanged.
    await closeQuietly(handle);
    await removeReservedOutput(input.outputPath);
    throw error;
  }
  const deliver =
    input.deliver ??
    ((output: FileHandle, bearer: string) =>
      deliverBearerToFile(output, bearer, input.syncFile ?? ((file) => file.sync())));
  try {
    await deliver(handle, created.bearer);
    await handle.close();
    await (input.syncDirectory ?? syncParentDirectory)(input.outputPath);
  } catch {
    await closeQuietly(handle);
    // The audited grant is committed but its bearer was not delivered.
    // Attempt an immediate audited revocation by jti so no committed grant
    // outlives an undelivered bearer. Crash gap: if this process dies before
    // either the delivery or the revocation lands, the grant stays durable
    // and `inventory` plus `revoke --jti` is the documented recovery.
    let revoked = false;
    try {
      const result = await lifecycle.revoke(
        created.grant.jti,
        grantOpsActorSubject,
        "security-response",
      );
      revoked = result.status === "revoked" || result.status === "already_revoked";
    } catch {
      revoked = false;
    }
    await removeReservedOutput(input.outputPath);
    throw new Error(
      revoked
        ? `grant_ops_output_failed_revoked ${created.grant.jti}`
        : `grant_ops_output_rollback_failed ${created.grant.jti}`,
    );
  }
  return { command: "issue", grant: created.grant, outputPath: input.outputPath };
}

/** Flushes the parent directory entry after the file contents are durable. */
async function syncParentDirectory(outputPath: string): Promise<void> {
  const directory = await open(dirname(outputPath), "r");
  try {
    await directory.sync();
  } finally {
    await directory.close();
  }
}

/** Writes the bearer plus one newline and fsyncs; the caller closes the file. */
async function deliverBearerToFile(
  handle: FileHandle,
  bearer: string,
  syncFile: (handle: FileHandle) => Promise<void>,
): Promise<void> {
  await handle.writeFile(`${bearer}\n`, "utf8");
  await syncFile(handle);
}

/** Closes a reserved handle when creation or injectable delivery fails. */
async function closeQuietly(handle: FileHandle): Promise<void> {
  try {
    await handle.close();
  } catch {
    // Preserve the bounded failure selected by the command.
  }
}

/** Best-effort removal of this run's empty or partial exclusive output file. */
async function removeReservedOutput(outputPath: string): Promise<void> {
  try {
    await unlink(outputPath);
  } catch {
    // The path was created exclusively by this run, so at worst a partial
    // 0600 file remains; cleanup failure must never mask the primary error.
  }
}

/** Dependencies for the testable inventory core; no pool is opened here. */
export interface GrantOpsInventoryInput {
  /** Key ids whose bearers current replicas can still verify. */
  readonly acceptedKids: readonly string[];
  /** Current REST issuer, if configured on this host. */
  readonly issuer?: string | null;
  /** Deterministic classification clock. */
  readonly now?: () => Date;
  /** Existing durable session id; never `*`. */
  readonly sessionScope: string;
  /** Narrow grant store carrying the uncapped inventory read. */
  readonly stores: Pick<AuthPersistenceStores, "grants">;
  /** Exact subject filter, or null to list every subject. */
  readonly subject: string | null;
}

/** One metadata-only inventory row with durable state and eligibility label. */
export interface GrantOpsInventoryRow extends PublicAuthGrant {
  /**
   * Metadata classification for generic REST bearer use. `browser_source`
   * marks browser-originated grants, which generic REST and bearer WebSocket
   * boundaries reject. `unknown_kid` and `unknown_issuer` mark records this
   * host cannot accept. This label is never a claim that any
   * bearer token is valid, expired, or revoked.
   */
  readonly restBearerEligibility: "browser_source" | "eligible" | "unknown_issuer" | "unknown_kid";
  /** Durable-row state at inventory time, independent of any bearer. */
  readonly state: "active" | "expired" | "revoked";
}

/** Metadata-only inventory result; no bearer is ever selected or returned. */
export interface GrantOpsInventoryResult {
  readonly acceptedKids: readonly string[];
  readonly command: "inventory";
  readonly grants: readonly GrantOpsInventoryRow[];
  readonly note: "Eligibility labels classify durable metadata only; no bearer token is read or verified.";
  readonly sessionScope: string;
  readonly subjectFilter: string | null;
}

/** Lists every grant relevant to one session, including `*` rows, metadata only. */
export async function inventorySessionGrants(
  input: GrantOpsInventoryInput,
): Promise<GrantOpsInventoryResult> {
  const at = (input.now ?? (() => new Date()))();
  const records = await input.stores.grants.listForSessionInventory({
    sessionScope: input.sessionScope,
    subject: input.subject,
  });
  return {
    acceptedKids: [...input.acceptedKids],
    command: "inventory",
    grants: records.map((record) =>
      projectInventoryRow(record, input.acceptedKids, input.issuer ?? null, at),
    ),
    note: "Eligibility labels classify durable metadata only; no bearer token is read or verified.",
    sessionScope: input.sessionScope,
    subjectFilter: input.subject,
  };
}

/** Projects one durable record into its labeled metadata-only inventory row. */
function projectInventoryRow(
  record: Parameters<typeof toPublicAuthGrant>[0],
  acceptedKids: readonly string[],
  issuer: string | null,
  at: Date,
): GrantOpsInventoryRow {
  const restBearerEligibility =
    record.metadata.source === "browser"
      ? "browser_source"
      : issuer === null || record.issuer !== issuer
        ? "unknown_issuer"
        : acceptedKids.includes(record.kid)
          ? "eligible"
          : "unknown_kid";
  return {
    ...toPublicAuthGrant(record),
    restBearerEligibility,
    state:
      record.revokedAt !== null
        ? "revoked"
        : record.expiresAt.getTime() <= at.getTime()
          ? "expired"
          : "active",
  };
}

/** Dependencies for the testable revoke core; no pool is opened here. */
export interface GrantOpsRevokeInput {
  /** Public token id of the grant to revoke. */
  readonly jti: string;
  /** Deterministic lifecycle clock. */
  readonly now?: () => Date;
  /** Bounded audit reason recorded with the revocation. */
  readonly reasonCode: GrantOpsRevokeReasonCode;
  /** Transaction-owning persistence interface. */
  readonly stores: AuthPersistenceStores;
}

/** Metadata-only revoke result. */
export interface GrantOpsRevokeResult {
  readonly command: "revoke";
  readonly grant: PublicAuthGrant | null;
  readonly jti: string;
  readonly status: "already_revoked" | "not_found" | "revoked";
}

/** Revokes one grant by jti through the audited lifecycle; no bearer needed. */
export async function revokeGrantOpsGrant(
  input: GrantOpsRevokeInput,
): Promise<GrantOpsRevokeResult> {
  // A lifecycle without issuer or secrets cannot issue, and revoke never
  // reads those fields; only the transaction-owning store and clock are used.
  const lifecycle = createAuthGrantLifecycle({
    activeKid: "",
    issuer: null,
    ...(input.now === undefined ? {} : { now: input.now }),
    secrets: {},
    stores: input.stores,
  });
  const result = await lifecycle.revoke(input.jti, grantOpsActorSubject, input.reasonCode);
  return { command: "revoke", grant: result.grant, jti: input.jti, status: result.status };
}

/** Executes one parsed operation against one short-lived pool. */
export async function executeGrantOpsCli(options: GrantOpsCliOptions): Promise<string> {
  if (options.command === "help") {
    return `${grantOpsUsage}\n`;
  }
  const database = createPool(options.databaseUrl, { max: 1 });
  const stores = createAuthPersistenceStores(database);
  try {
    if (options.command === "issue") {
      const result = await issueGrantOpsGrant({
        activeKid: options.kid,
        issuer: options.issuer,
        outputPath: options.outputPath,
        role: options.role,
        secrets: { [options.kid]: options.secret },
        sessionExists: (sessionId) => confirmSessionExists(database, sessionId),
        sessionScope: options.sessionScope,
        stores,
        subject: options.subject,
        ttlSeconds: options.ttlSeconds,
      });
      return `${JSON.stringify(result)}\n`;
    }
    if (options.command === "inventory") {
      const result = await inventorySessionGrants({
        acceptedKids: options.acceptedKids,
        issuer: options.issuer,
        sessionScope: options.sessionScope,
        stores,
        subject: options.subject,
      });
      return `${JSON.stringify(result)}\n`;
    }
    const result = await revokeGrantOpsGrant({
      jti: options.jti,
      reasonCode: options.reasonCode,
      stores,
    });
    return `${JSON.stringify(result)}\n`;
  } finally {
    await closePoolQuietly(database);
  }
}

/** Runs the CLI with injectable streams while printing metadata only. */
export async function runGrantOpsCli(
  args: readonly string[] = process.argv.slice(2),
  env: GrantOpsCliEnvironment = process.env,
  writeOutput: (value: string) => void = (value) => process.stdout.write(value),
): Promise<void> {
  writeOutput(await executeGrantOpsCli(parseGrantOpsCliOptions(args, env)));
}

/** Confirms the durable session row exists via the narrow existing read. */
async function confirmSessionExists(database: DatabasePool, sessionId: string): Promise<boolean> {
  try {
    await readSession(database, sessionId);
    return true;
  } catch (error) {
    // readSession reports a missing row with this exact stable message; any
    // other failure is a database failure and must not read as "absent".
    if (error instanceof Error && error.message === "Missing session row") {
      return false;
    }
    throw error;
  }
}

/** Ends the single-shot pool without masking the already-decided result. */
async function closePoolQuietly(database: DatabasePool): Promise<void> {
  try {
    await database.end();
  } catch {
    // Host-local one-shot process; the command result is already determined.
  }
}

/** Parses strict `--name value` pairs limited to the allowed flag names. */
function parseFlagValues(
  args: readonly string[],
  allowed: readonly string[],
): ReadonlyMap<string, string> {
  const values = new Map<string, string>();
  for (let index = 0; index < args.length; index += 2) {
    const flag = args[index];
    const value = args[index + 1];
    if (
      typeof flag !== "string" ||
      !flag.startsWith("--") ||
      !allowed.includes(flag.slice(2)) ||
      value === undefined ||
      value.startsWith("--") ||
      values.has(flag.slice(2))
    ) {
      throw new Error("grant_ops_arguments_invalid");
    }
    values.set(flag.slice(2), value);
  }
  return values;
}

/** Reads one required flag value, rejecting blank input. */
function readRequiredFlag(values: ReadonlyMap<string, string>, name: string): string {
  const value = values.get(name)?.trim();
  if (!value) {
    throw new Error(`grant_ops_${name}_required`);
  }
  return value;
}

/** Reads one required nonempty environment value. */
function readRequiredEnvironment(value: string | undefined, name: string): string {
  const trimmed = value?.trim();
  if (!trimmed) {
    throw new Error(`grant_ops_${name}_required`);
  }
  return trimmed;
}

/** Reads only verification key names; secret values never enter command output. */
function acceptedSigningKids(env: GrantOpsCliEnvironment, activeKid: string): string[] {
  let parsed: unknown;
  try {
    parsed = JSON.parse(env.AUTH_ACCEPTED_SIGNING_SECRETS ?? "{}");
  } catch {
    throw new Error("grant_ops_accepted_keys_invalid");
  }
  if (parsed === null || typeof parsed !== "object" || Array.isArray(parsed)) {
    throw new Error("grant_ops_accepted_keys_invalid");
  }
  const keys: string[] = [];
  for (const [kid, secret] of Object.entries(parsed)) {
    if (kid.length === 0 || typeof secret !== "string" || secret.trim() === "") {
      throw new Error("grant_ops_accepted_keys_invalid");
    }
    keys.push(kid);
  }
  if (env.AUTH_SIGNING_SECRET?.trim()) {
    keys.push(activeKid);
  }
  return [...new Set(keys)].sort();
}

/** Accepts one session scope that is neither blank nor service-wide `*`. */
function parseSessionScope(value: string): string {
  const sessionScope = parseBoundedText(value, 255, () => {
    throw new Error("grant_ops_session_scope_invalid");
  });
  if (sessionScope === serviceWideAuthGrantScope) {
    throw new Error("grant_ops_session_scope_invalid");
  }
  return sessionScope;
}

/** Accepts one trimmed bounded text value or throws the supplied error. */
function parseBoundedText(value: string, maximumLength: number, invalid: () => Error): string {
  const trimmed = value.trim();
  if (trimmed.length === 0 || trimmed.length > maximumLength) {
    throw invalid();
  }
  return trimmed;
}

/** Parses TTL values as seconds, or with h/d suffixes, bounded to seven days. */
function parseTtlSeconds(value: string): number {
  const match = value.match(/^(\d+)([dh])?$/u);
  if (!match?.[1]) {
    throw new Error("grant_ops_ttl_invalid");
  }
  const multiplier = match[2] === "d" ? 86_400 : match[2] === "h" ? 3_600 : 1;
  const ttlSeconds = Number.parseInt(match[1], 10) * multiplier;
  if (
    !Number.isSafeInteger(ttlSeconds) ||
    ttlSeconds <= 0 ||
    ttlSeconds > maximumAuthGrantLifetimeSeconds
  ) {
    throw new Error("grant_ops_ttl_invalid");
  }
  return ttlSeconds;
}

/** Projects any failure onto bounded output that can never carry a bearer. */
export function projectGrantOpsCliError(error: unknown): string {
  if (error instanceof Error) {
    const bounded = error.message.match(
      /^(grant_ops_[a-z0-9_]+|auth_[a-z0-9_]+)(?: (grant_[A-Za-z0-9_-]{1,120}))?$/u,
    );
    // The regex only admits bounded codes and an optional public jti, so the
    // validated message itself is the safe output.
    if (bounded) {
      return error.message;
    }
  }
  return "grant_ops_failed";
}

const grantOpsUsage = `tether grant-ops - host-local durable auth grant operations

Usage:
  grant-ops issue --session <id> --subject <subject> --role observer|participant --out <path> [--ttl <seconds|Nh|Nd>]
  grant-ops inventory --session <id> [--subject <subject>]
  grant-ops revoke --jti <grant id> [--reason key-rotation|operator-request|security-response]
  grant-ops help

issue
  Creates one audited session-scoped observer or participant grant (never "*"
  scope, never admin) for an existing session, with a TTL of at most 7 days.
  The bearer is written only to --out, which must not already exist: the file
  is reserved with an exclusive mode-0600 create before the grant is created,
  then written, fsynced, and closed. The parent directory is also fsynced
  before success. Stdout carries grant metadata only; the
  bearer never reaches argv, stdout, stderr, logs, or audit metadata.
  Requires DATABASE_URL, AUTH_ISSUER, AUTH_SIGNING_SECRET, and
  AUTH_GRANT_BOOTSTRAP_COMPATIBILITY_CONFIRMED=true (AUTH_SIGNING_KID
  optional, default "default"). The database schema must already be migrated;
  this CLI never runs migrations itself.

  Crash gap: the audited durable grant commits before the bearer file is
  complete. If the process dies in between, the grant exists but its bearer
  is unrecoverable. Run "inventory" to find the jti and "revoke" it; the
  bounded TTL limits exposure. If the output write fails after the commit,
  the CLI immediately revokes the grant by jti and reports
  grant_ops_output_failed_revoked (or grant_ops_output_rollback_failed when
  the revocation itself fails) with the jti.

inventory
  Lists every durable grant relevant to one session, including service-wide
  "*" grants and revoked or expired rows, with issuer, durable state, and a
  REST bearer eligibility label (eligible, unknown_issuer, unknown_kid,
  browser_source). It reads AUTH_ISSUER, AUTH_SIGNING_KID,
  AUTH_SIGNING_SECRET presence, and AUTH_ACCEPTED_SIGNING_SECRETS key names
  to classify this host's metadata eligibility.
  Labels classify durable metadata only; no bearer token is read, returned,
  or verified. Requires DATABASE_URL.

revoke
  Idempotently revokes one grant by its public token id through the audited
  lifecycle. No bearer is needed. Requires DATABASE_URL.`;

async function main(): Promise<void> {
  try {
    await runGrantOpsCli();
  } catch (error) {
    process.stderr.write(`${projectGrantOpsCliError(error)}\n`);
    process.exitCode = 1;
  }
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  void main();
}
