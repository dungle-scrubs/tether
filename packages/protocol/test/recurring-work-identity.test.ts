import { describe, expect, it } from "vitest";

import {
  candidateScheduleIdentitySchema,
  currentScheduledTaskIdentityVersion,
  recurringWorkScopeSchema,
  scheduledTaskIdentitySchema,
} from "../src/index.js";

describe("recurring work scope", () => {
  it("accepts an opaque scope key and rejects provider-specific fields", () => {
    expect(recurringWorkScopeSchema.parse({ scopeKey: "scope_01JEMAIL" })).toEqual({
      scopeKey: "scope_01JEMAIL",
    });
    expect(
      recurringWorkScopeSchema.safeParse({
        accountId: "acct_private",
        provider: "fastmail",
        scopeKey: "scope_01JEMAIL",
      }).success,
    ).toBe(false);
  });

  it("uses only an opaque scope key in scheduled task creation", () => {
    const identity = {
      scheduleAlgorithmVersion: 1,
      scheduleIntervalMs: 3_600_000,
      scheduleWindowStart: 1_699_999_200_000,
      scopeKey: "scope_01JEMAIL",
    };

    expect(scheduledTaskIdentitySchema.parse(identity)).toEqual(identity);
    expect(
      scheduledTaskIdentitySchema.safeParse({
        ...identity,
        mailboxAccountId: "acct_private",
        mailboxProvider: "fastmail",
      }).success,
    ).toBe(false);
  });

  it("uses only an opaque scope key in durable task records", () => {
    const schedule = {
      identityVersion: currentScheduledTaskIdentityVersion,
      scheduleWindow: {
        algorithmVersion: 1,
        endMs: 1_700_002_800_000,
        intervalMs: 3_600_000,
        startMs: 1_699_999_200_000,
      },
      scopeKey: "scope_01JEMAIL",
    };

    expect(candidateScheduleIdentitySchema.parse(schedule)).toEqual(schedule);
    expect(
      candidateScheduleIdentitySchema.safeParse({
        ...schedule,
        mailboxScope: { accountId: "acct_private", provider: "fastmail" },
      }).success,
    ).toBe(false);
  });

  it("versions durable scheduled identities independently of windowing", () => {
    expect(currentScheduledTaskIdentityVersion).toBe(2);
    expect(
      candidateScheduleIdentitySchema.parse({
        identityVersion: 1,
        scheduleWindow: {
          algorithmVersion: 1,
          endMs: 1_700_002_800_000,
          intervalMs: 3_600_000,
          startMs: 1_699_999_200_000,
        },
        scopeKey: "legacy_v1_opaque",
      }),
    ).toMatchObject({ identityVersion: 1, scopeKey: "legacy_v1_opaque" });
  });
});
