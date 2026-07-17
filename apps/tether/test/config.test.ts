import { Effect } from "effect";
import { describe, expect, it } from "vitest";

import {
  readConfig,
  readConfigEffectFromEnv,
  ServerConfigService,
  serverConfigLayerFromEnv,
} from "../src/config.js";
import { defaultResourceLimits } from "../src/resource-limits.js";

const testAuthIssuer = "https://auth.example.test";

describe("server config", () => {
  it("fails closed when runtime topology is missing", () => {
    expect(() =>
      readConfig({
        AUTH_ISSUER: testAuthIssuer,
        AUTH_SIGNING_SECRET: "test-secret",
        DATABASE_URL: "postgres://example.test/tether",
      }),
    ).toThrow("RUNTIME_TOPOLOGY must be single or multi");
  });

  it("rejects unsupported runtime topology without echoing its value", () => {
    expect(() =>
      readConfig({
        AUTH_ISSUER: testAuthIssuer,
        AUTH_SIGNING_SECRET: "test-secret",
        DATABASE_URL: "postgres://example.test/tether",
        RUNTIME_TOPOLOGY: "secret-unsupported-topology",
      }),
    ).toThrowError(new Error("RUNTIME_TOPOLOGY must be single or multi"));
  });

  it("fails closed through Effect when runtime topology is missing", async () => {
    await expect(
      Effect.runPromise(
        readConfigEffectFromEnv({
          AUTH_ISSUER: testAuthIssuer,
          AUTH_SIGNING_SECRET: "test-secret",
          DATABASE_URL: "postgres://example.test/tether",
        }),
      ),
    ).rejects.toThrow("RUNTIME_TOPOLOGY must be single or multi");
  });

  it("rejects unsupported runtime topology through Effect", async () => {
    await expect(
      Effect.runPromise(
        readConfigEffectFromEnv({
          AUTH_ISSUER: testAuthIssuer,
          AUTH_SIGNING_SECRET: "test-secret",
          DATABASE_URL: "postgres://example.test/tether",
          RUNTIME_TOPOLOGY: "cluster",
        }),
      ),
    ).rejects.toThrow("RUNTIME_TOPOLOGY must be single or multi");
  });

  it("reads current scheduler environment names", () => {
    expect(
      readConfig({
        AUTH_ISSUER: testAuthIssuer,
        AUTH_SIGNING_SECRET: "test-secret",
        DATABASE_POOL_MAX: "20",
        DATABASE_URL: "postgres://example.test/tether",
        EVENT_FANOUT_CATCH_UP_MS: "500",
        EVENT_FANOUT_CATCH_UP_STALE_MS: "1500",
        EVENT_FANOUT_BATCH_LIMIT: "11",
        EVENT_LIST_DEFAULT_LIMIT: "12",
        EVENT_LIST_MAX_LIMIT: "13",
        HTTP_MAX_BODY_BYTES: "14",
        PORT: "4100",
        RUNTIME_TOPOLOGY: "single",
        TASK_CLAIM_SWEEP_BATCH_SIZE: "7",
        TASK_CLAIM_SWEEP_MS: "250",
        WS_BACKPRESSURE_BUFFERED_BYTES: "15",
        WS_MAX_PAYLOAD_BYTES: "16",
        WS_MESSAGE_RATE_LIMIT: "17",
        WS_MESSAGE_RATE_WINDOW_MS: "18",
        WS_REPLAY_MAX_EVENTS: "19",
      }),
    ).toEqual({
      authAcceptedSigningSecrets: {},
      authIssuer: testAuthIssuer,
      authMode: "required",
      authSigningKid: "default",
      authSigningSecret: "test-secret",
      controlEpochEnforcement: true,
      databasePoolMax: 20,
      databaseUrl: "postgres://example.test/tether",
      eventFanoutCatchUpPollMs: 500,
      eventFanoutCatchUpStaleMs: 1500,
      port: 4100,
      resourceLimits: {
        eventFanoutBatchLimit: 11,
        eventListDefaultLimit: 12,
        eventListMaxLimit: 13,
        httpMaxBodyBytes: 14,
        wsBackpressureBufferedBytes: 15,
        wsMaxPayloadBytes: 16,
        wsMessageRateLimit: 17,
        wsMessageRateWindowMs: 18,
        wsReplayMaxEvents: 19,
      },
      runtimeTopology: "single",
      taskClaimSweepBatchSize: 7,
      taskClaimSweepMs: 250,
    });
  });

  it("falls back to scheduler defaults for invalid values", () => {
    const config = readConfig({
      AUTH_ISSUER: testAuthIssuer,
      AUTH_SIGNING_SECRET: "test-secret",
      DATABASE_URL: "postgres://example.test/tether",
      EVENT_FANOUT_CATCH_UP_MS: "-1",
      EVENT_FANOUT_CATCH_UP_STALE_MS: "0",
      EVENT_FANOUT_BATCH_LIMIT: "0",
      EVENT_LIST_DEFAULT_LIMIT: "-1",
      EVENT_LIST_MAX_LIMIT: "not-a-number",
      HTTP_MAX_BODY_BYTES: "0",
      RUNTIME_TOPOLOGY: "single",
      TASK_CLAIM_SWEEP_BATCH_SIZE: "0",
      TASK_CLAIM_SWEEP_MS: "-1",
      WS_BACKPRESSURE_BUFFERED_BYTES: "0",
      WS_MAX_PAYLOAD_BYTES: "-1",
      WS_MESSAGE_RATE_LIMIT: "0",
      WS_MESSAGE_RATE_WINDOW_MS: "-1",
      WS_REPLAY_MAX_EVENTS: "bad",
    });

    expect(config.eventFanoutCatchUpPollMs).toBe(1_000);
    expect(config.eventFanoutCatchUpStaleMs).toBe(30_000);
    expect(config.resourceLimits).toEqual(defaultResourceLimits);
    expect(config.taskClaimSweepBatchSize).toBe(50);
    expect(config.taskClaimSweepMs).toBe(1_000);
  });

  it("provides config through an Effect service layer", async () => {
    const config = await Effect.runPromise(
      ServerConfigService.pipe(
        Effect.provide(
          serverConfigLayerFromEnv({
            AUTH_ISSUER: testAuthIssuer,
            AUTH_SIGNING_SECRET: "test-secret",
            DATABASE_URL: "postgres://example.test/effect",
            PORT: "4101",
            RUNTIME_TOPOLOGY: "single",
          }),
        ),
      ),
    );

    expect(config).toMatchObject({
      databaseUrl: "postgres://example.test/effect",
      port: 4101,
      resourceLimits: defaultResourceLimits,
    });
  });

  it("defaults auth mode to required and fails without a signing secret", () => {
    expect(() => readConfig({ AUTH_ISSUER: testAuthIssuer, RUNTIME_TOPOLOGY: "single" })).toThrow(
      "AUTH_SIGNING_SECRET is required",
    );
  });

  it("requires an explicit auth issuer whenever authentication is required", async () => {
    expect(() =>
      readConfig({
        AUTH_SIGNING_SECRET: "test-secret",
        DATABASE_URL: "postgres://example.test/tether",
        RUNTIME_TOPOLOGY: "single",
      }),
    ).toThrow("AUTH_ISSUER is required when AUTH_MODE=required");

    await expect(
      Effect.runPromise(
        readConfigEffectFromEnv({
          AUTH_SIGNING_SECRET: "test-secret",
          DATABASE_URL: "postgres://example.test/tether",
          RUNTIME_TOPOLOGY: "single",
        }),
      ),
    ).rejects.toThrow("AUTH_ISSUER is required when AUTH_MODE=required");
  });

  it("rejects an issuer longer than the grant claim limit without echoing it", () => {
    const oversizedIssuer = `https://${"secret-marker".repeat(50)}.example.test`;
    expect(oversizedIssuer.length).toBeGreaterThan(512);

    expect(() =>
      readConfig({
        AUTH_ISSUER: oversizedIssuer,
        AUTH_SIGNING_SECRET: "test-secret",
        DATABASE_URL: "postgres://example.test/tether",
        RUNTIME_TOPOLOGY: "single",
      }),
    ).toThrow("AUTH_ISSUER exceeds maximum length 512");
    try {
      readConfig({
        AUTH_ISSUER: oversizedIssuer,
        AUTH_SIGNING_SECRET: "test-secret",
        DATABASE_URL: "postgres://example.test/tether",
        RUNTIME_TOPOLOGY: "single",
      });
    } catch (error) {
      expect(String(error)).not.toContain(oversizedIssuer);
    }
  });

  it("fails closed when DATABASE_URL is missing", () => {
    expect(() =>
      readConfig({
        AUTH_ISSUER: testAuthIssuer,
        AUTH_SIGNING_SECRET: "test-secret",
        RUNTIME_TOPOLOGY: "single",
      }),
    ).toThrow("DATABASE_URL is required");
  });

  it("fails closed through Effect config when DATABASE_URL is missing", async () => {
    await expect(
      Effect.runPromise(
        ServerConfigService.pipe(
          Effect.provide(
            serverConfigLayerFromEnv({
              AUTH_ISSUER: testAuthIssuer,
              AUTH_SIGNING_SECRET: "test-secret",
              RUNTIME_TOPOLOGY: "single",
            }),
          ),
        ),
      ),
    ).rejects.toThrow("DATABASE_URL is required");
  });

  it("allows auth disabled without a signing secret", async () => {
    expect(
      readConfig({
        AUTH_MODE: "disabled",
        DATABASE_URL: "postgres://example.test/tether",
        RUNTIME_TOPOLOGY: "single",
      }),
    ).toMatchObject({
      authMode: "disabled",
      authSigningSecret: null,
    });

    await expect(
      Effect.runPromise(
        ServerConfigService.pipe(
          Effect.provide(
            serverConfigLayerFromEnv({
              AUTH_MODE: "disabled",
              DATABASE_URL: "postgres://example.test/tether",
              RUNTIME_TOPOLOGY: "single",
            }),
          ),
        ),
      ),
    ).resolves.toMatchObject({
      authMode: "disabled",
      authSigningSecret: null,
    });
  });

  it("parses auth key id and accepted signing-secret rotation map", () => {
    expect(
      readConfig({
        AUTH_ISSUER: testAuthIssuer,
        AUTH_ACCEPTED_SIGNING_SECRETS: JSON.stringify({
          previous: "old-secret",
        }),
        AUTH_SIGNING_KID: "current",
        AUTH_SIGNING_SECRET: "new-secret",
        DATABASE_URL: "postgres://example.test/tether",
        RUNTIME_TOPOLOGY: "single",
      }),
    ).toMatchObject({
      authAcceptedSigningSecrets: { previous: "old-secret" },
      authIssuer: testAuthIssuer,
      authMode: "required",
      authSigningKid: "current",
      authSigningSecret: "new-secret",
    });
  });

  it("defaults control epoch enforcement on and permits only an explicit false override", () => {
    expect(
      readConfig({
        AUTH_ISSUER: testAuthIssuer,
        AUTH_SIGNING_SECRET: "test-secret",
        DATABASE_URL: "postgres://example.test/tether",
        RUNTIME_TOPOLOGY: "single",
      }).controlEpochEnforcement,
    ).toBe(true);
    expect(
      readConfig({
        AUTH_ISSUER: testAuthIssuer,
        AUTH_SIGNING_SECRET: "test-secret",
        CONTROL_EPOCH_ENFORCEMENT: "true",
        DATABASE_URL: "postgres://example.test/tether",
        RUNTIME_TOPOLOGY: "single",
      }).controlEpochEnforcement,
    ).toBe(true);
    expect(
      readConfig({
        AUTH_ISSUER: testAuthIssuer,
        AUTH_SIGNING_SECRET: "test-secret",
        CONTROL_EPOCH_ENFORCEMENT: "false",
        DATABASE_URL: "postgres://example.test/tether",
        RUNTIME_TOPOLOGY: "single",
      }).controlEpochEnforcement,
    ).toBe(false);
    expect(() =>
      readConfig({
        AUTH_ISSUER: testAuthIssuer,
        AUTH_SIGNING_SECRET: "test-secret",
        CONTROL_EPOCH_ENFORCEMENT: "nonsense",
        DATABASE_URL: "postgres://example.test/tether",
        RUNTIME_TOPOLOGY: "single",
      }),
    ).toThrow("CONTROL_EPOCH_ENFORCEMENT must be a documented boolean");
  });

  it("rejects invalid explicit control enforcement through Effect configuration", async () => {
    await expect(
      Effect.runPromise(
        ServerConfigService.pipe(
          Effect.provide(
            serverConfigLayerFromEnv({
              AUTH_ISSUER: testAuthIssuer,
              AUTH_SIGNING_SECRET: "test-secret",
              CONTROL_EPOCH_ENFORCEMENT: "nonsense",
              DATABASE_URL: "postgres://example.test/tether",
              RUNTIME_TOPOLOGY: "single",
            }),
          ),
        ),
      ),
    ).rejects.toThrow("CONTROL_EPOCH_ENFORCEMENT must be a documented boolean");
  });

  it("uses the default auth key id for blank config values", () => {
    expect(
      readConfig({
        AUTH_ISSUER: testAuthIssuer,
        AUTH_SIGNING_KID: " ",
        AUTH_SIGNING_SECRET: "test-secret",
        DATABASE_URL: "postgres://example.test/tether",
        RUNTIME_TOPOLOGY: "single",
      }),
    ).toMatchObject({
      authSigningKid: "default",
    });
  });
});
