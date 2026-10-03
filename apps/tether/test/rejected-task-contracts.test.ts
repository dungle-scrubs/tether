import type { IncomingMessage, ServerResponse } from "node:http";
import { Readable } from "node:stream";
import { Effect } from "effect";
import { describe, expect, it, vi } from "vitest";
import {
  participantRecordSchema,
  rejectedTaskContracts,
  restControlAcquisitionResponseSchema,
  restControlRenewalResponseSchema,
} from "@dungle-scrubs/tether-protocol";
import { createParticipantContractDiagnostics } from "../src/participant-contract-diagnostics.js";
import { HostPresenceRuntime } from "../src/host-presence.js";
import { handleSessionHttpRoute } from "../src/http-session-route-handlers.js";
import type { SubscriptionHub } from "../src/hub.js";
import type { ResourceLimits } from "../src/resource-limits.js";
import { buildParticipantTaskContracts } from "../src/session-service-context.js";
import type { SessionServiceEffect } from "../src/session-service.js";
import type { ParticipantRecord } from "../src/types.js";

const valid = {
  approval: "none",
  description: "Synthetic contract",
  inputSchemaRef: "synthetic.input",
  participantRuntimeKind: "worker",
  readOnlyByDefault: true,
  resultSchemaRef: "synthetic.result",
  taskKind: "synthetic.task",
  title: "Synthetic",
  version: "1",
};
const { participantRuntimeKind: _runtime, ...missingRuntime } = valid;
const invalid = { ...missingRuntime, description: "SYNTHETIC_PAYLOAD_DO_NOT_LOG" };
const rejection = [
  {
    index: 1,
    taskKind: "synthetic.task",
    issues: [{ path: ["participantRuntimeKind"], code: "invalid_type" }],
  },
];
function participant(id: string): ParticipantRecord {
  return {
    capabilities: { contracts: [valid, invalid] },
    displayName: "Worker",
    joinedAt: "2026-01-01T00:00:00.000Z",
    lastSeenAt: "2026-01-01T00:00:00.000Z",
    participantId: id,
    runtimeKind: "worker",
    sessionId: "sess_rejections",
  };
}

describe("rejected task contracts", () => {
  it("returns only paths and codes for malformed entries and accepts diagnostics in protocol records", () => {
    const capabilities = {
      contracts: [null, { ...valid, taskKind: 42, approval: "SYNTHETIC_BAD_ENUM" }, valid],
    };
    const diagnostics = rejectedTaskContracts(capabilities);
    expect(diagnostics).toEqual([
      { index: 0, issues: [{ path: [], code: "invalid_type" }] },
      {
        index: 1,
        issues: [
          { path: ["approval"], code: "invalid_value" },
          { path: ["taskKind"], code: "invalid_type" },
        ],
      },
    ]);
    const record = {
      ...participant("part_protocol"),
      capabilities,
      rejectedContracts: diagnostics,
      rejectedContractsTruncated: 0,
    };
    expect(participantRecordSchema.parse(record)).toEqual(record);
    expect(JSON.stringify(diagnostics)).not.toContain("SYNTHETIC_BAD_ENUM");
    const response = {
      acquisitionId: "acq_synthetic",
      acquisitionStatus: "claimed",
      registrationStatus: "joined",
      controlEpoch: 1,
      leaseExpiresAt: "2026-01-01T00:01:00.000Z",
      renewAfterMs: 1000,
      participant: record,
      rejectedContracts: diagnostics,
      rejectedContractsTruncated: 7,
    };
    expect(restControlAcquisitionResponseSchema.parse(response).rejectedContractsTruncated).toBe(7);
    expect(restControlRenewalResponseSchema.parse(response).rejectedContractsTruncated).toBe(7);
    expect(restControlAcquisitionResponseSchema.parse(response).rejectedContracts).toEqual(
      diagnostics,
    );
    expect(restControlRenewalResponseSchema.parse(response).rejectedContracts).toEqual(diagnostics);
  });

  it("omits task kinds beyond 200 UTF-8 bytes from records and warning data", () => {
    const log = vi.fn();
    const project = createParticipantContractDiagnostics({ log }, () => 0);
    const contracts = ["a".repeat(200), "a".repeat(201), "é".repeat(100), "é".repeat(101)].map(
      (taskKind) => ({ ...invalid, taskKind }),
    );
    const record = project({ ...participant("part_task_kind_size"), capabilities: { contracts } });
    expect(record.rejectedContracts?.map((item) => item.taskKind)).toEqual([
      "a".repeat(200),
      undefined,
      "é".repeat(100),
      undefined,
    ]);
    const serialized = JSON.stringify(log.mock.calls);
    expect(serialized).not.toContain("a".repeat(201));
    expect(serialized).not.toContain("é".repeat(101));
  });

  it("bounds examined contracts, reported rejections and issues with explicit truncation counts", () => {
    const log = vi.fn();
    const project = createParticipantContractDiagnostics({ log }, () => 0);
    const contracts = Array.from({ length: 10_000 }, () => ({}));
    Object.defineProperty(contracts, 128, {
      get() {
        throw new Error("diagnostics examined past the limit");
      },
    });
    const record = project({ ...participant("part_caps"), capabilities: { contracts } });
    expect(record.rejectedContracts).toHaveLength(32);
    expect(record).toMatchObject({ rejectedContractsTruncated: 9968 });
    expect(participantRecordSchema.parse(record).rejectedContractsTruncated).toBe(9968);
    expect(record.rejectedContracts?.[0]).toMatchObject({
      issues: expect.any(Array),
      truncated: 1,
    });
    expect(record.rejectedContracts?.[0]?.issues).toHaveLength(8);
    expect(log).toHaveBeenCalledTimes(1);
    expect(log.mock.calls[0]?.[0].data).toMatchObject({ truncated: 9968 });
    expect(JSON.stringify(record.rejectedContracts).length).toBeLessThan(20_000);
  });

  it("deduplicates whole participant rejection sets beyond the old 4096-entry capacity", () => {
    const log = vi.fn();
    const project = createParticipantContractDiagnostics({ log }, () => 0);
    const records = Array.from({ length: 1100 }, (_, index) => ({
      ...participant(`part_capacity_${index}`),
      sessionId: `sess_capacity_${index % 50}`,
      capabilities: { contracts: [invalid, invalid, invalid, invalid] },
    }));
    for (const record of records) project(record);
    expect(log).toHaveBeenCalledTimes(1100);
    for (const record of records) project(record);
    expect(log).toHaveBeenCalledTimes(1100);
    project({
      ...participant("part_capacity_0"),
      sessionId: "sess_capacity_0",
      capabilities: { contracts: [invalid] },
    });
    expect(log).toHaveBeenCalledTimes(1101);
  });

  it("does not scan the warning cache when projecting a participant without rejections", () => {
    const project = createParticipantContractDiagnostics({ log: vi.fn() }, () => 0);
    for (let index = 0; index < 100; index += 1) project(participant(`part_hot_path_${index}`));
    const iterate = vi.spyOn(Map.prototype, Symbol.iterator);
    let iterations: number;
    try {
      project({ ...participant("part_valid_hot_path"), capabilities: { contracts: [valid] } });
      iterations = iterate.mock.calls.length;
    } finally {
      iterate.mockRestore();
    }
    expect(iterations).toBe(0);
  });

  it("rate limits warning-cache overflow instead of flooding on repeated heartbeat cycles", () => {
    let now = 0;
    const log = vi.fn();
    const project = createParticipantContractDiagnostics({ log }, () => now);
    const records = Array.from({ length: 4200 }, (_, index) =>
      participant(`part_overflow_${index}`),
    );
    for (const record of records) project(record);
    for (const record of records) project(record);
    expect(log).toHaveBeenCalledTimes(4096);
    now = 60_000;
    project(participant("part_overflow_4199"));
    expect(log).toHaveBeenCalledTimes(4097);
    project(participant("part_overflow_4199"));
    expect(log).toHaveBeenCalledTimes(4097);
  });

  it("notices changes to examined rejections even beyond the reported subset", () => {
    const log = vi.fn();
    const project = createParticipantContractDiagnostics({ log }, () => 0);
    const record = {
      ...participant("part_fingerprint"),
      capabilities: { contracts: Array.from({ length: 128 }, () => invalid) },
    };
    project(record);
    project({
      ...record,
      capabilities: { contracts: [...record.capabilities.contracts.slice(0, 127), {}] },
    });
    expect(log).toHaveBeenCalledTimes(2);
  });

  it("suppresses unchanged diagnostics indefinitely and isolates participant and session identities", () => {
    let now = 0;
    const log = vi.fn();
    const project = createParticipantContractDiagnostics({ log }, () => now);
    const record = participant("part_suppression");
    project(record);
    for (let i = 0; i < 10; i += 1) project(record);
    expect(log).toHaveBeenCalledTimes(1);
    project({ ...record, participantId: "part_other" });
    project({ ...record, sessionId: "sess_other" });
    expect(log).toHaveBeenCalledTimes(3);
    now = 300_000;
    project(record);
    expect(log).toHaveBeenCalledTimes(3);
    now = 86_400_000;
    project(record);
    expect(log).toHaveBeenCalledTimes(3);
    expect(project({ ...record, capabilities: { contracts: [valid] } }).rejectedContracts).toEqual(
      [],
    );
    project(record);
    expect(log).toHaveBeenCalledTimes(4);
  });

  it("excludes invalid contracts and warns once with paths and codes but no payload values", () => {
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
    try {
      const record = participant("part_listing_rejections");
      const contracts = buildParticipantTaskContracts([record]);
      expect(contracts).toEqual([
        {
          ...valid,
          displayName: record.displayName,
          participantId: record.participantId,
          runtimeKind: record.runtimeKind,
          sessionId: record.sessionId,
        },
      ]);
      buildParticipantTaskContracts([record]);
      expect(warn).toHaveBeenCalledTimes(1);
      const entry = JSON.parse(String(warn.mock.calls[0]?.[0]));
      expect(entry).toMatchObject({
        level: "warn",
        message: "participant.contract_rejected",
        data: {
          rejectedContracts: rejection,
          truncated: 0,
          participantId: record.participantId,
          sessionId: record.sessionId,
        },
      });
      expect(JSON.stringify(warn.mock.calls)).not.toContain("SYNTHETIC_PAYLOAD_DO_NOT_LOG");
      expect(entry.data).not.toHaveProperty("description");
    } finally {
      warn.mockRestore();
    }
  });

  it.each([
    ["register", false],
    ["heartbeat", false],
    ["register", true],
    ["heartbeat", true],
  ] as const)("reports rejection diagnostics in the %s HTTP success response and readable participant record (large: %s)", async (operation, large) => {
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
    try {
      const record = participant(`part_http_${operation}_${large}`);
      if (large) record.capabilities.contracts = Array.from({ length: 1000 }, () => invalid);
      const expectedRejections = large
        ? Array.from({ length: 32 }, (_, index) => ({ ...rejection[0], index }))
        : rejection;
      const truncated = large ? 968 : 0;
      const result = {
        acquisitionId: "acq_synthetic",
        acquisitionStatus: "acquired",
        controlEpoch: 1,
        events: [],
        leaseExpiresAt: "2026-01-01T00:01:00.000Z",
        participant: record,
        registrationStatus: "joined",
        renewAfterMs: 1000,
        status: "ok",
      };
      const service = {
        registerRestParticipant: () => Effect.succeed(result),
        heartbeatRestParticipant: () => Effect.succeed(result),
        listParticipants: () => Effect.succeed([record]),
      } as unknown as SessionServiceEffect;
      const response = {
        statusCode: 0,
        body: {} as Record<string, unknown>,
        writeHead(status: number) {
          this.statusCode = status;
        },
        end(payload: string) {
          this.body = JSON.parse(payload);
        },
      };
      async function run(method: string, suffix: string, body: unknown) {
        const request = Readable.from([JSON.stringify(body)]) as unknown as IncomingMessage;
        request.method = method;
        await Effect.runPromise(
          handleSessionHttpRoute({
            authContext: null,
            hostPresence: new HostPresenceRuntime(),
            hub: { broadcast: () => {} } as unknown as SubscriptionHub,
            replicaId: "replica_test",
            request,
            resourceLimits: { httpMaxBodyBytes: 1_000_000 } as ResourceLimits,
            response: response as unknown as ServerResponse,
            runtimeTopology: "single",
            service,
            url: new URL(`http://localhost/sessions/${record.sessionId}/participants${suffix}`),
          }),
        );
      }
      await run("POST", operation === "register" ? "" : `/${record.participantId}/heartbeat`, {
        capabilities: record.capabilities,
        participantId: record.participantId,
        runtimeKind: record.runtimeKind,
        controlChannel: "rest",
        instanceId: "inst_synthetic",
        controlEpoch: 1,
        acquisitionId: "acq_synthetic",
      });
      expect(response.statusCode).toBe(operation === "register" ? 201 : 200);
      expect(response.body.rejectedContracts).toEqual(expectedRejections);
      expect(response.body.rejectedContractsTruncated).toBe(truncated);
      expect(response.body.participant).toMatchObject({
        rejectedContracts: expectedRejections,
        rejectedContractsTruncated: truncated,
      });
      await run("GET", "", {});
      expect(response.body.participants).toEqual([
        { ...record, rejectedContracts: expectedRejections, rejectedContractsTruncated: truncated },
      ]);
      expect(warn).toHaveBeenCalledTimes(1);
    } finally {
      warn.mockRestore();
    }
  });
});
