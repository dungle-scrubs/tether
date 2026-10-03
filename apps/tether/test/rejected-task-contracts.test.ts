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
    };
    expect(restControlAcquisitionResponseSchema.parse(response).rejectedContracts).toEqual(
      diagnostics,
    );
    expect(restControlRenewalResponseSchema.parse(response).rejectedContracts).toEqual(diagnostics);
  });

  it("suppresses repeated heartbeat diagnostics for five minutes and isolates participant and session identities", () => {
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
    expect(log).toHaveBeenCalledTimes(4);
    expect(project({ ...record, capabilities: { contracts: [valid] } }).rejectedContracts).toEqual(
      [],
    );
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
        data: { ...rejection[0], participantId: record.participantId, sessionId: record.sessionId },
      });
      expect(JSON.stringify(warn.mock.calls)).not.toContain("SYNTHETIC_PAYLOAD_DO_NOT_LOG");
      expect(entry.data).not.toHaveProperty("description");
    } finally {
      warn.mockRestore();
    }
  });

  it.each([
    "register",
    "heartbeat",
  ] as const)("reports rejection diagnostics in the %s HTTP success response and readable participant record", async (operation) => {
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
    try {
      const record = participant(`part_http_${operation}`);
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
      expect(response.body.rejectedContracts).toEqual(rejection);
      expect(response.body.participant).toMatchObject({ rejectedContracts: rejection });
      await run("GET", "", {});
      expect(response.body.participants).toEqual([{ ...record, rejectedContracts: rejection }]);
      expect(warn).toHaveBeenCalledTimes(1);
    } finally {
      warn.mockRestore();
    }
  });
});
