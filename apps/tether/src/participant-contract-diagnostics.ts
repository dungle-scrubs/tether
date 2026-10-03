import { rejectedTaskContracts } from "@dungle-scrubs/tether-protocol";
import { ConsoleStructuredLogger, type StructuredLogger } from "./observability.js";
import type { ParticipantRecord } from "./types.js";

/** Bounded process-local warning suppression shared by writes and discovery reads. */
export function createParticipantContractDiagnostics(
  logger: StructuredLogger = new ConsoleStructuredLogger(),
  now: () => number = Date.now,
) {
  const warned = new Map<string, number>();
  const suppressionMs = 5 * 60_000;
  const maxWarnings = 4096;
  return (participant: ParticipantRecord): ParticipantRecord => {
    const rejectedContracts = rejectedTaskContracts(participant.capabilities);
    const timestamp = now();
    for (const [key, expiresAt] of warned) {
      if (expiresAt <= timestamp) {
        warned.delete(key);
      }
    }
    for (const rejection of rejectedContracts) {
      const data = {
        ...rejection,
        participantId: participant.participantId,
        sessionId: participant.sessionId,
      };
      const key = JSON.stringify(data);
      if (warned.has(key)) {
        continue;
      }
      if (warned.size >= maxWarnings) {
        const oldest = warned.keys().next().value;
        if (oldest !== undefined) {
          warned.delete(oldest);
        }
      }
      warned.set(key, timestamp + suppressionMs);
      logger.log({
        at: new Date(timestamp).toISOString(),
        data,
        level: "warn",
        message: "participant.contract_rejected",
        moduleName: "ParticipantContracts",
        operation: "validate",
        traceId: "",
      });
    }
    return { ...participant, rejectedContracts };
  };
}

export const projectParticipantContractDiagnostics = createParticipantContractDiagnostics();
