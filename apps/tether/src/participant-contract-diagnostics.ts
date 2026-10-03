import { rejectedTaskContractDiagnostics } from "@dungle-scrubs/tether-protocol";
import { ConsoleStructuredLogger, type StructuredLogger } from "./observability.js";
import type { ParticipantRecord } from "./types.js";

/** Bounded process-local warning suppression shared by writes and discovery reads. */
export function createParticipantContractDiagnostics(
  logger: StructuredLogger = new ConsoleStructuredLogger(),
  now: () => number = Date.now,
) {
  const warned = new Map<string, string>();
  const maxParticipants = 4096;
  const maxWarningsPerMinute = 4096;
  let windowStart = now();
  let warningsInWindow = 0;
  return (participant: ParticipantRecord): ParticipantRecord => {
    const diagnostics = rejectedTaskContractDiagnostics(participant.capabilities);
    const { rejectedContracts, truncated } = diagnostics;
    const key = JSON.stringify([participant.sessionId, participant.participantId]);
    const { fingerprint } = diagnostics;
    const previous = warned.get(key);
    if (rejectedContracts.length === 0) {
      warned.delete(key);
    } else if (previous === fingerprint) {
      // Refresh LRU order without expiring unchanged diagnostics or scanning the cache.
      warned.delete(key);
      warned.set(key, fingerprint);
    } else {
      const timestamp = now();
      if (timestamp - windowStart >= 60_000) {
        windowStart = timestamp;
        warningsInWindow = 0;
      }
      // An overflow cycle must stay bounded even when more participants than the cache repeat.
      if (warningsInWindow >= maxWarningsPerMinute) {
        return { ...participant, rejectedContracts, rejectedContractsTruncated: truncated };
      }
      warningsInWindow += 1;
      warned.delete(key);
      if (warned.size >= maxParticipants) {
        const oldest = warned.keys().next().value;
        if (oldest !== undefined) warned.delete(oldest);
      }
      warned.set(key, fingerprint);
      logger.log({
        at: new Date(timestamp).toISOString(),
        data: {
          participantId: participant.participantId,
          sessionId: participant.sessionId,
          rejectedContracts,
          truncated,
        },
        level: "warn",
        message: "participant.contract_rejected",
        moduleName: "ParticipantContracts",
        operation: "validate",
        traceId: "",
      });
    }
    return { ...participant, rejectedContracts, rejectedContractsTruncated: truncated };
  };
}

export const projectParticipantContractDiagnostics = createParticipantContractDiagnostics();
