import type { IncomingMessage, ServerResponse } from "node:http";
import type { URL } from "node:url";

import { sessionSummaryCandidateSubmissionSchema } from "@dungle-scrubs/tether-protocol";
import { Effect } from "effect";

import {
  authorize,
  authorizeParticipantIdentity,
  effectiveParticipantId,
} from "./auth/authorize.js";
import type { AuthContext } from "./auth/token.js";
import { parseJsonBody, sendAuthError, sendJson } from "./http-route-runtime.js";
import { defineHttpRoute, matchHttpRoute } from "./http-route-spec.js";
import type { ResourceLimits } from "./resource-limits.js";
import { SessionSummaryPolicyError } from "./session-summary-policy.js";
import { type SessionSummaryStore, SessionSummaryStoreError } from "./session-summary-store.js";

/** Dedicated authenticated and fenced Session Summary transport route. */
export const sessionSummaryHttpRoutes = {
  candidateSubmit: defineHttpRoute({
    control: "fenced",
    method: "POST",
    name: "session.summary.candidate.submit",
    pattern: /^\/sessions\/([^/]+)\/tasks\/([^/]+)\/summary-candidate$/u,
  }),
} as const;

interface SessionSummaryHttpRouteInput {
  readonly authContext: AuthContext | null;
  readonly request: IncomingMessage;
  readonly resourceLimits: ResourceLimits;
  readonly response: ServerResponse;
  readonly store: SessionSummaryStore;
  readonly url: URL;
}

/** Routes dedicated candidate submission independently from generic task completion. */
export function handleSessionSummaryHttpRoute(
  input: SessionSummaryHttpRouteInput,
): Effect.Effect<boolean, unknown> {
  return Effect.gen(function* () {
    const match = matchHttpRoute(
      sessionSummaryHttpRoutes.candidateSubmit,
      input.request.method,
      input.url.pathname,
    );
    if (!match?.[1] || !match[2]) {
      return false;
    }
    const sessionId = routeParam(match, 1);
    const taskId = routeParam(match, 2);
    const authError = authorize({
      action: "task-mutate",
      context: input.authContext,
      sessionId,
    });
    if (authError) {
      sendAuthError(input.response, authError);
      return true;
    }
    const body = yield* parseJsonBody(input.request, sessionSummaryCandidateSubmissionSchema, {
      maxBytes: input.resourceLimits.httpMaxBodyBytes,
      routeName: sessionSummaryHttpRoutes.candidateSubmit.name,
    });
    if (body.sessionId !== sessionId || body.taskId !== taskId) {
      sendJson(input.response, 409, {
        error: "Candidate route identity does not match submission",
        reason: "candidate_identity_mismatch",
      });
      return true;
    }
    const participantError = authorizeParticipantIdentity(input.authContext, body.claimantId);
    if (participantError) {
      sendAuthError(input.response, participantError);
      return true;
    }
    const submission = {
      ...body,
      claimantId: effectiveParticipantId(input.authContext, body.claimantId),
    };
    const published = yield* Effect.tryPromise({
      catch: (error) => error,
      try: () => input.store.submitCandidate(submission),
    });
    sendJson(input.response, 201, published);
    return true;
  }).pipe(
    Effect.catchAll((error) => {
      const response = sessionSummaryFailureResponse(error);
      if (!response) {
        return Effect.fail(error);
      }
      return Effect.sync(() => {
        sendJson(input.response, response.status, {
          error: response.message,
          reason: response.reason,
        });
        return true;
      });
    }),
  );
}

/** Maps expected worker outcomes to stable route-safe 4xx responses. */
function sessionSummaryFailureResponse(error: unknown): {
  readonly message: string;
  readonly reason: string;
  readonly status: number;
} | null {
  if (error instanceof SessionSummaryPolicyError) {
    return {
      message: "Session Summary candidate rejected by publication policy",
      reason: error.code,
      status: error.code === "unsafe_sequence" ? 400 : 409,
    };
  }
  if (!(error instanceof SessionSummaryStoreError)) {
    return null;
  }
  const status =
    error.code === "summary_not_found" || error.code === "task_not_found"
      ? 404
      : error.code === "integrity_mismatch"
        ? 422
        : 409;
  return {
    message: "Session Summary candidate rejected",
    reason: error.code,
    status,
  };
}

/** Reads one required regex capture. */
function routeParam(match: RegExpMatchArray, index: number): string {
  const value = match[index];
  if (!value) {
    throw new Error(`Missing Session Summary route parameter ${index}`);
  }
  return value;
}
