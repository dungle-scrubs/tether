/**
 * REST-only entry for external participants: the lease lifecycle client,
 * the REST task-route client, and auth-token helpers. Import from
 * `@dungle-scrubs/tether-client/rest` to stay free of the WebSocket
 * runtime and its dependencies.
 */

export { readServiceAuthToken, resolveServiceAuthToken } from "./auth-token.js";
export type {
  RestParticipantControlClientConfig,
  RestParticipantControlClientDebugInfo,
  RestParticipantControlClientOptions,
  RestParticipantControlContext,
  RestParticipantControlErrorCode,
  RestParticipantControlFetch,
  RestParticipantControlTimerHandle,
  RestParticipantControlTimerScheduler,
} from "./rest-participant-control-client.js";
export {
  RestParticipantControlClient,
  RestParticipantControlError,
} from "./rest-participant-control-client.js";
export type {
  AppendTaskEventCall,
  FencedTaskCall,
  ListEventsQuery,
  RecordTaskApprovalCall,
  RestParticipantTaskClientConfig,
  RestParticipantTaskClientOptions,
  RestParticipantTaskErrorCode,
  RestParticipantTaskFetch,
} from "./rest-participant-task-client.js";
export {
  RestParticipantTaskClient,
  RestParticipantTaskError,
} from "./rest-participant-task-client.js";
