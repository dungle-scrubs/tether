/**
 * Human-readable task payload display prepared for client surfaces.
 */
export interface TaskPayloadDisplay {
  /** Stable label for the payload section. */
  readonly label: "Failure" | "Result";
  /** Lines that can be rendered directly by text-based clients. */
  readonly lines: readonly string[];
}

/** Formats a successful task result payload for text clients. */
export function formatTaskResultPayload(
  payload: Record<string, unknown> | null,
): TaskPayloadDisplay {
  return formatTaskPayload("Result", payload);
}

/** Formats a failed task payload for text clients. */
export function formatTaskFailurePayload(
  payload: Record<string, unknown> | null,
): TaskPayloadDisplay {
  return formatTaskPayload("Failure", payload);
}

/**
 * Formats a task payload using common agent result conventions with a stable
 * JSON fallback for unknown shapes.
 */
function formatTaskPayload(
  label: TaskPayloadDisplay["label"],
  payload: Record<string, unknown> | null,
): TaskPayloadDisplay {
  if (payload === null) {
    return { label, lines: ["none"] };
  }
  const lines = [
    ...formatStringField(payload, "summary"),
    ...formatStringField(payload, "output"),
    ...formatStringArrayField(payload, "actions"),
    ...formatStringField(payload, "error"),
  ];
  if (lines.length > 0) {
    return { label, lines };
  }
  return { label, lines: [JSON.stringify(payload)] };
}

/** Formats one string field when present. */
function formatStringField(payload: Record<string, unknown>, field: string): readonly string[] {
  const value = payload[field];
  if (typeof value !== "string" || value.length === 0) {
    return [];
  }
  return [value];
}

/** Formats one string-array field as bullet lines when present. */
function formatStringArrayField(
  payload: Record<string, unknown>,
  field: string,
): readonly string[] {
  const value = payload[field];
  if (!Array.isArray(value)) {
    return [];
  }
  const items = value.filter((item): item is string => typeof item === "string" && item.length > 0);
  return items.map((item) => `- ${item}`);
}
