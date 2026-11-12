/** Maximum number of event types one event-list request may exclude. */
export const maxEventListExcludeTypes = 16;

/** Maximum length of one excluded event type. */
export const maxEventListExcludeTypeLength = 128;

/**
 * ISO 8601 date-time with an explicit offset or `Z`, the only `since` shape
 * the event list accepts.
 */
const eventListSincePattern =
  /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}(:\d{2}(\.\d{1,9})?)?(Z|[+-]\d{2}:\d{2})$/u;

/** Parsed `since` parameter of the REST event list. */
export type EventListSince =
  | { readonly status: "absent" }
  | { readonly status: "invalid" }
  | { readonly status: "present"; readonly since: Date };

/** Parsed `exclude` parameter of the REST event list. */
export type EventListExclude =
  | { readonly status: "absent" }
  | { readonly status: "invalid" }
  | { readonly status: "present"; readonly types: readonly string[] };

/**
 * Parses the `since` parameter of the REST event list.
 *
 * The value is an ISO 8601 date-time with an explicit offset or `Z`. A date
 * without a time, a local time without an offset, and an unparseable value are
 * all refused, so a caller never silently reads a window it did not ask for.
 */
export function parseEventListSince(value: string | null): EventListSince {
  if (value === null) {
    return { status: "absent" };
  }
  const trimmed = value.trim();
  if (!eventListSincePattern.test(trimmed)) {
    return { status: "invalid" };
  }
  const parsed = Date.parse(trimmed);
  if (!Number.isFinite(parsed)) {
    return { status: "invalid" };
  }
  return { status: "present", since: new Date(parsed) };
}

/**
 * Parses the `exclude` parameter of the REST event list.
 *
 * The value is a comma-separated list of event types. Entries are trimmed of
 * surrounding whitespace, an empty entry is refused, and the list carries at
 * most `maxEventListExcludeTypes` entries of at most
 * `maxEventListExcludeTypeLength` characters each.
 */
export function parseEventListExclude(value: string | null): EventListExclude {
  if (value === null) {
    return { status: "absent" };
  }
  const types = value.split(",").map((entry) => entry.trim());
  if (types.length > maxEventListExcludeTypes) {
    return { status: "invalid" };
  }
  for (const type of types) {
    if (type.length === 0 || type.length > maxEventListExcludeTypeLength) {
      return { status: "invalid" };
    }
  }
  return { status: "present", types };
}
