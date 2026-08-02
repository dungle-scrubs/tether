/**
 * Owns the serialized byte budget of one non-participant browser snapshot:
 * bounded keyset reads per collection and the authoritative final trim. This
 * module does not know how snapshots are routed, authorized, or rendered.
 */

import type { BrowserSessionSnapshot } from "@dungle-scrubs/tether-protocol";
import { Effect } from "effect";

/** Bounded snapshot collection page and whether more rows remain past it. */
export interface BoundedSnapshotPage<TRecord> {
  readonly hasMore: boolean;
  readonly records: TRecord[];
}

/**
 * Typical serialized size of one participant or task row. This bounds a single
 * read page only; it is deliberately not the persisted per-row CHECK ceiling,
 * because sizing pages by the worst case collapses them to a handful of rows
 * and turns one snapshot into hundreds of sequential round trips.
 */
const snapshotTypicalRecordBytes = 4 * 1_024;

/** Caps one keyset read so a page stays useful without unbounded materialization. */
function snapshotReadPageSize(maxBytes: number): number {
  return Math.max(64, Math.min(500, Math.floor(maxBytes / snapshotTypicalRecordBytes)));
}

/**
 * Reads one snapshot collection in indexed keyset pages until either the row
 * limit or the serialized byte budget is reached. The caller supplies only how
 * to fetch a page after a cursor row, so participants and tasks share one
 * accounting rule instead of two copies that can drift apart.
 */
export function listRecordsWithinSnapshotBudget<TRecord>(input: {
  readonly fetch: (before: TRecord | undefined, limit: number) => Effect.Effect<TRecord[], unknown>;
  readonly limit: number;
  readonly maxBytes: number;
}): Effect.Effect<BoundedSnapshotPage<TRecord>, unknown> {
  return Effect.gen(function* () {
    const records: TRecord[] = [];
    let before: TRecord | undefined;
    let byteLength = 0;
    for (;;) {
      // Read one row beyond the requested limit so a full page still detects
      // that more rows remain.
      const pageLimit = Math.min(
        snapshotReadPageSize(input.maxBytes),
        input.limit + 1 - records.length,
      );
      const page = yield* input.fetch(before, pageLimit);
      for (const record of page) {
        if (records.length >= input.limit) return { hasMore: true, records };
        const nextByteLength = byteLength + Buffer.byteLength(JSON.stringify(record)) + 1;
        if (nextByteLength > input.maxBytes) return { hasMore: true, records };
        byteLength = nextByteLength;
        records.push(record);
      }
      if (page.length < pageLimit) return { hasMore: false, records };
      before = page.at(-1);
    }
  });
}

/**
 * Trims a row-bounded snapshot until the complete serialized response meets its
 * byte budget. Tasks are dropped before participants and participants before
 * events, so the replay cursor stays as far forward as the budget allows.
 * Sizing is arithmetic over per-row serialized lengths rather than repeated
 * whole-snapshot serialization, with one final serialization proving the
 * committed guarantee.
 */
export function fitBrowserSnapshotWithinByteBudget(
  input: BrowserSessionSnapshot,
  maxBytes: number,
): BrowserSessionSnapshot {
  const snapshot: BrowserSessionSnapshot = {
    ...input,
    events: [...input.events],
    participants: [...input.participants],
    tasks: [...input.tasks],
    truncated: { ...input.truncated },
  };
  let byteLength = Buffer.byteLength(JSON.stringify(snapshot));
  if (byteLength <= maxBytes) return snapshot;
  const taskFit = fitCollection(snapshot.tasks, byteLength, maxBytes);
  if (taskFit !== null) {
    snapshot.tasks = snapshot.tasks.slice(0, taskFit.keep);
    snapshot.truncated.tasks = true;
    byteLength = taskFit.byteLength;
  }
  const participantFit = fitCollection(snapshot.participants, byteLength, maxBytes);
  if (participantFit !== null) {
    snapshot.participants = snapshot.participants.slice(0, participantFit.keep);
    snapshot.truncated.participants = true;
    byteLength = participantFit.byteLength;
  }
  const eventFit = fitCollection(snapshot.events, byteLength, maxBytes);
  if (eventFit !== null) {
    snapshot.events = snapshot.events.slice(0, eventFit.keep);
    snapshot.cursor = snapshot.events.at(-1)?.seq ?? 0;
    snapshot.truncated.events = true;
  }
  if (Buffer.byteLength(JSON.stringify(snapshot)) > maxBytes) {
    throw new Error("browser_snapshot_byte_budget_invalid");
  }
  return snapshot;
}

/** Longest retained prefix of one collection and the response size it leaves. */
interface SnapshotCollectionFit {
  readonly byteLength: number;
  readonly keep: number;
}

/**
 * Finds the longest prefix of one collection that brings the whole response
 * within budget, using prefix sums of the rows' serialized lengths. Trimming a
 * JSON array shortens the response by exactly the removed rows plus their
 * separators, so no candidate snapshot has to be serialized to measure it.
 */
function fitCollection(
  values: readonly unknown[],
  byteLength: number,
  maxBytes: number,
): SnapshotCollectionFit | null {
  if (byteLength <= maxBytes || values.length === 0) return null;
  const prefixByteLengths = serializedPrefixByteLengths(values);
  const total = prefixByteLengths[values.length] as number;
  const target = total - (byteLength - maxBytes);
  let lower = 0;
  let upper = values.length;
  while (lower < upper) {
    const midpoint = Math.ceil((lower + upper) / 2);
    if ((prefixByteLengths[midpoint] as number) <= target) lower = midpoint;
    else upper = midpoint - 1;
  }
  if (lower === values.length) return null;
  return { byteLength: byteLength - total + (prefixByteLengths[lower] as number), keep: lower };
}

/** Serialized byte length of every prefix of one JSON array, indexed by kept rows. */
function serializedPrefixByteLengths(values: readonly unknown[]): readonly number[] {
  const lengths = [2];
  let total = 2;
  for (const [index, value] of values.entries()) {
    total += Buffer.byteLength(JSON.stringify(value)) + (index === 0 ? 0 : 1);
    lengths.push(total);
  }
  return lengths;
}
