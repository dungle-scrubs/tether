/** Owns REST task reservations under a budget independently equal to serial delivery. */
import { ParticipantRestReplayError } from "./participant-rest-replay.js";
import type { TaskRecord } from "./types.js";

/** Counts a retained canonical task record for the recovery-state owner. */
function replayTaskByteCost(task: TaskRecord): number {
  return Buffer.byteLength(JSON.stringify(task));
}

/** One queued task's reservation, shared by replay, snapshot and live admission. */
interface QueuedRecoveryTask {
  readonly kind: "queued";
  readonly task: TaskRecord;
  readonly generation: number;
  readonly bytes: number;
  readonly touchedGeneration: number | null;
}

/** Exclusive states retained under a single task-id reservation. */
type RecoveryTaskSlot =
  | QueuedRecoveryTask
  | {
      readonly kind: "running";
      readonly task: TaskRecord;
      readonly generation: number;
      readonly activeBytes: number;
      readonly next: QueuedRecoveryTask | null;
      readonly bytes: number;
      readonly touchedGeneration: number | null;
    }
  | {
      readonly kind: "touched";
      readonly generation: number;
      readonly bytes: number;
      readonly touchedGeneration: number;
    };

/** Owns all REST candidate, running and snapshot-touch reservations. */
export class RecoveryTaskState {
  private readonly slots = new Map<string, RecoveryTaskSlot>();
  private bytes = 0;
  private snapshotGeneration: number | null = null;

  /** Uses the existing delivery policy without adding a runtime tuning surface. */
  constructor(
    private readonly limits: { readonly maxQueueBytes: number; readonly maxQueueSize: number },
  ) {}

  /** Creates one canonical queued reservation with a precomputed cost. */
  private queued(
    task: TaskRecord,
    generation: number,
    touchedGeneration: number | null,
  ): QueuedRecoveryTask {
    return { kind: "queued", task, generation, bytes: replayTaskByteCost(task), touchedGeneration };
  }

  /** Checks a replacement's net charge before retaining it. */
  private put(taskId: string, slot: RecoveryTaskSlot): void {
    const previous = this.slots.get(taskId);
    const observedQueueSize = this.slots.size + (previous ? 0 : 1);
    if (observedQueueSize > this.limits.maxQueueSize) {
      throw new ParticipantRestReplayError("delivery_queue_overflow", {
        maxQueueSize: this.limits.maxQueueSize,
        observedQueueSize,
      });
    }
    const observedQueueBytes = this.bytes - (previous?.bytes ?? 0) + slot.bytes;
    if (observedQueueBytes > this.limits.maxQueueBytes) {
      throw new ParticipantRestReplayError("delivery_byte_overflow", {
        maxQueueBytes: this.limits.maxQueueBytes,
        observedQueueBytes,
      });
    }
    this.slots.set(taskId, slot);
    this.bytes = observedQueueBytes;
  }

  /** Releases exactly the reservation currently owned by one task id. */
  private remove(taskId: string): void {
    const slot = this.slots.get(taskId);
    if (!slot) return;
    this.bytes -= slot.bytes;
    this.slots.delete(taskId);
  }

  /** Observes canonical lifecycle state while preserving running claim ownership. */
  observe(task: TaskRecord, generation: number, claimable: boolean, selected: boolean): void {
    const previous = this.slots.get(task.taskId);
    const touchedGeneration = this.snapshotGeneration === generation ? generation : null;
    if (previous?.kind === "running") {
      const next =
        selected && (claimable || previous.next) ? this.queued(task, generation, null) : null;
      this.put(task.taskId, {
        ...previous,
        next,
        bytes: previous.activeBytes + (next?.bytes ?? 0),
        touchedGeneration,
      });
    } else if (selected && (claimable || previous?.kind === "queued")) {
      this.put(task.taskId, this.queued(task, generation, touchedGeneration));
    } else if (touchedGeneration !== null) {
      this.put(task.taskId, {
        kind: "touched",
        generation,
        touchedGeneration,
        bytes: Buffer.byteLength(JSON.stringify(task.taskId)),
      });
    } else {
      this.remove(task.taskId);
    }
  }

  /** Starts one generation's snapshot window without a separate unbounded touched-id set. */
  beginSnapshot(generation: number): void {
    this.snapshotGeneration = generation;
  }

  /** Merges only rows not superseded by live state or a running claim. */
  seed(task: TaskRecord, generation: number): void {
    const previous = this.slots.get(task.taskId);
    if (previous?.kind === "running" || previous?.touchedGeneration === generation) return;
    this.put(task.taskId, this.queued(task, generation, null));
  }

  /** Releases snapshot tombstones and flags, without touching a newer snapshot. */
  endSnapshot(generation: number): void {
    if (this.snapshotGeneration === generation) this.snapshotGeneration = null;
    for (const [taskId, slot] of this.slots) {
      if (slot.touchedGeneration !== generation) continue;
      if (slot.kind === "touched") this.remove(taskId);
      else this.slots.set(taskId, { ...slot, touchedGeneration: null });
    }
  }

  /** Takes the oldest eligible queued task; its reservation remains charged while running. */
  take(generation: number): TaskRecord | null {
    for (const [taskId, slot] of this.slots) {
      if (slot.kind !== "queued" || slot.generation !== generation) continue;
      this.slots.set(taskId, { ...slot, kind: "running", activeBytes: slot.bytes, next: null });
      return slot.task;
    }
    return null;
  }

  /** Releases settled running work or retains its later claimable lifecycle update. */
  complete(taskId: string): void {
    const slot = this.slots.get(taskId);
    if (slot?.kind !== "running") return;
    if (slot.next) this.put(taskId, { ...slot.next, touchedGeneration: slot.touchedGeneration });
    else this.remove(taskId);
  }

  /** Returns whether this generation has work waiting behind the active claim. */
  hasPending(generation: number): boolean {
    for (const slot of this.slots.values()) {
      if (slot.kind === "queued" && slot.generation === generation) return true;
      if (slot.kind === "running" && slot.next?.generation === generation) return true;
    }
    return false;
  }

  /** Discards stale queued work while retaining the one running claim's reservation. */
  discard(generation: number): void {
    if (this.snapshotGeneration === generation) this.snapshotGeneration = null;
    for (const [taskId, slot] of this.slots) {
      if (slot.kind !== "running") {
        if (slot.generation === generation) this.remove(taskId);
      } else {
        const next = slot.next?.generation === generation ? null : slot.next;
        this.put(taskId, {
          ...slot,
          next,
          bytes: slot.activeBytes + (next?.bytes ?? 0),
          touchedGeneration: slot.touchedGeneration === generation ? null : slot.touchedGeneration,
        });
      }
    }
  }
}
