import type pg from "pg";

import type { DatabasePool } from "../src/db.js";

/** Raw node-postgres query shape accepted by test-only pool facades. */
export type PoolQueryInput = string | pg.QueryConfig;

/** Calls the underlying client without retaining SQL parameters. */
export type PoolQueryNext = (
  query: PoolQueryInput,
  values?: readonly unknown[],
) => Promise<pg.QueryResult<pg.QueryResultRow>>;

/** Test-only query interceptor used for fault injection and coordination. */
export type PoolQueryInterceptor = (
  query: PoolQueryInput,
  values: readonly unknown[] | undefined,
  next: PoolQueryNext,
) => Promise<pg.QueryResult<pg.QueryResultRow>>;

/** A stable, redaction-safe label for a coordinated SQL statement. */
export interface CoordinatedQuery {
  readonly class: string;
  readonly text: string;
}

/** A rendezvous attached to one exact query boundary. */
export interface CoordinatorPhase<TActor extends string> {
  readonly actors: readonly TActor[];
  readonly name: string;
  readonly position: "after" | "before";
  readonly query: CoordinatedQuery;
  /** Manual phases remain paused until the run context explicitly releases them. */
  readonly release?: "manual" | "rendezvous";
}

/** Database-side limits installed in every transaction opened by an actor. */
export interface TransactionTimeouts {
  readonly lockTimeoutMs: number;
  readonly statementTimeoutMs: number;
}

/** Configuration for one bounded coordinator scope. */
export interface PostgresConcurrencyCoordinatorOptions<TActor extends string> {
  readonly actors: readonly TActor[];
  readonly barrierTimeoutMs: number;
  readonly phases: readonly CoordinatorPhase<TActor>[];
  readonly transactionTimeouts: TransactionTimeouts;
}

/** A redacted record that an actor reached a named query boundary. */
export interface CoordinatorPhaseEvent<TActor extends string> {
  readonly actor: TActor;
  readonly backendPid: number;
  readonly name: string;
  readonly position: "after" | "before";
  readonly queryClass: string;
}

/** Bounded PostgreSQL wait metadata read without selecting query text. */
export interface CoordinatorLockWaitState {
  readonly blocked: boolean;
  readonly blockingBackendCount: number;
  readonly blockingBackendCountTruncated: boolean;
  readonly state: string | null;
  readonly waitEvent: string | null;
  readonly waitEventType: string | null;
}

/** Last named phase and lock state observed for one actor backend. */
export interface CoordinatorActorSnapshot<TActor extends string> {
  readonly actor: TActor;
  readonly backendPid: number;
  readonly lockWait: CoordinatorLockWaitState | null;
  readonly phase: {
    readonly name: string;
    readonly position: "after" | "before";
    readonly queryClass: string;
  } | null;
}

/** Redacted cleanup operation that did not complete successfully. */
export interface CoordinatorCleanupFailure<TActor extends string> {
  readonly actor: TActor | "control";
  readonly errorClass: string;
  readonly operation: "cancel" | "client-error" | "inspect" | "release" | "reset" | "rollback";
}

/** Final resource accounting captured after a coordinated run. */
export interface CoordinatorCleanupSnapshot<TActor extends string> {
  readonly blockedActorsAfterRollback: number;
  readonly cancellationAttempts: number;
  readonly cancelledActors: number;
  readonly checkedOutAfter: number;
  readonly checkedOutBefore: number;
  readonly failures: readonly CoordinatorCleanupFailure<TActor>[];
  readonly releaseAttempts: number;
  readonly releasedActors: number;
  readonly rollbackAttempts: number;
  readonly rolledBackActors: number;
}

/** Current redacted coordinator state for assertions and failure output. */
export interface CoordinatorSnapshot<TActor extends string> {
  readonly actors: readonly CoordinatorActorSnapshot<TActor>[];
  readonly cleanup: CoordinatorCleanupSnapshot<TActor> | null;
  readonly phaseEvents: readonly CoordinatorPhaseEvent<TActor>[];
}

/** Actor-scoped databases made available inside a coordinated run. */
export interface CoordinatorRunContext<TActor extends string> {
  readonly databaseFor: (actor: TActor) => DatabasePool;
  /** Releases one manually controlled phase after its required actors arrive. */
  readonly releasePhase: (name: string) => void;
  /** Polls lock state through the control backend until the actor is blocked. */
  readonly waitForLockWait: (actor: TActor) => Promise<CoordinatorLockWaitState>;
  /** Waits until every actor configured for a named phase reaches its query boundary. */
  readonly waitForPhase: (name: string) => Promise<readonly CoordinatorPhaseEvent<TActor>[]>;
}

interface ActorClient<TActor extends string> {
  readonly actor: TActor;
  readonly client: pg.PoolClient;
  database: DatabasePool;
  readonly pid: number;
  connectionError: unknown | null;
  lockWait: CoordinatorLockWaitState | null;
  readonly onClientError: (error: Error) => void;
  phase: CoordinatorActorSnapshot<TActor>["phase"];
}

interface PhaseBarrier<TActor extends string> {
  readonly arrivals: Set<TActor>;
  readonly arrived: Promise<void>;
  readonly phase: CoordinatorPhase<TActor>;
  readonly released: Promise<void>;
  reject(error: Error): void;
  resolveArrival(): void;
  resolve(): void;
  timer: NodeJS.Timeout | null;
}

/** Test-only coordinator for deterministic, bounded PostgreSQL races. */
export interface PostgresConcurrencyCoordinator<TActor extends string> {
  /** Runs work with dedicated actor clients and always cleans up those clients. */
  run<TResult>(
    operation: (context: CoordinatorRunContext<TActor>) => Promise<TResult>,
  ): Promise<TResult>;
  /** Returns only bounded labels and backend identifiers, never SQL values. */
  snapshot(): CoordinatorSnapshot<TActor>;
}

/** Cleanup failure with a fully redacted resource report. */
export class PostgresConcurrencyCleanupError<TActor extends string> extends Error {
  constructor(
    readonly cleanup: CoordinatorCleanupSnapshot<TActor>,
    readonly operationAlsoFailed: boolean,
  ) {
    super(`PostgreSQL concurrency cleanup failed: ${JSON.stringify(cleanup)}`);
    this.name = "PostgresConcurrencyCleanupError";
  }
}

/**
 * Creates a reusable description of a single coordinated run. Connections are
 * acquired only by `run`, so constructing a coordinator cannot leak clients.
 */
export function createPostgresConcurrencyCoordinator<TActor extends string>(
  database: DatabasePool,
  options: PostgresConcurrencyCoordinatorOptions<TActor>,
): PostgresConcurrencyCoordinator<TActor> {
  validateOptions(options);
  const phaseEvents: CoordinatorPhaseEvent<TActor>[] = [];
  let actorSnapshots: CoordinatorActorSnapshot<TActor>[] = [];
  let cleanupSnapshot: CoordinatorCleanupSnapshot<TActor> | null = null;
  let running = false;

  return {
    async run<TResult>(
      operation: (context: CoordinatorRunContext<TActor>) => Promise<TResult>,
    ): Promise<TResult> {
      if (running) {
        throw new Error("PostgreSQL concurrency coordinator is already running");
      }
      running = true;
      phaseEvents.length = 0;
      actorSnapshots = [];
      cleanupSnapshot = null;
      const checkedOutBefore = checkedOutClientCount(database.pool);
      let controlClient: pg.PoolClient;
      try {
        controlClient = await database.pool.connect();
      } catch (error) {
        running = false;
        throw error;
      }
      const actorClients = new Map<TActor, ActorClient<TActor>>();
      const barriers = options.phases.map((phase) => createPhaseBarrier(phase));
      const noOperationError = Symbol("no-operation-error");
      let operationError: unknown | typeof noOperationError = noOperationError;
      let operationResult:
        | { readonly status: "pending" }
        | { readonly status: "success"; readonly value: TResult } = { status: "pending" };
      try {
        const controlTimeoutMs = Math.max(
          options.barrierTimeoutMs,
          options.transactionTimeouts.statementTimeoutMs,
        );
        await controlClient.query(`SET statement_timeout = '${controlTimeoutMs}ms'`);
        for (const actor of options.actors) {
          const client = await database.pool.connect();
          try {
            const pidResult = await client.query<{ readonly pid: number }>(
              "SELECT pg_backend_pid()::int AS pid",
            );
            const pid = pidResult.rows[0]?.pid;
            if (pid === undefined) {
              throw new Error(`PostgreSQL did not return a backend PID for actor ${actor}`);
            }
            const actorClient = createActorClient(
              database,
              actor,
              client,
              pid,
              options,
              barriers,
              phaseEvents,
              controlClient,
            );
            actorClients.set(actor, actorClient);
          } catch (error) {
            client.release();
            throw error;
          }
        }

        operationResult = {
          status: "success",
          value: await operation({
            databaseFor: (actor) => {
              const actorClient = actorClients.get(actor);
              if (actorClient === undefined) {
                throw new Error(`Unknown PostgreSQL concurrency actor: ${actor}`);
              }
              return actorClient.database;
            },
            releasePhase: (name) => {
              const barrier = findPhaseBarrier(barriers, name);
              if (barrier.phase.release !== "manual") {
                throw new Error(`PostgreSQL concurrency phase ${name} is not manually released`);
              }
              if (barrier.arrivals.size !== barrier.phase.actors.length) {
                throw new Error(`PostgreSQL concurrency phase ${name} has not been reached`);
              }
              if (barrier.timer !== null) {
                clearTimeout(barrier.timer);
                barrier.timer = null;
              }
              barrier.resolve();
            },
            waitForLockWait: async (actor) => {
              const actorClient = actorClients.get(actor);
              if (actorClient === undefined) {
                throw new Error(`Unknown PostgreSQL concurrency actor: ${actor}`);
              }
              return waitForActorLockWait(controlClient, actorClient, options.barrierTimeoutMs);
            },
            waitForPhase: async (name) => {
              const barrier = findPhaseBarrier(barriers, name);
              await waitForPhaseArrival(barrier, options.barrierTimeoutMs);
              return phaseEvents.filter((event) => event.name === name);
            },
          }),
        };
      } catch (error) {
        operationError = error;
      } finally {
        for (const barrier of barriers) {
          if (barrier.timer !== null) {
            clearTimeout(barrier.timer);
          }
          barrier.resolveArrival();
          barrier.resolve();
        }
        cleanupSnapshot = await cleanupRun(
          database.pool,
          controlClient,
          actorClients.values(),
          checkedOutBefore,
        );
        actorSnapshots = [...actorClients.values()].map(snapshotActor);
        running = false;
      }

      if (cleanupSnapshot.failures.length > 0) {
        throw new PostgresConcurrencyCleanupError(
          cleanupSnapshot,
          operationError !== noOperationError,
        );
      }
      if (operationError !== noOperationError) {
        throw operationError;
      }
      if (operationResult.status !== "success") {
        throw new Error("PostgreSQL concurrency operation finished without a result");
      }
      return operationResult.value;
    },
    snapshot(): CoordinatorSnapshot<TActor> {
      return {
        actors: actorSnapshots.map((actor) => ({
          ...actor,
          lockWait: actor.lockWait === null ? null : { ...actor.lockWait },
          phase: actor.phase === null ? null : { ...actor.phase },
        })),
        cleanup: cleanupSnapshot === null ? null : { ...cleanupSnapshot },
        phaseEvents: [...phaseEvents],
      };
    },
  };
}

/** Validates bounded numeric settings and unique actor identities. */
function validateOptions<TActor extends string>(
  options: PostgresConcurrencyCoordinatorOptions<TActor>,
): void {
  if (options.actors.length === 0 || new Set(options.actors).size !== options.actors.length) {
    throw new Error("PostgreSQL concurrency actors must be a non-empty unique list");
  }
  for (const [name, value] of [
    ["barrierTimeoutMs", options.barrierTimeoutMs],
    ["lockTimeoutMs", options.transactionTimeouts.lockTimeoutMs],
    ["statementTimeoutMs", options.transactionTimeouts.statementTimeoutMs],
  ] as const) {
    if (!Number.isSafeInteger(value) || value <= 0) {
      throw new Error(`${name} must be a positive safe integer`);
    }
  }
  const actors = new Set<TActor>(options.actors);
  const phaseNames = new Set<string>();
  for (const phase of options.phases) {
    if (
      phase.name.length === 0 ||
      phase.query.class.length === 0 ||
      phase.query.text.length === 0
    ) {
      throw new Error("PostgreSQL concurrency phases require names and exact query metadata");
    }
    if (
      phase.actors.length === 0 ||
      new Set(phase.actors).size !== phase.actors.length ||
      phase.actors.some((actor) => !actors.has(actor))
    ) {
      throw new Error(`PostgreSQL concurrency phase ${phase.name} has invalid actors`);
    }
    if (phaseNames.has(phase.name)) {
      throw new Error(`Duplicate PostgreSQL concurrency phase: ${phase.name}`);
    }
    phaseNames.add(phase.name);
  }
}

/** Allocates the one-shot latch used by a named phase. */
function createPhaseBarrier<TActor extends string>(
  phase: CoordinatorPhase<TActor>,
): PhaseBarrier<TActor> {
  let resolveArrival: (() => void) | undefined;
  let resolveBarrier: (() => void) | undefined;
  let rejectBarrier: ((error: Error) => void) | undefined;
  const arrived = new Promise<void>((resolve) => {
    resolveArrival = resolve;
  });
  const released = new Promise<void>((resolve, reject) => {
    resolveBarrier = resolve;
    rejectBarrier = reject;
  });
  return {
    arrivals: new Set<TActor>(),
    arrived,
    phase,
    released,
    reject: (error) => rejectBarrier?.(error),
    resolveArrival: () => resolveArrival?.(),
    resolve: () => resolveBarrier?.(),
    timer: null,
  };
}

/** Resolves one unique named phase or rejects an invalid run-context request. */
function findPhaseBarrier<TActor extends string>(
  barriers: readonly PhaseBarrier<TActor>[],
  name: string,
): PhaseBarrier<TActor> {
  const barrier = barriers.find((candidate) => candidate.phase.name === name);
  if (barrier === undefined) {
    throw new Error(`Unknown PostgreSQL concurrency phase: ${name}`);
  }
  return barrier;
}

/** Bounds a controller waiting for actors that have not reached their phase. */
async function waitForPhaseArrival<TActor extends string>(
  barrier: PhaseBarrier<TActor>,
  timeoutMs: number,
): Promise<void> {
  let timer: NodeJS.Timeout | null = null;
  const timeout = new Promise<never>((_resolve, reject) => {
    timer = setTimeout(() => {
      reject(
        new Error(
          `PostgreSQL concurrency phase arrival timed out: ${JSON.stringify({
            actors: barrier.phase.actors,
            name: barrier.phase.name,
            position: barrier.phase.position,
            queryClass: barrier.phase.query.class,
          })}`,
        ),
      );
    }, timeoutMs);
    timer.unref();
  });
  try {
    await Promise.race([barrier.arrived, timeout]);
  } finally {
    if (timer !== null) {
      clearTimeout(timer);
    }
  }
}

/** Builds the pool facade that lends exactly one dedicated client to an actor. */
function createActorClient<TActor extends string>(
  database: DatabasePool,
  actor: TActor,
  client: pg.PoolClient,
  pid: number,
  options: PostgresConcurrencyCoordinatorOptions<TActor>,
  barriers: readonly PhaseBarrier<TActor>[],
  phaseEvents: CoordinatorPhaseEvent<TActor>[],
  controlClient: pg.PoolClient,
): ActorClient<TActor> {
  const onClientError = (error: Error): void => {
    actorClient.connectionError = error;
  };
  const actorClient: ActorClient<TActor> = {
    actor,
    client,
    connectionError: null,
    database,
    lockWait: null,
    onClientError,
    phase: null,
    pid,
  };
  client.on("error", onClientError);
  const originalQuery = client.query.bind(client) as pg.PoolClient["query"];
  const next: PoolQueryNext = (query, values) =>
    values === undefined
      ? (originalQuery(query as never) as Promise<pg.QueryResult<pg.QueryResultRow>>)
      : (originalQuery(query as never, values as never) as Promise<
          pg.QueryResult<pg.QueryResultRow>
        >);
  const wrappedClient = Object.create(client) as pg.PoolClient;
  wrappedClient.query = (async (query: PoolQueryInput, values?: readonly unknown[]) => {
    const text = queryText(query);
    await reachMatchingPhases(
      actorClient,
      "before",
      text,
      barriers,
      phaseEvents,
      options,
      controlClient,
    );
    const result = await next(query, values);
    if (isTransactionStart(text)) {
      await next(`SET LOCAL lock_timeout = '${options.transactionTimeouts.lockTimeoutMs}ms'`);
      await next(
        `SET LOCAL statement_timeout = '${options.transactionTimeouts.statementTimeoutMs}ms'`,
      );
    }
    await reachMatchingPhases(
      actorClient,
      "after",
      text,
      barriers,
      phaseEvents,
      options,
      controlClient,
    );
    return result;
  }) as pg.PoolClient["query"];
  wrappedClient.release = () => undefined;

  const wrappedPool = Object.create(database.pool) as pg.Pool;
  wrappedPool.connect = async () => wrappedClient;
  actorClient.database = { ...database, pool: wrappedPool };
  return actorClient;
}

/** Waits at every exact query phase matching this actor and boundary. */
async function reachMatchingPhases<TActor extends string>(
  actorClient: ActorClient<TActor>,
  position: "after" | "before",
  text: string,
  barriers: readonly PhaseBarrier<TActor>[],
  phaseEvents: CoordinatorPhaseEvent<TActor>[],
  options: PostgresConcurrencyCoordinatorOptions<TActor>,
  controlClient: pg.PoolClient,
): Promise<void> {
  const { actor, pid: backendPid } = actorClient;
  for (const barrier of barriers) {
    const { phase } = barrier;
    if (phase.position !== position || phase.query.text !== text || !phase.actors.includes(actor)) {
      continue;
    }
    if (barrier.arrivals.has(actor)) {
      throw new Error(`Actor ${actor} reached phase ${phase.name} more than once`);
    }
    barrier.arrivals.add(actor);
    actorClient.phase = {
      name: phase.name,
      position,
      queryClass: phase.query.class,
    };
    phaseEvents.push({
      actor,
      backendPid,
      name: phase.name,
      position,
      queryClass: phase.query.class,
    });
    if (barrier.arrivals.size === 1) {
      barrier.timer = setTimeout(async () => {
        actorClient.lockWait = await inspectActorLockWait(controlClient, actorClient).catch(
          () => unknownLockWaitState,
        );
        barrier.reject(createBarrierTimeoutError(actorClient));
      }, options.barrierTimeoutMs);
      barrier.timer.unref();
    }
    if (barrier.arrivals.size === phase.actors.length) {
      barrier.resolveArrival();
      if (phase.release !== "manual") {
        if (barrier.timer !== null) {
          clearTimeout(barrier.timer);
          barrier.timer = null;
        }
        barrier.resolve();
      }
    }
    await barrier.released;
  }
}

/** Reads SQL text without inspecting or retaining parameter values. */
function queryText(query: PoolQueryInput): string {
  return typeof query === "string" ? query : query.text;
}

/** Recognizes PostgreSQL transaction-open commands before installing local limits. */
function isTransactionStart(text: string): boolean {
  const normalized = text.trim().replaceAll(/\s+/gu, " ").toUpperCase();
  return /^(?:BEGIN(?: WORK| TRANSACTION)?|START TRANSACTION)(?: |$)/u.test(normalized);
}

/** Creates a test-only database facade that intercepts every acquired client query. */
export function wrapPoolQueries(
  database: DatabasePool,
  interceptor: PoolQueryInterceptor,
): DatabasePool {
  const wrappedPool = Object.create(database.pool) as pg.Pool;
  wrappedPool.connect = async () => {
    const client = await database.pool.connect();
    const wrappedClient = Object.create(client) as pg.PoolClient;
    const originalQuery = client.query.bind(client) as pg.PoolClient["query"];
    const next: PoolQueryNext = (query, values) =>
      values === undefined
        ? (originalQuery(query as never) as Promise<pg.QueryResult<pg.QueryResultRow>>)
        : (originalQuery(query as never, values as never) as Promise<
            pg.QueryResult<pg.QueryResultRow>
          >);
    wrappedClient.query = ((query: PoolQueryInput, values?: readonly unknown[]) =>
      interceptor(query, values, next)) as pg.PoolClient["query"];
    wrappedClient.release = client.release.bind(client);
    return wrappedClient;
  };
  return { ...database, pool: wrappedPool };
}

const unknownLockWaitState: CoordinatorLockWaitState = {
  blocked: false,
  blockingBackendCount: 0,
  blockingBackendCountTruncated: false,
  state: null,
  waitEvent: null,
  waitEventType: null,
};

/** Polls bounded control-plane state until PostgreSQL reports a lock wait. */
async function waitForActorLockWait<TActor extends string>(
  controlClient: pg.PoolClient,
  actor: ActorClient<TActor>,
  timeoutMs: number,
): Promise<CoordinatorLockWaitState> {
  const startedAt = Date.now();
  while (Date.now() - startedAt < timeoutMs) {
    const lockWait = await inspectActorLockWait(controlClient, actor);
    actor.lockWait = lockWait;
    if (lockWait.blocked) {
      return lockWait;
    }
    await waitForPoll();
  }
  throw createLockWaitTimeoutError(actor);
}

/** Reads bounded wait metadata while deliberately excluding SQL text and values. */
async function inspectActorLockWait<TActor extends string>(
  controlClient: pg.PoolClient,
  actor: ActorClient<TActor>,
): Promise<CoordinatorLockWaitState> {
  const result = await controlClient.query<{
    readonly blockingBackendCount: number;
    readonly state: string | null;
    readonly waitEvent: string | null;
    readonly waitEventType: string | null;
  }>(
    `
      SELECT
        cardinality(pg_blocking_pids(pid))::int AS "blockingBackendCount",
        state,
        wait_event AS "waitEvent",
        wait_event_type AS "waitEventType"
      FROM pg_stat_activity
      WHERE pid = $1
    `,
    [actor.pid],
  );
  const row = result.rows[0];
  if (row === undefined) {
    return unknownLockWaitState;
  }
  const blockingBackendCountTruncated = row.blockingBackendCount > 8;
  return {
    blocked: row.blockingBackendCount > 0 || row.waitEventType === "Lock",
    blockingBackendCount: Math.min(row.blockingBackendCount, 8),
    blockingBackendCountTruncated,
    state: boundedLabel(row.state),
    waitEvent: boundedLabel(row.waitEvent),
    waitEventType: boundedLabel(row.waitEventType),
  };
}

/** Performs failure-tolerant cancellation, rollback, inspection, and release. */
async function cleanupRun<TActor extends string>(
  pool: pg.Pool,
  controlClient: pg.PoolClient,
  actors: Iterable<ActorClient<TActor>>,
  checkedOutBefore: number,
): Promise<CoordinatorCleanupSnapshot<TActor>> {
  const actorList = [...actors];
  const failures: CoordinatorCleanupFailure<TActor>[] = [];
  let cancelledActors = 0;
  let releasedActors = 0;
  let rolledBackActors = 0;

  for (const actor of actorList) {
    if (actor.connectionError !== null) {
      failures.push(cleanupFailure(actor.actor, "client-error", actor.connectionError));
    }
  }
  for (const actor of actorList) {
    try {
      actor.lockWait = await inspectActorLockWait(controlClient, actor);
    } catch (error) {
      failures.push(cleanupFailure(actor.actor, "inspect", error));
    }
  }
  for (const actor of actorList) {
    try {
      await controlClient.query("SELECT pg_cancel_backend($1)", [actor.pid]);
      cancelledActors += 1;
    } catch (error) {
      failures.push(cleanupFailure(actor.actor, "cancel", error));
    }
  }
  for (const actor of actorList) {
    try {
      await actor.client.query("ROLLBACK");
      rolledBackActors += 1;
    } catch (error) {
      failures.push(cleanupFailure(actor.actor, "rollback", error));
    }
  }

  let blockedActorsAfterRollback = 0;
  for (const actor of actorList) {
    try {
      const lockWait = await inspectActorLockWait(controlClient, actor);
      if (lockWait.blocked) {
        blockedActorsAfterRollback += 1;
      }
    } catch (error) {
      failures.push(cleanupFailure(actor.actor, "inspect", error));
    }
  }
  for (const actor of actorList) {
    try {
      actor.client.off("error", actor.onClientError);
      actor.client.release();
      releasedActors += 1;
    } catch (error) {
      failures.push(cleanupFailure(actor.actor, "release", error));
    }
  }
  try {
    await controlClient.query("RESET statement_timeout");
  } catch (error) {
    failures.push(cleanupFailure<TActor>("control", "reset", error));
  }
  try {
    controlClient.release();
  } catch (error) {
    failures.push(cleanupFailure<TActor>("control", "release", error));
  }

  return {
    blockedActorsAfterRollback,
    cancellationAttempts: actorList.length,
    cancelledActors,
    checkedOutAfter: checkedOutClientCount(pool),
    checkedOutBefore,
    failures,
    releaseAttempts: actorList.length,
    releasedActors,
    rollbackAttempts: actorList.length,
    rolledBackActors,
  };
}

/** Projects cleanup errors without retaining messages that could contain SQL. */
function cleanupFailure<TActor extends string>(
  actor: TActor | "control",
  operation: CoordinatorCleanupFailure<TActor>["operation"],
  error: unknown,
): CoordinatorCleanupFailure<TActor> {
  return {
    actor,
    errorClass: boundedLabel(error instanceof Error ? error.name : typeof error) ?? "unknown",
    operation,
  };
}

/** Creates a diagnostic snapshot after all client work has settled. */
function snapshotActor<TActor extends string>(
  actor: ActorClient<TActor>,
): CoordinatorActorSnapshot<TActor> {
  return {
    actor: actor.actor,
    backendPid: actor.pid,
    lockWait: actor.lockWait,
    phase: actor.phase,
  };
}

/** Formats a phase timeout from redacted, bounded fields only. */
function createBarrierTimeoutError<TActor extends string>(actor: ActorClient<TActor>): Error {
  return new Error(
    `PostgreSQL concurrency barrier timed out: ${JSON.stringify(snapshotActor(actor))}`,
  );
}

/** Formats a lock-inspection timeout from redacted, bounded fields only. */
function createLockWaitTimeoutError<TActor extends string>(actor: ActorClient<TActor>): Error {
  return new Error(
    `PostgreSQL lock wait inspection timed out: ${JSON.stringify(snapshotActor(actor))}`,
  );
}

/** Bounds database labels included in diagnostics. */
function boundedLabel(value: string | null): string | null {
  return value === null ? null : value.slice(0, 64);
}

/** Yields briefly between control-connection lock-state samples. */
async function waitForPoll(): Promise<void> {
  await new Promise<void>((resolve) => {
    const timer = setTimeout(resolve, 10);
    timer.unref();
  });
}

/** Counts clients currently borrowed from a node-postgres pool. */
function checkedOutClientCount(pool: pg.Pool): number {
  return pool.totalCount - pool.idleCount;
}
