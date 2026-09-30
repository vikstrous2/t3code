import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import * as SqlSchema from "effect/unstable/sql/SqlSchema";

import { ThreadId } from "@t3tools/contracts";

import { PersistenceDecodeError, PersistenceSqlError } from "./Errors.ts";

/** One pool checkout. See migration 055 for what each column means. */
export const WorktreePoolSlot = Schema.Struct({
  slotPath: Schema.String,
  repoKey: Schema.String,
  holderThreadId: Schema.NullOr(ThreadId),
  lastHolderThreadId: Schema.NullOr(ThreadId),
  createdAt: Schema.String,
  updatedAt: Schema.String,
});
export type WorktreePoolSlot = typeof WorktreePoolSlot.Type;

/** A thread whose uncommitted changes wait in a parked ref of this repository. */
export const WorktreePoolParkedThread = Schema.Struct({
  threadId: ThreadId,
  repoKey: Schema.String,
});
export type WorktreePoolParkedThread = typeof WorktreePoolParkedThread.Type;

export type WorktreePoolSlotsError = PersistenceSqlError | PersistenceDecodeError;

/**
 * Which pool checkouts exist and who holds them. The single source of truth
 * for slot ownership; the thread projection only mirrors leased/parked.
 */
export class WorktreePoolSlotRepository extends Context.Service<
  WorktreePoolSlotRepository,
  {
    readonly listByRepo: (
      repoKey: string,
    ) => Effect.Effect<ReadonlyArray<WorktreePoolSlot>, WorktreePoolSlotsError>;
    readonly listHeld: () => Effect.Effect<ReadonlyArray<WorktreePoolSlot>, WorktreePoolSlotsError>;
    readonly getByHolder: (
      threadId: ThreadId,
    ) => Effect.Effect<Option.Option<WorktreePoolSlot>, WorktreePoolSlotsError>;
    readonly upsert: (slot: WorktreePoolSlot) => Effect.Effect<void, WorktreePoolSlotsError>;
    readonly remove: (slotPath: string) => Effect.Effect<void, WorktreePoolSlotsError>;
    readonly setParked: (
      input: WorktreePoolParkedThread & { readonly parkedAt: string },
    ) => Effect.Effect<void, WorktreePoolSlotsError>;
    readonly getParked: (
      threadId: ThreadId,
    ) => Effect.Effect<Option.Option<WorktreePoolParkedThread>, WorktreePoolSlotsError>;
    readonly clearParked: (threadId: ThreadId) => Effect.Effect<void, WorktreePoolSlotsError>;
  }
>()("t3/persistence/WorktreePoolSlots/WorktreePoolSlotRepository") {}

const toError = (operation: string) => (cause: unknown) =>
  Schema.isSchemaError(cause)
    ? PersistenceDecodeError.fromSchemaError(operation, cause)
    : new PersistenceSqlError({ operation, cause });

const make = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const slotColumns = sql`
    slot_path AS "slotPath",
    repo_key AS "repoKey",
    holder_thread_id AS "holderThreadId",
    last_holder_thread_id AS "lastHolderThreadId",
    created_at AS "createdAt",
    updated_at AS "updatedAt"
  `;

  const listByRepo = SqlSchema.findAll({
    Request: Schema.String,
    Result: WorktreePoolSlot,
    execute: (repoKey) =>
      sql`SELECT ${slotColumns} FROM worktree_pool_slots WHERE repo_key = ${repoKey} ORDER BY created_at, slot_path`,
  });
  const listHeld = SqlSchema.findAll({
    Request: Schema.Void,
    Result: WorktreePoolSlot,
    execute: () =>
      sql`SELECT ${slotColumns} FROM worktree_pool_slots WHERE holder_thread_id IS NOT NULL`,
  });
  const getByHolder = SqlSchema.findOneOption({
    Request: ThreadId,
    Result: WorktreePoolSlot,
    execute: (threadId) =>
      sql`SELECT ${slotColumns} FROM worktree_pool_slots WHERE holder_thread_id = ${threadId}`,
  });
  const getParked = SqlSchema.findOneOption({
    Request: ThreadId,
    Result: WorktreePoolParkedThread,
    execute: (threadId) =>
      sql`
        SELECT thread_id AS "threadId", repo_key AS "repoKey"
        FROM worktree_pool_parked
        WHERE thread_id = ${threadId}
      `,
  });

  return WorktreePoolSlotRepository.of({
    listByRepo: (repoKey) =>
      listByRepo(repoKey).pipe(Effect.mapError(toError("WorktreePoolSlots.listByRepo"))),
    listHeld: () =>
      listHeld(undefined).pipe(Effect.mapError(toError("WorktreePoolSlots.listHeld"))),
    getByHolder: (threadId) =>
      getByHolder(threadId).pipe(Effect.mapError(toError("WorktreePoolSlots.getByHolder"))),
    upsert: (slot) =>
      sql`
        INSERT INTO worktree_pool_slots (
          slot_path, repo_key, holder_thread_id, last_holder_thread_id, created_at, updated_at
        )
        VALUES (
          ${slot.slotPath}, ${slot.repoKey}, ${slot.holderThreadId},
          ${slot.lastHolderThreadId}, ${slot.createdAt}, ${slot.updatedAt}
        )
        ON CONFLICT (slot_path) DO UPDATE SET
          repo_key = excluded.repo_key,
          holder_thread_id = excluded.holder_thread_id,
          last_holder_thread_id = excluded.last_holder_thread_id,
          updated_at = excluded.updated_at
      `.pipe(Effect.asVoid, Effect.mapError(toError("WorktreePoolSlots.upsert"))),
    remove: (slotPath) =>
      sql`DELETE FROM worktree_pool_slots WHERE slot_path = ${slotPath}`.pipe(
        Effect.asVoid,
        Effect.mapError(toError("WorktreePoolSlots.remove")),
      ),
    setParked: (input) =>
      sql`
        INSERT INTO worktree_pool_parked (thread_id, repo_key, parked_at)
        VALUES (${input.threadId}, ${input.repoKey}, ${input.parkedAt})
        ON CONFLICT (thread_id) DO UPDATE SET
          repo_key = excluded.repo_key,
          parked_at = excluded.parked_at
      `.pipe(Effect.asVoid, Effect.mapError(toError("WorktreePoolSlots.setParked"))),
    getParked: (threadId) =>
      getParked(threadId).pipe(Effect.mapError(toError("WorktreePoolSlots.getParked"))),
    clearParked: (threadId) =>
      sql`DELETE FROM worktree_pool_parked WHERE thread_id = ${threadId}`.pipe(
        Effect.asVoid,
        Effect.mapError(toError("WorktreePoolSlots.clearParked")),
      ),
  });
});

export const layer = Layer.effect(WorktreePoolSlotRepository, make);
