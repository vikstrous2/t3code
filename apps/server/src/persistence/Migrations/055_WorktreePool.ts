import * as Effect from "effect/Effect";
import * as SqlClient from "effect/unstable/sql/SqlClient";

export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const columns = yield* sql<{ readonly name: string }>`
    PRAGMA table_info(projection_threads)
  `;
  // Null for every thread that is not pooled, which is every existing one.
  if (!columns.some((column) => column.name === "worktree_pool_state")) {
    yield* sql`
      ALTER TABLE projection_threads
      ADD COLUMN worktree_pool_state TEXT
    `;
  }

  // One row per pool checkout. `repo_key` is the repository's git common dir,
  // shared by every worktree of it. `holder_thread_id` is the thread leasing
  // the checkout (null when free); `last_holder_thread_id` lets that thread
  // come back to the same path, and so to the same warm build cache.
  yield* sql`
    CREATE TABLE IF NOT EXISTS worktree_pool_slots (
      slot_path TEXT PRIMARY KEY,
      repo_key TEXT NOT NULL,
      holder_thread_id TEXT,
      last_holder_thread_id TEXT,
      created_at TEXT NOT NULL,
      updated_at TEXT NOT NULL
    )
  `;
  yield* sql`
    CREATE INDEX IF NOT EXISTS idx_worktree_pool_slots_repo
    ON worktree_pool_slots(repo_key)
  `;
  yield* sql`
    CREATE UNIQUE INDEX IF NOT EXISTS idx_worktree_pool_slots_holder
    ON worktree_pool_slots(holder_thread_id)
    WHERE holder_thread_id IS NOT NULL
  `;

  // Pooled threads whose uncommitted changes wait in a parked ref, and which
  // repository holds that ref, so deleting a thread can drop it without a
  // checkout of its own.
  yield* sql`
    CREATE TABLE IF NOT EXISTS worktree_pool_parked (
      thread_id TEXT PRIMARY KEY,
      repo_key TEXT NOT NULL,
      parked_at TEXT NOT NULL
    )
  `;
});
