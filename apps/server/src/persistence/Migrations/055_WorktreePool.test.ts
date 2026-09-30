import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";

import { runMigrations } from "../Migrations.ts";
import migrateWorktreePool from "./055_WorktreePool.ts";

it.layer(NodeSqliteClient.layer({ filename: ":memory:" }))("055_WorktreePool", (it) => {
  it.effect("leaves existing threads unpooled and adds the slot tables", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* runMigrations({ toMigrationInclusive: 54 });
      const now = "2026-01-01T00:00:00.000Z";
      yield* sql`
        INSERT INTO projection_threads (
          thread_id, project_id, title, model_selection_json, runtime_mode,
          created_at, updated_at
        ) VALUES (
          'thread-1', 'project-1', 'Existing thread',
          '{"instanceId":"codex","model":"gpt-5.4"}', 'full-access', ${now}, ${now}
        )
      `;
      yield* runMigrations({ toMigrationInclusive: 55 });
      const migrated = yield* sql<{ readonly state: string | null }>`
        SELECT worktree_pool_state AS "state" FROM projection_threads WHERE thread_id = 'thread-1'
      `;
      assert.deepEqual(migrated, [{ state: null }]);

      yield* sql`
        INSERT INTO worktree_pool_slots (slot_path, repo_key, holder_thread_id, created_at, updated_at)
        VALUES ('/pool/repo/1', '/repo/.git', 'thread-1', ${now}, ${now})
      `;
      // One slot per holder.
      const duplicate = yield* sql`
        INSERT INTO worktree_pool_slots (slot_path, repo_key, holder_thread_id, created_at, updated_at)
        VALUES ('/pool/repo/2', '/repo/.git', 'thread-1', ${now}, ${now})
      `.pipe(Effect.flip);
      assert.isDefined(duplicate);

      // Re-running against a migrated database keeps its rows.
      yield* migrateWorktreePool;
      const slots = yield* sql<{ readonly slotPath: string }>`
        SELECT slot_path AS "slotPath" FROM worktree_pool_slots
      `;
      assert.deepEqual(slots, [{ slotPath: "/pool/repo/1" }]);
    }),
  );
});
