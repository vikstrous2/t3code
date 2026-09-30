// @effect-diagnostics nodeBuiltinImport:off
import * as NodeChildProcess from "node:child_process";
import * as NodeFS from "node:fs";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";

import {
  CommandId,
  DEFAULT_PROVIDER_INTERACTION_MODE,
  ProjectId,
  ProviderInstanceId,
  ThreadId,
} from "@t3tools/contracts";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { afterEach, assert, describe, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";

import * as CheckpointStore from "../checkpointing/CheckpointStore.ts";
import { worktreePoolParkedRefForThread } from "../checkpointing/Utils.ts";
import { ServerConfig } from "../config.ts";
import { OrchestrationLayerLive } from "../orchestration/runtimeLayer.ts";
import { OrchestrationEngineService } from "../orchestration/Services/OrchestrationEngine.ts";
import { ProjectionSnapshotQuery } from "../orchestration/Services/ProjectionSnapshotQuery.ts";
import { SqlitePersistenceMemory } from "../persistence/Layers/Sqlite.ts";
import * as WorktreePoolSlots from "../persistence/WorktreePoolSlots.ts";
import * as RepositoryIdentityResolver from "../project/RepositoryIdentityResolver.ts";
import * as T3ProjectFileLoader from "../project/T3ProjectFileLoader.ts";
import { ProviderSessionDirectory } from "../provider/Services/ProviderSessionDirectory.ts";
import { ServerSettingsService } from "../serverSettings.ts";
import { TerminalManager } from "../terminal/Manager.ts";
import * as GitVcsDriver from "../vcs/GitVcsDriver.ts";
import * as VcsDriverRegistry from "../vcs/VcsDriverRegistry.ts";
import * as VcsProcess from "../vcs/VcsProcess.ts";
import * as WorktreePool from "./WorktreePool.ts";

const tempDirs: string[] = [];
afterEach(() => {
  for (const dir of tempDirs.splice(0)) NodeFS.rmSync(dir, { recursive: true, force: true });
});

const git = (cwd: string, args: ReadonlyArray<string>) =>
  NodeChildProcess.execFileSync("git", args, { cwd, encoding: "utf8" }).trim();

function createRepository(): string {
  const cwd = NodeFS.realpathSync(NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "t3-pool-")));
  tempDirs.push(cwd);
  git(cwd, ["init", "--initial-branch=main"]);
  git(cwd, ["config", "user.email", "test@example.com"]);
  git(cwd, ["config", "user.name", "Test"]);
  NodeFS.writeFileSync(NodePath.join(cwd, "README.md"), "hello\n");
  NodeFS.writeFileSync(NodePath.join(cwd, ".gitignore"), "cache/\n");
  git(cwd, ["add", "."]);
  git(cwd, ["commit", "-m", "initial"]);
  return cwd;
}

const PROJECT_ID = ProjectId.make("project-1");
const MODEL = { instanceId: ProviderInstanceId.make("codex"), model: "gpt-5-codex" };

const makeLayer = (repo: string, maxTrees: number) =>
  WorktreePool.layer.pipe(
    Layer.provideMerge(WorktreePoolSlots.layer),
    Layer.provideMerge(CheckpointStore.layer.pipe(Layer.provide(VcsDriverRegistry.layer))),
    Layer.provideMerge(GitVcsDriver.layer),
    Layer.provideMerge(T3ProjectFileLoader.layer),
    Layer.provideMerge(ServerSettingsService.layerTest({ worktreePool: { maxTrees } })),
    // No provider sessions: nothing but a pin or a terminal holds a slot.
    Layer.provideMerge(
      Layer.mock(ProviderSessionDirectory)({ getBinding: () => Effect.succeed(Option.none()) }),
    ),
    Layer.provideMerge(
      Layer.mock(TerminalManager)({ subscribeMetadata: () => Effect.succeed(() => undefined) }),
    ),
    Layer.provideMerge(OrchestrationLayerLive),
    Layer.provideMerge(RepositoryIdentityResolver.layer),
    Layer.provideMerge(SqlitePersistenceMemory),
    Layer.provideMerge(VcsProcess.layer),
    Layer.provideMerge(ServerConfig.layerTest(repo, { prefix: "t3-pool-base-" })),
    Layer.provideMerge(NodeServices.layer),
  );

const createThread = (threadId: ThreadId) =>
  Effect.gen(function* () {
    const engine = yield* OrchestrationEngineService;
    yield* engine.dispatch({
      type: "thread.create",
      commandId: CommandId.make(`create-${threadId}`),
      threadId,
      projectId: PROJECT_ID,
      title: "Thread",
      modelSelection: MODEL,
      runtimeMode: "full-access",
      interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
      branch: null,
      worktreePath: null,
      createdAt: "2026-01-01T00:00:00.000Z",
    });
  });

const setup = (repo: string) =>
  Effect.gen(function* () {
    const engine = yield* OrchestrationEngineService;
    yield* engine.dispatch({
      type: "project.create",
      commandId: CommandId.make("create-project"),
      projectId: PROJECT_ID,
      title: "Project",
      workspaceRoot: repo,
      defaultModelSelection: MODEL,
      createdAt: "2026-01-01T00:00:00.000Z",
    });
  });

const newBranch = (name: string) => ({
  newBranch: { name, startPoint: "main", baseBranch: "main" },
});

const threadShell = (threadId: ThreadId) =>
  ProjectionSnapshotQuery.use((query) => query.getThreadShellById(threadId)).pipe(
    Effect.map(Option.getOrThrow),
  );

describe("WorktreePool", () => {
  it.effect(
    "parks a thread's changes, keeps the warm cache, and restores them in another slot",
    () => {
      const repo = createRepository();
      return Effect.gen(function* () {
        const pool = yield* WorktreePool.WorktreePool;
        const config = yield* ServerConfig;
        yield* setup(repo);
        const first = ThreadId.make("thread-first");
        const second = ThreadId.make("thread-second");
        yield* createThread(first);
        yield* createThread(second);

        const firstSlot = yield* pool.withThreadWorkspace(
          first,
          (cwd) =>
            Effect.sync(() => {
              assert.isNotNull(cwd);
              NodeFS.writeFileSync(NodePath.join(cwd!, "README.md"), "changed\n");
              NodeFS.writeFileSync(NodePath.join(cwd!, "new.txt"), "untracked\n");
              NodeFS.mkdirSync(NodePath.join(cwd!, "cache"));
              NodeFS.writeFileSync(NodePath.join(cwd!, "cache", "out"), "warm\n");
              return cwd!;
            }),
          newBranch("t3code/first"),
        );
        assert.isTrue(firstSlot.startsWith(config.worktreePoolDir));
        // Nothing started in the checkout, so it goes straight back to the pool.
        yield* pool.drain;

        assert.deepEqual((yield* threadShell(first)).worktreePool, { state: "parked" });
        assert.isNull((yield* threadShell(first)).worktreePath);
        assert.equal((yield* threadShell(first)).branch, "t3code/first");
        assert.equal(git(firstSlot, ["status", "--porcelain"]), "");
        assert.equal(git(firstSlot, ["rev-parse", "--abbrev-ref", "HEAD"]), "HEAD");
        assert.equal(NodeFS.readFileSync(NodePath.join(firstSlot, "README.md"), "utf8"), "hello\n");
        assert.equal(
          NodeFS.readFileSync(NodePath.join(firstSlot, "cache", "out"), "utf8"),
          "warm\n",
        );
        assert.isNotEmpty(
          git(repo, ["rev-parse", "--verify", worktreePoolParkedRefForThread(first)]),
        );

        // The second thread takes the free checkout, ignored cache and all, while
        // the first comes back into a new one with its work restored.
        const restored = yield* Effect.scoped(
          Effect.gen(function* () {
            const secondSlot = yield* pool.lease(second, newBranch("t3code/second"));
            assert.equal(secondSlot, firstSlot);
            assert.equal(git(firstSlot, ["branch", "--show-current"]), "t3code/second");
            assert.isTrue(NodeFS.existsSync(NodePath.join(firstSlot, "cache", "out")));
            assert.isFalse(NodeFS.existsSync(NodePath.join(firstSlot, "new.txt")));

            const restored = (yield* pool.lease(first))!;
            assert.notEqual(restored, firstSlot);
            assert.equal(git(restored, ["branch", "--show-current"]), "t3code/first");
            assert.equal(
              NodeFS.readFileSync(NodePath.join(restored, "README.md"), "utf8"),
              "changed\n",
            );
            assert.equal(
              NodeFS.readFileSync(NodePath.join(restored, "new.txt"), "utf8"),
              "untracked\n",
            );
            assert.equal(
              git(repo, ["for-each-ref", "--format=%(refname)", "refs/t3/pool-parked/"]),
              "",
            );
            return restored;
          }),
        );
        yield* pool.drain;

        // Both are free now; the first thread's last checkout is the one it gets.
        const again = yield* pool.withThreadWorkspace(first, (cwd) => Effect.succeed(cwd!));
        assert.equal(again, restored);
        yield* pool.drain;
      }).pipe(Effect.provide(makeLayer(repo, 2)));
    },
  );

  it.effect("refuses a lease when every checkout is held", () => {
    const repo = createRepository();
    return Effect.gen(function* () {
      const pool = yield* WorktreePool.WorktreePool;
      yield* setup(repo);
      const first = ThreadId.make("thread-first");
      const second = ThreadId.make("thread-second");
      yield* createThread(first);
      yield* createThread(second);
      yield* Effect.scoped(
        Effect.gen(function* () {
          yield* pool.lease(first, newBranch("t3code/first"));
          const error = yield* Effect.flip(pool.lease(second, newBranch("t3code/second")));
          assert.include(error.message, "is full (1/1 in use)");
        }),
      );
    }).pipe(Effect.provide(makeLayer(repo, 1)));
  });

  it.effect("drops a deleted thread's parked changes", () => {
    const repo = createRepository();
    return Effect.gen(function* () {
      const pool = yield* WorktreePool.WorktreePool;
      const engine = yield* OrchestrationEngineService;
      yield* setup(repo);
      const thread = ThreadId.make("thread-deleted");
      yield* createThread(thread);
      yield* pool.withThreadWorkspace(
        thread,
        (cwd) => Effect.sync(() => NodeFS.writeFileSync(NodePath.join(cwd!, "work.txt"), "wip\n")),
        newBranch("t3code/deleted"),
      );
      yield* pool.drain;
      const ref = worktreePoolParkedRefForThread(thread);
      assert.isNotEmpty(git(repo, ["rev-parse", "--verify", ref]));

      yield* engine.dispatch({
        type: "thread.delete",
        commandId: CommandId.make("delete-thread"),
        threadId: thread,
      });
      yield* pool.releaseIfIdle(thread);
      yield* pool.drain;
      assert.equal(git(repo, ["for-each-ref", "--format=%(refname)", ref]), "");
    }).pipe(Effect.provide(makeLayer(repo, 2)));
  });

  it.effect("leaves unpooled threads in their own cwd", () => {
    const repo = createRepository();
    return Effect.gen(function* () {
      const pool = yield* WorktreePool.WorktreePool;
      yield* setup(repo);
      const thread = ThreadId.make("thread-local");
      yield* createThread(thread);
      assert.isNull(yield* pool.withThreadWorkspace(thread, (cwd) => Effect.succeed(cwd)));
      assert.isNull((yield* threadShell(thread)).worktreePool ?? null);
    }).pipe(Effect.provide(makeLayer(repo, 2)));
  });
});
