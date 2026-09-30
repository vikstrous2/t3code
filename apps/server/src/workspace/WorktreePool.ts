/**
 * WorktreePool - per-repository pool of git worktrees for pooled threads.
 *
 * A pooled thread holds a checkout (a "slot") only while something needs a
 * workspace: a provider session, a terminal, or a setup script. When nothing
 * does, its uncommitted changes go into a hidden ref, the checkout is
 * cleaned (ignored files such as build outputs stay, which is the point) and
 * detached, and the slot returns to the pool. The next lease switches a slot
 * to the thread's branch and restores the ref. Slot paths are stable, so
 * tools that key caches on the workspace path (Bazel output bases) stay warm.
 *
 * "Lease" here is a pool slot. It is unrelated to `withWorkspaceLease`, the
 * per-path mutex that serializes checkout work on one directory.
 *
 * @module WorktreePool
 */
import * as NodeCrypto from "node:crypto";

import {
  CommandId,
  type ProjectId,
  type ThreadId,
  type WorktreePool as WorktreePoolSetting,
} from "@t3tools/contracts";
import { resolveProjectSettings } from "@t3tools/shared/projectSettings";
import { makeDrainableWorker } from "@t3tools/shared/DrainableWorker";
import * as Cause from "effect/Cause";
import * as Context from "effect/Context";
import * as Crypto from "effect/Crypto";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";
import type * as Scope from "effect/Scope";
import * as Semaphore from "effect/Semaphore";
import * as Stream from "effect/Stream";

import * as CheckpointStore from "../checkpointing/CheckpointStore.ts";
import { worktreePoolParkedRefForThread } from "../checkpointing/Utils.ts";
import { ServerConfig } from "../config.ts";
import { OrchestrationEngineService } from "../orchestration/Services/OrchestrationEngine.ts";
import { ProjectionProjectRepository } from "../persistence/Services/ProjectionProjects.ts";
import { ProjectionThreadRepository } from "../persistence/Services/ProjectionThreads.ts";
import { ProjectionThreadSessionRepository } from "../persistence/Services/ProjectionThreadSessions.ts";
import {
  WorktreePoolSlotRepository,
  type WorktreePoolSlot,
} from "../persistence/WorktreePoolSlots.ts";
import { T3ProjectFileLoader } from "../project/T3ProjectFileLoader.ts";
import { ProviderSessionDirectory } from "../provider/Services/ProviderSessionDirectory.ts";
import { forkParked } from "../serverActivation.ts";
import { ServerSettingsService } from "../serverSettings.ts";
import * as TerminalManager from "../terminal/Manager.ts";
import type { CreateWorktreeProgress } from "../vcs/GitVcsDriver.ts";
import { GitVcsDriver } from "../vcs/GitVcsDriver.ts";
import { withWorkspaceLease } from "./workspaceLease.ts";

/** Capacity for threads that are pooled while their project's setting is off. */
export const DEFAULT_WORKTREE_POOL_MAX_TREES = 8;

export class WorktreePoolError extends Schema.TaggedError<WorktreePoolError>()(
  "WorktreePoolError",
  {
    threadId: Schema.String,
    detail: Schema.String,
    cause: Schema.optional(Schema.Defect()),
  },
) {
  override get message(): string {
    return this.detail;
  }
}

export interface WorktreePoolLeaseOptions {
  /**
   * Bootstrap of a new pooled thread: create this branch from `startPoint`
   * in the leased checkout (recording `baseBranch` like `createWorktree`)
   * and mark the thread pooled.
   */
  readonly newBranch?: {
    readonly name: string;
    readonly startPoint: string;
    readonly baseBranch: string;
  };
  readonly progress?: CreateWorktreeProgress;
}

export interface WorktreePoolShape {
  /** The project's resolved `worktreePool` setting, t3.json included. */
  readonly settingFor: (projectId: ProjectId) => Effect.Effect<WorktreePoolSetting>;
  /**
   * Runs `use` with the thread's pool checkout. A pooled thread without one
   * leases a slot first (switching it to the thread's branch and restoring
   * parked changes); the slot cannot be released while `use` runs. Unpooled
   * threads get `null` and keep their own cwd. Afterwards the slot is
   * released unless what `use` started (a session, a terminal) holds it.
   */
  /**
   * `withThreadWorkspace` for a caller whose use of the checkout spans more
   * than one effect: the slot stays pinned until the scope closes.
   */
  readonly lease: (
    threadId: ThreadId,
    options?: WorktreePoolLeaseOptions,
  ) => Effect.Effect<string | null, WorktreePoolError, Scope.Scope>;
  readonly withThreadWorkspace: <A, E, R>(
    threadId: ThreadId,
    use: (cwd: string | null) => Effect.Effect<A, E, R>,
    options?: WorktreePoolLeaseOptions,
  ) => Effect.Effect<A, E | WorktreePoolError, R>;
  /** Queues a release of the thread's slot if nothing needs it any more. */
  readonly releaseIfIdle: (threadId: ThreadId) => Effect.Effect<void>;
  /** Subscribes to the release triggers and releases slots orphaned by a restart. */
  readonly start: () => Effect.Effect<void, never, Scope.Scope>;
  /** Resolves once queued releases have finished. For tests. */
  readonly drain: Effect.Effect<void>;
}

const unpooled: WorktreePoolShape = {
  settingFor: () => Effect.succeed("off"),
  lease: () => Effect.succeed(null),
  withThreadWorkspace: (_threadId, use) => use(null),
  releaseIfIdle: () => Effect.void,
  start: () => Effect.void,
  drain: Effect.void,
};

/**
 * Defaults to "no pool": every thread keeps its own cwd. The server provides
 * the real pool; harnesses that do not keep the unpooled behavior.
 */
export class WorktreePool extends Context.Reference<WorktreePoolShape>(
  "t3/workspace/WorktreePool",
  {
    defaultValue: () => unpooled,
  },
) {}

type ReleaseReason = "idle" | "startup";

export const make = Effect.gen(function* () {
  const config = yield* ServerConfig;
  const fileSystem = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const crypto = yield* Crypto.Crypto;
  const git = yield* GitVcsDriver;
  const checkpointStore = yield* CheckpointStore.CheckpointStore;
  const slots = yield* WorktreePoolSlotRepository;
  const engine = yield* OrchestrationEngineService;
  const threads = yield* ProjectionThreadRepository;
  const threadSessions = yield* ProjectionThreadSessionRepository;
  const projects = yield* ProjectionProjectRepository;
  const directory = yield* ProviderSessionDirectory;
  const serverSettings = yield* ServerSettingsService;
  const projectFiles = yield* T3ProjectFileLoader;
  const terminalManager = yield* TerminalManager.TerminalManager;

  const repoLocks = new Map<string, Semaphore.Semaphore>();
  const withRepoLock = <A, E, R>(repoKey: string, effect: Effect.Effect<A, E, R>) =>
    Effect.suspend(() => {
      let lock = repoLocks.get(repoKey);
      if (!lock) {
        lock = Semaphore.makeUnsafe(1);
        repoLocks.set(repoKey, lock);
      }
      return lock.withPermit(effect);
    });
  // Threads inside withThreadWorkspace: their slot is in use even before a
  // session or terminal exists to say so.
  const pins = new Map<ThreadId, number>();
  // Terminals that are starting or running, per thread, from the terminal
  // metadata stream. Exited terminals keep their history but hold nothing.
  const liveTerminals = new Map<string, Set<string>>();

  const fail = (threadId: ThreadId, detail: string, cause?: unknown) =>
    new WorktreePoolError({ threadId, detail, ...(cause !== undefined ? { cause } : {}) });
  const nowIso = DateTime.now.pipe(Effect.map(DateTime.formatIso));

  const getThread = (threadId: ThreadId) =>
    threads.getById({ threadId }).pipe(
      Effect.map((row) => Option.getOrNull(row)),
      Effect.orElseSucceed(() => null),
    );

  const settingFor: WorktreePoolShape["settingFor"] = Effect.fn("WorktreePool.settingFor")(
    function* (projectId) {
      const project = Option.getOrNull(
        yield* projects.getById({ projectId }).pipe(Effect.orElseSucceed(() => Option.none())),
      );
      const settings = yield* serverSettings.getSettings.pipe(Effect.orElseSucceed(() => null));
      if (!project || !settings) return "off";
      const projectFile = Option.getOrNull(yield* projectFiles.load(project.workspaceRoot));
      return resolveProjectSettings(settings, projectId, project, projectFile).settings
        .worktreePool;
    },
  );

  const resolveRepoKey = (threadId: ThreadId, cwd: string) =>
    git
      .execute({
        operation: "WorktreePool.resolveRepoKey",
        cwd,
        args: ["rev-parse", "--path-format=absolute", "--git-common-dir"],
      })
      .pipe(
        Effect.flatMap((result) => fileSystem.realPath(result.stdout.trim())),
        Effect.mapError((cause) =>
          fail(threadId, `Could not find the git repository for ${cwd}.`, cause),
        ),
      );

  // <pool>/<repo>-<hash of its git dir>/<n>. The hash keeps two clones with
  // the same folder name apart.
  const repoPoolDir = (repoKey: string) => {
    const base = path.basename(repoKey);
    const name = base === ".git" ? path.basename(path.dirname(repoKey)) : base;
    const hash = NodeCrypto.createHash("sha256").update(repoKey).digest("hex").slice(0, 8);
    return path.join(config.worktreePoolDir, `${name.replace(/[^\w.-]+/g, "-")}-${hash}`);
  };

  const runGit = (threadId: ThreadId, cwd: string, args: ReadonlyArray<string>) =>
    git
      .execute({ operation: "WorktreePool.git", cwd, args })
      .pipe(
        Effect.mapError((cause) =>
          fail(threadId, `git ${args[0]} failed in ${cwd}: ${cause.message}`, cause),
        ),
      );

  // Detached at its commit with tracked and untracked files reset. No -x:
  // ignored files (build outputs, bazel-* links, node_modules) are the warm
  // cache the pool exists to keep.
  const scrubSlot = (threadId: ThreadId, slotPath: string) =>
    runGit(threadId, slotPath, ["reset", "--hard", "--quiet"]).pipe(
      Effect.andThen(runGit(threadId, slotPath, ["clean", "-fd", "--quiet"])),
      Effect.andThen(runGit(threadId, slotPath, ["switch", "--detach", "--quiet"])),
    );

  const setThreadPoolState = (input: {
    readonly threadId: ThreadId;
    readonly state: "leased" | "parked";
    readonly worktreePath: string | null;
    readonly branch?: string;
  }) =>
    Effect.gen(function* () {
      const commandId = CommandId.make(`server:worktree-pool:${yield* crypto.randomUUIDv4}`);
      yield* engine.dispatch({
        type: "thread.worktree-pool.set",
        commandId,
        threadId: input.threadId,
        worktreePool: { state: input.state },
        worktreePath: input.worktreePath,
        ...(input.branch !== undefined ? { branch: input.branch } : {}),
      });
    }).pipe(
      Effect.mapError((cause) =>
        fail(input.threadId, "Could not record the thread's worktree pool state.", cause),
      ),
    );

  const submodulesFor = (projectId: ProjectId) =>
    serverSettings.getSettings.pipe(
      Effect.map(
        (settings) => resolveProjectSettings(settings, projectId).settings.worktreeSubmodules,
      ),
      Effect.orElseSucceed(() => null),
    );

  const acquire = Effect.fn("WorktreePool.acquire")(function* (
    threadId: ThreadId,
    options: WorktreePoolLeaseOptions | undefined,
  ) {
    const thread = yield* getThread(threadId);
    if (!thread || thread.deletedAt !== null) {
      return yield* fail(threadId, `Thread '${threadId}' was not found.`);
    }
    const project = Option.getOrNull(
      yield* projects
        .getById({ projectId: thread.projectId })
        .pipe(Effect.orElseSucceed(() => Option.none())),
    );
    if (!project) {
      return yield* fail(threadId, `Project '${thread.projectId}' was not found.`);
    }
    const projectRoot = project.workspaceRoot;
    const repoKey = yield* resolveRepoKey(threadId, projectRoot);

    return yield* withRepoLock(
      repoKey,
      Effect.gen(function* () {
        const held = Option.getOrNull(
          yield* slots
            .getByHolder(threadId)
            .pipe(
              Effect.mapError((cause) =>
                fail(threadId, "Could not read the worktree pool.", cause),
              ),
            ),
        );
        if (held) {
          if (yield* fileSystem.exists(held.slotPath).pipe(Effect.orElseSucceed(() => false))) {
            if (thread.worktreePoolState !== "leased" || thread.worktreePath !== held.slotPath) {
              yield* setThreadPoolState({ threadId, state: "leased", worktreePath: held.slotPath });
            }
            return held.slotPath;
          }
          // The checkout was deleted behind the pool's back.
          yield* slots.remove(held.slotPath).pipe(Effect.ignore);
          yield* git.pruneWorktrees({ cwd: projectRoot }).pipe(Effect.ignore);
        }

        const branch = options?.newBranch?.name ?? thread.branch;
        if (branch === null) {
          return yield* fail(threadId, "This thread has no branch to check out.");
        }
        const refName = options?.newBranch?.startPoint ?? branch;
        const worktreeInput = {
          cwd: projectRoot,
          refName,
          ...(options?.newBranch
            ? { newRefName: options.newBranch.name, baseRefName: options.newBranch.baseBranch }
            : {}),
        };
        const worktreeOptions = {
          submodules: yield* submodulesFor(thread.projectId),
          ...(options?.progress ? { progress: options.progress } : {}),
        };
        const all = yield* slots
          .listByRepo(repoKey)
          .pipe(
            Effect.mapError((cause) => fail(threadId, "Could not read the worktree pool.", cause)),
          );
        const free = all.filter((slot) => slot.holderThreadId === null);
        // The thread's previous checkout first: its build cache is this branch's.
        const candidates = [
          ...free.filter((slot) => slot.lastHolderThreadId === threadId),
          ...free.filter((slot) => slot.lastHolderThreadId !== threadId),
        ];

        let slotPath: string | null = null;
        let created = false;
        for (const candidate of candidates) {
          if (
            !(yield* fileSystem.exists(candidate.slotPath).pipe(Effect.orElseSucceed(() => false)))
          ) {
            yield* slots.remove(candidate.slotPath).pipe(Effect.ignore);
            continue;
          }
          slotPath = candidate.slotPath;
          break;
        }
        if (slotPath === null) {
          const setting = yield* settingFor(thread.projectId);
          const maxTrees = setting === "off" ? DEFAULT_WORKTREE_POOL_MAX_TREES : setting.maxTrees;
          const existing = yield* slots
            .listByRepo(repoKey)
            .pipe(
              Effect.mapError((cause) =>
                fail(threadId, "Could not read the worktree pool.", cause),
              ),
            );
          if (existing.length >= maxTrees) {
            return yield* fail(
              threadId,
              `The worktree pool for ${path.basename(projectRoot)} is full (${existing.length}/${maxTrees} in use). Stop a session or close a terminal in another thread, or raise worktreePool.maxTrees.`,
            );
          }
          const dir = repoPoolDir(repoKey);
          const taken = new Set(existing.map((slot) => slot.slotPath));
          let index = 1;
          while (
            taken.has(path.join(dir, String(index))) ||
            (yield* fileSystem
              .exists(path.join(dir, String(index)))
              .pipe(Effect.orElseSucceed(() => false)))
          ) {
            index++;
          }
          slotPath = path.join(dir, String(index));
          yield* fileSystem
            .makeDirectory(dir, { recursive: true })
            .pipe(Effect.mapError((cause) => fail(threadId, `Could not create ${dir}.`, cause)));
          const newSlotPath = slotPath;
          yield* git
            .createWorktree({ ...worktreeInput, path: newSlotPath }, worktreeOptions)
            .pipe(
              Effect.mapError((cause) =>
                fail(threadId, `Could not create a pool worktree: ${cause.message}`, cause),
              ),
            );
          created = true;
        }

        const leasedPath = slotPath;
        const now = yield* nowIso;
        const createdAt = all.find((slot) => slot.slotPath === leasedPath)?.createdAt ?? now;
        const parked = yield* slots
          .getParked(threadId)
          .pipe(Effect.orElseSucceed(() => Option.none()));
        yield* Effect.gen(function* () {
          if (!created) {
            yield* git
              .switchWorktree({ ...worktreeInput, path: leasedPath }, worktreeOptions)
              .pipe(
                Effect.mapError((cause) =>
                  fail(
                    threadId,
                    `Could not check out ${branch} in ${leasedPath}: ${cause.message}`,
                    cause,
                  ),
                ),
              );
          }
          yield* slots
            .upsert({
              slotPath: leasedPath,
              repoKey,
              holderThreadId: threadId,
              lastHolderThreadId: threadId,
              createdAt,
              updatedAt: now,
            })
            .pipe(
              Effect.mapError((cause) => fail(threadId, "Could not record the pool lease.", cause)),
            );
          if (Option.isSome(parked)) {
            const checkpointRef = worktreePoolParkedRefForThread(threadId);
            yield* checkpointStore
              .restoreCheckpoint({ cwd: leasedPath, checkpointRef })
              .pipe(
                Effect.mapError((cause) =>
                  fail(
                    threadId,
                    `Could not restore the thread's parked changes: ${cause.message}`,
                    cause,
                  ),
                ),
              );
            yield* checkpointStore
              .deleteCheckpointRefs({ cwd: leasedPath, checkpointRefs: [checkpointRef] })
              .pipe(Effect.ignore);
            yield* slots.clearParked(threadId).pipe(Effect.ignore);
          }
        }).pipe(
          // Hand a half-prepared checkout back clean. The parked ref stays for
          // the next attempt.
          Effect.onError(() =>
            scrubSlot(threadId, leasedPath).pipe(
              Effect.andThen(
                slots.upsert({
                  slotPath: leasedPath,
                  repoKey,
                  holderThreadId: null,
                  lastHolderThreadId: threadId,
                  createdAt,
                  updatedAt: now,
                }),
              ),
              Effect.ignore,
            ),
          ),
        );

        yield* setThreadPoolState({
          threadId,
          state: "leased",
          worktreePath: slotPath,
          ...(options?.newBranch ? { branch: options.newBranch.name } : {}),
        });
        yield* Effect.logInfo("worktree pool leased a checkout", {
          threadId,
          slotPath,
          branch,
          created,
          restoredParkedChanges: Option.isSome(parked),
        });
        return slotPath;
      }),
    );
  });

  const isHeldElsewhere = Effect.fn("WorktreePool.isInUse")(function* (
    threadId: ThreadId,
    reason: ReleaseReason,
    deleted: boolean,
  ) {
    if ((pins.get(threadId) ?? 0) > 0) return true;
    if ((liveTerminals.get(threadId)?.size ?? 0) > 0) return true;
    // No provider process survives a server restart.
    if (reason === "startup") return false;
    const session = Option.getOrNull(
      yield* threadSessions
        .getByThreadId({ threadId })
        .pipe(Effect.orElseSucceed(() => Option.none())),
    );
    // A turn is on its way or running, possibly before any binding exists.
    if (session?.status === "starting" || session?.status === "running") return true;
    const binding = Option.getOrNull(
      yield* directory.getBinding(threadId).pipe(Effect.orElseSucceed(() => Option.none())),
    );
    const bindingLive = binding !== null && binding.status !== "stopped";
    // Either record saying "stopped" is enough: ingestion marks the projection
    // when a process exits, stopSession marks the binding.
    return bindingLive && (deleted || (session !== null && session.status !== "stopped"));
  });

  const dropParkedRef = Effect.fn("WorktreePool.dropParkedRef")(function* (threadId: ThreadId) {
    const parked = yield* slots.getParked(threadId).pipe(Effect.orElseSucceed(() => Option.none()));
    if (Option.isNone(parked)) return;
    // Refs live in the common git dir, which git accepts as a working directory.
    yield* git
      .execute({
        operation: "WorktreePool.dropParkedRef",
        cwd: parked.value.repoKey,
        args: ["update-ref", "-d", worktreePoolParkedRefForThread(threadId)],
        allowNonZeroExit: true,
      })
      .pipe(Effect.ignore);
    yield* slots.clearParked(threadId).pipe(Effect.ignore);
  });

  const release = Effect.fn("WorktreePool.release")(function* (
    threadId: ThreadId,
    reason: ReleaseReason,
  ) {
    const heldBefore = Option.getOrNull(yield* slots.getByHolder(threadId));
    const thread = yield* getThread(threadId);
    const deleted = thread === null || thread.deletedAt !== null;
    if (!heldBefore) {
      if (deleted) yield* dropParkedRef(threadId);
      // Repair a projection that still claims a slot the table does not give it.
      else if (thread.worktreePoolState === "leased" && (pins.get(threadId) ?? 0) === 0) {
        yield* setThreadPoolState({ threadId, state: "parked", worktreePath: null });
      }
      return;
    }
    yield* withRepoLock(
      heldBefore.repoKey,
      Effect.gen(function* () {
        // Re-read under the lock: a lease or release may have run meanwhile.
        const held: WorktreePoolSlot | null = Option.getOrNull(yield* slots.getByHolder(threadId));
        if (!held || (yield* isHeldElsewhere(threadId, reason, deleted))) return;
        const slotPath = held.slotPath;
        yield* withWorkspaceLease(
          path.resolve(slotPath),
          Effect.gen(function* () {
            if (!(yield* fileSystem.exists(slotPath).pipe(Effect.orElseSucceed(() => false)))) {
              yield* slots.remove(slotPath);
              return;
            }
            if (!deleted) {
              const status = yield* runGit(threadId, slotPath, [
                "status",
                "--porcelain",
                "--untracked-files=normal",
              ]);
              const head = yield* git.execute({
                operation: "WorktreePool.headBranch",
                cwd: slotPath,
                args: ["symbolic-ref", "--quiet", "--short", "HEAD"],
                allowNonZeroExit: true,
              });
              const headBranch = head.exitCode === 0 ? head.stdout.trim() : null;
              // A checkout left on another branch (or detached) may hold work
              // only this tree has. Keep it even when the tree matches HEAD.
              if (status.stdout.trim().length > 0 || headBranch !== thread.branch) {
                yield* checkpointStore.captureCheckpoint({
                  cwd: slotPath,
                  checkpointRef: worktreePoolParkedRefForThread(threadId),
                });
                yield* slots.setParked({
                  threadId,
                  repoKey: held.repoKey,
                  parkedAt: yield* nowIso,
                });
              }
            }
            yield* scrubSlot(threadId, slotPath);
            yield* slots.upsert({
              ...held,
              holderThreadId: null,
              lastHolderThreadId: threadId,
              updatedAt: yield* nowIso,
            });
          }),
        );
        if (deleted) {
          yield* dropParkedRef(threadId);
        } else {
          yield* setThreadPoolState({ threadId, state: "parked", worktreePath: null });
        }
        yield* Effect.logInfo("worktree pool released a checkout", { threadId, slotPath, reason });
      }),
    );
  });

  const releaseWorker = yield* makeDrainableWorker(
    (item: { readonly threadId: ThreadId; readonly reason: ReleaseReason }) =>
      release(item.threadId, item.reason).pipe(
        Effect.catchCause((cause) =>
          Cause.hasInterruptsOnly(cause)
            ? Effect.interrupt
            : Effect.logWarning("worktree pool failed to release a checkout", {
                threadId: item.threadId,
                cause: Cause.pretty(cause),
              }),
        ),
      ),
  );
  const releaseIfIdle: WorktreePoolShape["releaseIfIdle"] = (threadId) =>
    releaseWorker.enqueue({ threadId, reason: "idle" });

  const isPooled = (threadId: ThreadId, options: WorktreePoolLeaseOptions | undefined) =>
    getThread(threadId).pipe(
      Effect.map(
        (thread) =>
          thread !== null && (thread.worktreePoolState != null || options?.newBranch !== undefined),
      ),
    );
  const pin = (threadId: ThreadId) =>
    Effect.sync(() => void pins.set(threadId, (pins.get(threadId) ?? 0) + 1));
  const unpin = (threadId: ThreadId) =>
    Effect.sync(() => {
      const remaining = (pins.get(threadId) ?? 1) - 1;
      if (remaining > 0) pins.set(threadId, remaining);
      else pins.delete(threadId);
    }).pipe(Effect.andThen(releaseIfIdle(threadId)));

  const lease: WorktreePoolShape["lease"] = (threadId, options) =>
    Effect.gen(function* () {
      if (!(yield* isPooled(threadId, options))) return null;
      yield* Effect.acquireRelease(pin(threadId), () => unpin(threadId));
      return yield* acquire(threadId, options);
    });

  // Not built on `lease`: a scope here would also be the scope of anything
  // `use` forks, and turn starts fork the provider send.
  const withThreadWorkspace: WorktreePoolShape["withThreadWorkspace"] = (threadId, use, options) =>
    Effect.gen(function* () {
      if (!(yield* isPooled(threadId, options))) return yield* use(null);
      return yield* Effect.acquireUseRelease(
        pin(threadId),
        () => acquire(threadId, options).pipe(Effect.flatMap(use)),
        () => unpin(threadId),
      );
    });

  const trackTerminal = (terminal: {
    readonly threadId: string;
    readonly terminalId: string;
    readonly status: string;
  }) => {
    const live = terminal.status === "starting" || terminal.status === "running";
    const set = liveTerminals.get(terminal.threadId) ?? new Set<string>();
    const wasLive = set.has(terminal.terminalId);
    if (live) set.add(terminal.terminalId);
    else set.delete(terminal.terminalId);
    if (set.size > 0) liveTerminals.set(terminal.threadId, set);
    else liveTerminals.delete(terminal.threadId);
    return wasLive && !live;
  };

  const start: WorktreePoolShape["start"] = Effect.fn("WorktreePool.start")(function* () {
    const unsubscribe = yield* terminalManager.subscribeMetadata((event) => {
      switch (event.type) {
        case "snapshot":
          liveTerminals.clear();
          for (const terminal of event.terminals) trackTerminal(terminal);
          return Effect.void;
        case "upsert":
          return trackTerminal(event.terminal)
            ? releaseIfIdle(event.terminal.threadId as ThreadId)
            : Effect.void;
        case "remove":
          return trackTerminal({ ...event, status: "exited" })
            ? releaseIfIdle(event.threadId as ThreadId)
            : Effect.void;
      }
    });
    yield* Effect.addFinalizer(() => Effect.sync(unsubscribe));

    const domainEvents = yield* engine.subscribeDomainEvents;
    yield* forkParked(
      Stream.runForEach(domainEvents, (event) => {
        switch (event.type) {
          case "thread.session-set":
            return event.payload.session.status === "stopped"
              ? releaseIfIdle(event.payload.threadId)
              : Effect.void;
          case "thread.archived":
          case "thread.deleted":
            return releaseIfIdle(event.payload.threadId);
          default:
            return Effect.void;
        }
      }),
    );

    // Slots held when the server stopped have no process left in them.
    yield* forkParked(
      slots.listHeld().pipe(
        Effect.flatMap((held) =>
          Effect.forEach(
            held,
            (slot) =>
              slot.holderThreadId === null
                ? Effect.void
                : releaseWorker.enqueue({ threadId: slot.holderThreadId, reason: "startup" }),
            { discard: true },
          ),
        ),
        Effect.catchCause((cause) =>
          Effect.logWarning("worktree pool failed to reconcile held checkouts", {
            cause: Cause.pretty(cause),
          }),
        ),
      ),
    );
  });

  return {
    settingFor,
    lease,
    withThreadWorkspace,
    releaseIfIdle,
    start,
    drain: releaseWorker.drain,
  } satisfies WorktreePoolShape;
});

export const layer = Layer.effect(WorktreePool, make);
