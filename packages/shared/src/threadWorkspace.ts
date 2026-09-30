import type { ThreadWorktreePool } from "@t3tools/contracts";

interface ThreadWorkspaceInput {
  readonly worktreePath: string | null;
  readonly worktreePool?: ThreadWorktreePool | null | undefined;
}

/**
 * Where a thread's files live. "local" runs in the project root, "worktree"
 * in its own worktree, "pooled-leased" in the pool checkout it holds right
 * now. "pooled-parked" holds no checkout: its changes wait in git refs and
 * it has no files on disk until a session or terminal leases one again, so
 * readers must not fall back to the project root for it.
 */
export type ThreadWorkspaceState =
  | { readonly kind: "local"; readonly path: null }
  | { readonly kind: "worktree"; readonly path: string }
  | { readonly kind: "pooled-leased"; readonly path: string }
  | { readonly kind: "pooled-parked"; readonly path: null };

export function threadWorkspaceState(thread: ThreadWorkspaceInput): ThreadWorkspaceState {
  if (thread.worktreePool) {
    // A leased thread without a path is mid-transition; treat it as parked
    // so nothing reads the project root in the meantime.
    return thread.worktreePool.state === "leased" && thread.worktreePath
      ? { kind: "pooled-leased", path: thread.worktreePath }
      : { kind: "pooled-parked", path: null };
  }
  return thread.worktreePath
    ? { kind: "worktree", path: thread.worktreePath }
    : { kind: "local", path: null };
}

/** True for threads whose worktree comes from the project's worktree pool. */
export function isPooledThread(thread: Pick<ThreadWorkspaceInput, "worktreePool">): boolean {
  return thread.worktreePool != null;
}

/** True when a pooled thread holds no checkout and so has no files on disk. */
export function isThreadWorkspaceParked(thread: ThreadWorkspaceInput): boolean {
  return threadWorkspaceState(thread).kind === "pooled-parked";
}

/**
 * The directory a thread's files live in: its worktree, or the project root
 * for a local thread. Null for a parked pooled thread, which has none.
 */
export function threadWorkspaceCwd<Root extends string | null | undefined>(
  thread: ThreadWorkspaceInput,
  projectRoot: Root,
): string | Root | null {
  const state = threadWorkspaceState(thread);
  if (state.kind === "pooled-parked") return null;
  return state.path ?? projectRoot;
}

/**
 * Where a "new thread on this branch" follow-up starts: in the same worktree,
 * or on the local checkout for a local thread. A pool checkout belongs to the
 * pool, so a pooled thread's branch seeds a new worktree instead.
 */
export function branchCarryOverWorkspace(thread: ThreadWorkspaceInput): {
  readonly worktreePath: string | null;
  readonly envMode: "local" | "worktree";
} {
  const state = threadWorkspaceState(thread);
  switch (state.kind) {
    case "worktree":
      return { worktreePath: state.path, envMode: "worktree" };
    case "local":
      return { worktreePath: null, envMode: "local" };
    default:
      return { worktreePath: null, envMode: "worktree" };
  }
}

/** Why a thread has no workspace path to copy or open. */
export function missingWorkspacePathReason(thread: ThreadWorkspaceInput): string {
  return isThreadWorkspaceParked(thread)
    ? PARKED_WORKTREE_DESCRIPTION
    : "This thread does not have a workspace path to copy.";
}

export const PARKED_WORKTREE_LABEL = "Worktree · parked";
export const PARKED_WORKTREE_DESCRIPTION =
  "Checkout returned to the pool; it is leased again when the agent or a terminal starts.";
export const PARKED_WORKTREE_CHANGES_MESSAGE =
  "This thread's checkout is parked in the worktree pool, so there is no working tree to show. Turn diffs still load. Send a message or open a terminal to lease a checkout again.";
