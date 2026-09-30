import { isPooledThread } from "@t3tools/shared/threadWorkspace";

import type { ThreadShell } from "./types";

function normalizeWorktreePath(path: string | null): string | null {
  const trimmed = path?.trim();
  if (!trimmed) {
    return null;
  }
  return trimmed;
}

/**
 * The worktree only this thread uses, which deleting the thread may remove.
 * Never a worktree-pool checkout: the pool owns those and reclaims the slot
 * itself when the thread is deleted.
 */
export function getOrphanedWorktreePathForThread(
  threads: ReadonlyArray<Pick<ThreadShell, "id" | "worktreePath" | "worktreePool">>,
  threadId: ThreadShell["id"],
): string | null {
  const targetThread = threads.find((thread) => thread.id === threadId);
  if (!targetThread || isPooledThread(targetThread)) {
    return null;
  }

  const targetWorktreePath = normalizeWorktreePath(targetThread.worktreePath);
  if (!targetWorktreePath) {
    return null;
  }

  const isShared = threads.some((thread) => {
    if (thread.id === threadId) {
      return false;
    }
    return normalizeWorktreePath(thread.worktreePath) === targetWorktreePath;
  });

  return isShared ? null : targetWorktreePath;
}

export function formatWorktreePathForDisplay(worktreePath: string): string {
  const trimmed = worktreePath.trim();
  if (!trimmed) {
    return worktreePath;
  }

  const normalized = trimmed.replace(/\\/g, "/").replace(/\/+$/, "");
  const parts = normalized.split("/");
  const lastPart = parts[parts.length - 1]?.trim() ?? "";
  return lastPart.length > 0 ? lastPart : trimmed;
}
