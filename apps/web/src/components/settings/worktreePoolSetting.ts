import type { WorktreePool } from "@t3tools/contracts";

export const WORKTREE_POOL_MIN_TREES = 1;
export const WORKTREE_POOL_MAX_TREES = 64;
/** The cap a pool starts with when it is switched on. */
export const WORKTREE_POOL_DEFAULT_TREES = 8;

/**
 * The cap typed into the "Max checkouts" field, or null when the text is not
 * an integer in range. Number(), not parseInt, so "3.5" is rejected rather
 * than committed as 3 while the field still shows 3.5.
 */
export function parseWorktreePoolMaxTrees(text: string): number | null {
  if (text.trim() === "") return null;
  const parsed = Number(text);
  return Number.isInteger(parsed) &&
    parsed >= WORKTREE_POOL_MIN_TREES &&
    parsed <= WORKTREE_POOL_MAX_TREES
    ? parsed
    : null;
}

export function formatWorktreePool(value: WorktreePool): string {
  if (value === "off") return "Off";
  return `On · ${value.maxTrees} ${value.maxTrees === 1 ? "checkout" : "checkouts"}`;
}
