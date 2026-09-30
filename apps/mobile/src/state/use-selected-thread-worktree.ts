import { threadWorkspaceState } from "@t3tools/shared/threadWorkspace";
import { useMemo } from "react";

import { useSelectedThreadDetail } from "./use-thread-detail";
import { useThreadSelection } from "./use-thread-selection";
import { resolvePreferredThreadWorktreePath } from "../features/terminal/terminalLaunchContext";

export function useSelectedThreadWorktree() {
  const { selectedThread, selectedThreadProject } = useThreadSelection();
  const selectedThreadDetail = useSelectedThreadDetail();

  const selectedThreadWorktreePath = useMemo(
    () =>
      resolvePreferredThreadWorktreePath({
        threadShellWorktreePath: selectedThread?.worktreePath ?? null,
        threadDetailWorktreePath: selectedThreadDetail?.worktreePath ?? null,
      }),
    [selectedThread?.worktreePath, selectedThreadDetail?.worktreePath],
  );

  // The shell carries the pool state; a parked pooled thread has no checkout,
  // so it gets no cwd rather than the project root (another checkout).
  const selectedThreadPoolState = selectedThread?.worktreePool?.state ?? null;
  const selectedThreadWorkspace = useMemo(
    () =>
      threadWorkspaceState({
        worktreePath: selectedThreadWorktreePath,
        worktreePool: selectedThreadPoolState === null ? null : { state: selectedThreadPoolState },
      }),
    [selectedThreadPoolState, selectedThreadWorktreePath],
  );

  return {
    selectedThreadWorktreePath,
    selectedThreadWorkspace,
    selectedThreadCwd:
      selectedThreadWorkspace.kind === "pooled-parked"
        ? null
        : (selectedThreadWorkspace.path ?? selectedThreadProject?.workspaceRoot ?? null),
  };
}
