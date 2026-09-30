import { scopeThreadRef } from "@t3tools/client-runtime/environment";
import type { EnvironmentId, ScopedThreadRef } from "@t3tools/contracts";
import { threadWorkspaceCwd } from "@t3tools/shared/threadWorkspace";

import { useProjects } from "~/state/entities";

import { useHandleNewThread } from "./useHandleNewThread";

export interface ActiveProjectTarget {
  readonly environmentId: EnvironmentId;
  readonly cwd: string;
  readonly projectName: string;
  readonly threadRef: ScopedThreadRef;
}

/**
 * Resolves the project workspace behind the active thread (or draft) so
 * project-scoped surfaces like the file picker and content search know which
 * workspace to query and which thread's right panel opens their results.
 */
export function useActiveProjectTarget(): ActiveProjectTarget | null {
  const { activeDraftThread, activeThread } = useHandleNewThread();
  const projects = useProjects();
  const thread = activeThread ?? activeDraftThread;
  const threadId = activeThread?.id ?? activeDraftThread?.threadId;
  const project = thread
    ? projects.find(
        (candidate) =>
          candidate.environmentId === thread.environmentId && candidate.id === thread.projectId,
      )
    : null;
  // A parked pooled thread has no files on disk to pick or search.
  const cwd = thread ? threadWorkspaceCwd(thread, project?.workspaceRoot) : undefined;

  if (!thread || !threadId || !project || !cwd) return null;

  return {
    environmentId: project.environmentId,
    cwd,
    projectName: project.title,
    threadRef: scopeThreadRef(thread.environmentId, threadId),
  };
}
