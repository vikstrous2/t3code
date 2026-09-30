import { describe, expect, it } from "vite-plus/test";

import {
  branchCarryOverWorkspace,
  isThreadWorkspaceParked,
  missingWorkspacePathReason,
  PARKED_WORKTREE_DESCRIPTION,
  threadWorkspaceCwd,
  threadWorkspaceState,
} from "./threadWorkspace.ts";

const SLOT = "/home/me/.t3/pool/repo/slot-1";

describe("threadWorkspaceState", () => {
  it("keeps unpooled threads on their worktree or the project root", () => {
    expect(threadWorkspaceState({ worktreePath: null })).toEqual({ kind: "local", path: null });
    expect(threadWorkspaceState({ worktreePath: "/wt/a", worktreePool: null })).toEqual({
      kind: "worktree",
      path: "/wt/a",
    });
  });

  it("reads the pool state instead of the path for pooled threads", () => {
    expect(threadWorkspaceState({ worktreePath: SLOT, worktreePool: { state: "leased" } })).toEqual(
      { kind: "pooled-leased", path: SLOT },
    );
    expect(threadWorkspaceState({ worktreePath: null, worktreePool: { state: "parked" } })).toEqual(
      { kind: "pooled-parked", path: null },
    );
  });

  it("treats a leased thread without a path as parked so nothing reads the project root", () => {
    expect(
      threadWorkspaceState({ worktreePath: null, worktreePool: { state: "leased" } }).kind,
    ).toBe("pooled-parked");
  });
});

describe("threadWorkspaceCwd", () => {
  it("falls back to the project root only for local threads", () => {
    expect(threadWorkspaceCwd({ worktreePath: null }, "/repo")).toBe("/repo");
    expect(threadWorkspaceCwd({ worktreePath: "/wt/a" }, "/repo")).toBe("/wt/a");
    expect(
      threadWorkspaceCwd({ worktreePath: SLOT, worktreePool: { state: "leased" } }, "/repo"),
    ).toBe(SLOT);
    expect(
      threadWorkspaceCwd({ worktreePath: null, worktreePool: { state: "parked" } }, "/repo"),
    ).toBeNull();
  });
});

describe("branchCarryOverWorkspace", () => {
  it("never hands a pool checkout to a follow-up thread", () => {
    expect(
      branchCarryOverWorkspace({ worktreePath: SLOT, worktreePool: { state: "leased" } }),
    ).toEqual({ worktreePath: null, envMode: "worktree" });
    expect(
      branchCarryOverWorkspace({ worktreePath: null, worktreePool: { state: "parked" } }),
    ).toEqual({ worktreePath: null, envMode: "worktree" });
  });

  it("reuses a plain worktree and keeps local threads local", () => {
    expect(branchCarryOverWorkspace({ worktreePath: "/wt/a" })).toEqual({
      worktreePath: "/wt/a",
      envMode: "worktree",
    });
    expect(branchCarryOverWorkspace({ worktreePath: null })).toEqual({
      worktreePath: null,
      envMode: "local",
    });
  });
});

describe("missingWorkspacePathReason", () => {
  it("explains a parked checkout", () => {
    const parked = { worktreePath: null, worktreePool: { state: "parked" as const } };
    expect(isThreadWorkspaceParked(parked)).toBe(true);
    expect(missingWorkspacePathReason(parked)).toBe(PARKED_WORKTREE_DESCRIPTION);
    expect(missingWorkspacePathReason({ worktreePath: null })).not.toBe(
      PARKED_WORKTREE_DESCRIPTION,
    );
  });
});
