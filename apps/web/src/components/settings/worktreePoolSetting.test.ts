import { describe, expect, it } from "vite-plus/test";

import { formatWorktreePool, parseWorktreePoolMaxTrees } from "./worktreePoolSetting";

describe("parseWorktreePoolMaxTrees", () => {
  it("accepts integers within the pool cap", () => {
    expect(parseWorktreePoolMaxTrees("1")).toBe(1);
    expect(parseWorktreePoolMaxTrees(" 8 ")).toBe(8);
    expect(parseWorktreePoolMaxTrees("64")).toBe(64);
  });

  it("rejects empty, fractional and out-of-range input", () => {
    expect(parseWorktreePoolMaxTrees("")).toBeNull();
    expect(parseWorktreePoolMaxTrees("0")).toBeNull();
    expect(parseWorktreePoolMaxTrees("65")).toBeNull();
    expect(parseWorktreePoolMaxTrees("3.5")).toBeNull();
    expect(parseWorktreePoolMaxTrees("eight")).toBeNull();
  });
});

describe("formatWorktreePool", () => {
  it("names the cap when the pool is on", () => {
    expect(formatWorktreePool("off")).toBe("Off");
    expect(formatWorktreePool({ maxTrees: 1 })).toBe("On · 1 checkout");
    expect(formatWorktreePool({ maxTrees: 8 })).toBe("On · 8 checkouts");
  });
});
