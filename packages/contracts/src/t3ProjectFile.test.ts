import * as Schema from "effect/Schema";
import { describe, expect, it } from "vite-plus/test";

import { T3ProjectFile } from "./t3ProjectFile.ts";

const decode = Schema.decodeUnknownSync(T3ProjectFile);

describe("T3ProjectFile", () => {
  it("decodes a full project file", () => {
    const decoded = decode({
      $schema: "https://t3.codes/schema/t3.json",
      iconPath: "assets/logo.svg",
      scripts: [
        {
          name: "Dev",
          command: "pnpm dev",
          icon: "play",
          runOnWorktreeCreate: false,
          previewUrl: "http://localhost:3000",
          autoOpenPreview: true,
        },
        { name: "Test", command: "pnpm test" },
        { name: "Setup", command: "pnpm i", runOnWorktreeCreate: true, async: false },
      ],
    });

    expect(decoded.iconPath).toBe("assets/logo.svg");
    expect(decoded.scripts).toHaveLength(3);
    expect(decoded.scripts?.[1]).toEqual({ name: "Test", command: "pnpm test" });
    expect(decoded.scripts?.[2]?.async).toBe(false);
  });

  it("decodes an empty object and ignores unknown fields", () => {
    expect(decode({})).toEqual({});
    expect(decode({ futureField: true })).toEqual({});
  });

  it("trims icon paths and script fields", () => {
    const decoded = decode({
      iconPath: " assets/logo.svg ",
      scripts: [{ name: " Dev ", command: " pnpm dev " }],
    });

    expect(decoded.iconPath).toBe("assets/logo.svg");
    expect(decoded.scripts?.[0]).toEqual({ name: "Dev", command: "pnpm dev" });
  });

  it("rejects scripts without a command", () => {
    expect(() => decode({ scripts: [{ name: "Dev" }] })).toThrow();
  });

  it("rejects unknown script icons", () => {
    expect(() =>
      decode({ scripts: [{ name: "Dev", command: "pnpm dev", icon: "rocket" }] }),
    ).toThrow();
  });

  it("decodes defaultThreadEnvMode and rejects unknown modes", () => {
    expect(decode({ defaultThreadEnvMode: "worktree" }).defaultThreadEnvMode).toBe("worktree");
    expect(decode({ defaultThreadEnvMode: "local" }).defaultThreadEnvMode).toBe("local");
    expect(() => decode({ defaultThreadEnvMode: "remote" })).toThrow();
  });

  it("decodes worktreeSubmodules and rejects unknown modes", () => {
    expect(decode({ worktreeSubmodules: "none" }).worktreeSubmodules).toBe("none");
    expect(decode({ worktreeSubmodules: "top-level" }).worktreeSubmodules).toBe("top-level");
    expect(() => decode({ worktreeSubmodules: "shallow" })).toThrow();
  });

  it("decodes worktreePool and rejects a cap that is not a positive integer", () => {
    expect(decode({ worktreePool: { maxTrees: 8 } }).worktreePool).toEqual({ maxTrees: 8 });
    expect(decode({ worktreePool: "off" }).worktreePool).toBe("off");
    expect(() => decode({ worktreePool: { maxTrees: 0 } })).toThrow();
    expect(() => decode({ worktreePool: { maxTrees: 2.5 } })).toThrow();
    expect(() => decode({ worktreePool: true })).toThrow();
  });
});
