// Builds and starts cmd/jetcast-dev for the integration tests.

import { spawn, execFileSync, type ChildProcess } from "node:child_process";
import { mkdtempSync, rmSync } from "node:fs";
import { createServer } from "node:net";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import type { TestProject } from "vitest/node";

declare module "vitest" {
  export interface ProvidedContext {
    wsUrl: string;
    httpUrl: string;
    maxAgeMs: number;
  }
}

const maxAgeMs = 3000;

function freePort(): Promise<number> {
  return new Promise((res, rej) => {
    const srv = createServer();
    srv.once("error", rej);
    srv.listen(0, "127.0.0.1", () => {
      const addr = srv.address();
      const port = typeof addr === "object" && addr ? addr.port : 0;
      srv.close(() => res(port));
    });
  });
}

let child: ChildProcess | undefined;
let dir: string | undefined;

export async function setup(project: TestProject): Promise<void> {
  const root = resolve(import.meta.dirname, "..", "..");
  dir = mkdtempSync(join(tmpdir(), "jetcast-js-test-"));
  const bin = join(dir, "jetcast-dev");
  execFileSync("go", ["build", "-o", bin, "./cmd/jetcast-dev"], { cwd: root, stdio: "inherit" });
  const port = await freePort();
  child = spawn(bin, ["-http", `127.0.0.1:${port}`, "-max-age", `${maxAgeMs}ms`, "-renew", "500ms"], {
    cwd: root,
    stdio: ["ignore", "pipe", "inherit"],
  });
  const wsUrl = await new Promise<string>((res, rej) => {
    let out = "";
    const timer = setTimeout(() => rej(new Error(`jetcast-dev did not start: ${out}`)), 60_000);
    child!.once("exit", (code) => rej(new Error(`jetcast-dev exited with ${code}: ${out}`)));
    child!.stdout!.on("data", (chunk: Buffer) => {
      out += chunk.toString();
      const m = /jetcast-dev ready ws=(\S+)/.exec(out);
      if (m) {
        clearTimeout(timer);
        res(m[1]);
      }
    });
  });
  project.provide("wsUrl", wsUrl);
  project.provide("httpUrl", `http://127.0.0.1:${port}`);
  project.provide("maxAgeMs", maxAgeMs);
}

export async function teardown(): Promise<void> {
  if (child && child.exitCode === null) {
    const exited = new Promise((res) => child!.once("exit", res));
    child.kill("SIGINT");
    const timer = setTimeout(() => child?.kill("SIGKILL"), 5000);
    await exited;
    clearTimeout(timer);
  }
  if (dir) rmSync(dir, { recursive: true, force: true });
}
