// Helpers for integration tests against jetcast-dev.

import { inject } from "vitest";
import { connect, UnauthorizedError, type ChannelState, type Echo, type EventMeta, type Status } from "../src/index.js";

export const wsUrl = inject("wsUrl");
export const httpUrl = inject("httpUrl");
export const maxAgeMs = inject("maxAgeMs");

let counter = 0;

/** Returns a unique identifier token for users and channels. */
export function uniq(prefix: string): string {
  counter++;
  return `${prefix}${counter}_${Math.random().toString(36).slice(2, 8)}`;
}

/** Calls the jetcast-dev control API. */
export async function control(path: string, body: unknown = {}): Promise<any> {
  const r = await fetch(httpUrl + path, { method: "POST", body: JSON.stringify(body) });
  const text = await r.text();
  if (!r.ok) throw new Error(`${path}: ${r.status} ${text}`);
  return text ? JSON.parse(text) : undefined;
}

/** Broadcasts an event; channels are "<kind>.<name>". */
export function broadcast(name: string, channels: string[], data: unknown, origin?: string): Promise<{ id: string; sequences: number[] }> {
  return control("/broadcast", { name, channels, data, origin });
}

/** Polls until f returns a truthy value. */
export async function waitFor<T>(f: () => T, what = "condition", timeout = 20_000): Promise<NonNullable<T>> {
  const deadline = Date.now() + timeout;
  for (;;) {
    const v = f();
    if (v) return v as NonNullable<T>;
    if (Date.now() > deadline) throw new Error(`timed out waiting for ${what}`);
    await new Promise((r) => setTimeout(r, 20));
  }
}

/** A client whose getToken can be paused or made to fail. */
export interface TestClient {
  echo: Echo;
  user: string;
  statuses: Status[];
  /** Makes getToken wait until resume() is called. */
  pause(): void;
  resume(): void;
  /** Makes getToken throw UnauthorizedError. */
  revoke(): void;
  tokenCalls: number;
}

/** Connects a client as user with a fresh session. */
export async function client(user = uniq("u"), headsInterval?: number): Promise<TestClient> {
  let gate: Promise<void> | undefined;
  let open: (() => void) | undefined;
  let revoked = false;
  const tc = {
    user,
    statuses: [] as Status[],
    tokenCalls: 0,
    pause() {
      gate = new Promise((r) => (open = r));
    },
    resume() {
      open?.();
      gate = undefined;
    },
    revoke() {
      revoked = true;
    },
  } as TestClient;
  tc.echo = await connect({
    servers: wsUrl,
    logger: {},
    headsInterval,
    getToken: async () => {
      tc.tokenCalls++;
      if (gate) await gate;
      if (revoked) throw new UnauthorizedError();
      return `${user}:s1`;
    },
  });
  tc.echo.onStatus((s) => tc.statuses.push(s));
  return tc;
}

/** Records events and states of a channel. */
export class Recorder {
  events: { data: any; meta: EventMeta }[] = [];
  states: ChannelState[] = [];

  /** Returns a listener that records events. */
  readonly listener = (data: any, meta: EventMeta) => {
    this.events.push({ data, meta });
  };
  readonly onState = (s: ChannelState) => {
    this.states.push(s);
  };

  names(): string[] {
    return this.events.map((e) => e.meta.event);
  }

  lastState(): ChannelState | undefined {
    return this.states.at(-1);
  }
}
