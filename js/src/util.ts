// Pure helpers: socket IDs, channel names, patterns and headers.

import { Match, type Msg } from "@nats-io/nats-core";

const base62 = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";

/** Returns 22 random base62 characters, the format of a socket ID. */
export function newSocketId(): string {
  let out = "";
  const buf = new Uint8Array(32);
  while (out.length < 22) {
    crypto.getRandomValues(buf);
    for (const b of buf) {
      // Rejection sampling keeps the distribution uniform: 248 = 4 * 62.
      if (b < 248) out += base62[b % 62];
      if (out.length === 22) break;
    }
  }
  return out;
}

/** Reports whether id is a well-formed socket ID. */
export function isSocketId(id: string): boolean {
  return /^[0-9A-Za-z]{22}$/.test(id);
}

const tokenRe = /^[A-Za-z0-9_-]+$/;

/**
 * Returns an error message when name is not a literal channel name: one to
 * eight dot-separated tokens of ASCII letters, digits, "-" or "_", at most 200
 * bytes in total. Returns undefined for valid names.
 */
export function channelNameError(name: string): string | undefined {
  if (name === "" || name.length > 200) return `invalid channel ${JSON.stringify(name)}`;
  const tokens = name.split(".");
  if (tokens.length > 8) return `channel ${JSON.stringify(name)} has more than 8 tokens`;
  if (!tokens.every((t) => tokenRe.test(t))) return `invalid channel ${JSON.stringify(name)}`;
  return undefined;
}

/**
 * Reports whether the literal channel name matches pattern with NATS wildcard
 * semantics: "*" matches one token, a final ">" one or more.
 */
export function matchPattern(pattern: string, name: string): boolean {
  const p = pattern.split(".");
  const n = name.split(".");
  for (let i = 0; i < p.length; i++) {
    const t = p[i];
    if (t === ">") return n.length > i;
    if (i >= n.length || (t !== "*" && t !== n[i])) return false;
  }
  return p.length === n.length;
}

/** Returns a header of a message, matched exactly first and then ignoring case. */
export function header(m: Pick<Msg, "headers">, key: string): string {
  const h = m.headers;
  if (!h) return "";
  return h.get(key) || h.get(key, Match.IgnoreCase);
}

/** Parses a decimal sequence header; malformed or missing values are 0. */
export function parseSeq(s: string): number {
  if (!/^\d+$/.test(s)) return 0;
  const v = Number(s);
  return Number.isSafeInteger(v) ? v : 0;
}

/** Parses an RFC 3339 timestamp with up to nanosecond precision. */
export function parseTime(s: string): Date | undefined {
  if (!s) return undefined;
  // Date only accepts millisecond precision reliably.
  const trimmed = s.replace(/(\.\d{3})\d+/, "$1");
  const d = new Date(trimmed);
  return Number.isNaN(d.getTime()) ? undefined : d;
}

const decoder = new TextDecoder();

/** Decodes an event payload: parsed JSON when it parses, else the raw string. */
export function decodePayload(data: Uint8Array): unknown {
  const text = decoder.decode(data);
  try {
    return JSON.parse(text);
  } catch {
    return text;
  }
}

/** Returns a random integer in [0, n]. */
export function randInt(n: number): number {
  return Math.floor(Math.random() * (Math.floor(n) + 1));
}

/** A promise with its resolve and reject functions. */
export interface Deferred<T> {
  promise: Promise<T>;
  resolve(v: T): void;
  reject(e: unknown): void;
}

/** Creates a Deferred. */
export function deferred<T>(): Deferred<T> {
  let resolve!: (v: T) => void;
  let reject!: (e: unknown) => void;
  const promise = new Promise<T>((res, rej) => {
    resolve = res;
    reject = rej;
  });
  return { promise, resolve, reject };
}
