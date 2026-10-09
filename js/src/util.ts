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

const sha256K = new Uint32Array([
  0x428a2f98, 0x71374491, 0xb5c0fbcf, 0xe9b5dba5, 0x3956c25b, 0x59f111f1, 0x923f82a4, 0xab1c5ed5, 0xd807aa98,
  0x12835b01, 0x243185be, 0x550c7dc3, 0x72be5d74, 0x80deb1fe, 0x9bdc06a7, 0xc19bf174, 0xe49b69c1, 0xefbe4786,
  0x0fc19dc6, 0x240ca1cc, 0x2de92c6f, 0x4a7484aa, 0x5cb0a9dc, 0x76f988da, 0x983e5152, 0xa831c66d, 0xb00327c8,
  0xbf597fc7, 0xc6e00bf3, 0xd5a79147, 0x06ca6351, 0x14292967, 0x27b70a85, 0x2e1b2138, 0x4d2c6dfc, 0x53380d13,
  0x650a7354, 0x766a0abb, 0x81c2c92e, 0x92722c85, 0xa2bfe8a1, 0xa81a664b, 0xc24b8b70, 0xc76c51a3, 0xd192e819,
  0xd6990624, 0xf40e3585, 0x106aa070, 0x19a4c116, 0x1e376c08, 0x2748774c, 0x34b0bcb5, 0x391c0cb3, 0x4ed8aa4a,
  0x5b9cca4f, 0x682e6ff3, 0x748f82ee, 0x78a5636f, 0x84c87814, 0x8cc70208, 0x90befffa, 0xa4506ceb, 0xbef9a3f7,
  0xc67178f2,
]);

/** Returns the SHA-256 digest of data. Synchronous, since WebCrypto is async and missing outside secure contexts. */
export function sha256(data: Uint8Array): Uint8Array {
  const len = data.length;
  const padded = new Uint8Array(((len + 9 + 63) >> 6) << 6);
  padded.set(data);
  padded[len] = 0x80;
  const view = new DataView(padded.buffer);
  view.setUint32(padded.length - 8, Math.floor(len / 0x20000000));
  view.setUint32(padded.length - 4, (len << 3) >>> 0);
  const h = new Uint32Array([
    0x6a09e667, 0xbb67ae85, 0x3c6ef372, 0xa54ff53a, 0x510e527f, 0x9b05688c, 0x1f83d9ab, 0x5be0cd19,
  ]);
  const w = new Uint32Array(64);
  const rotr = (x: number, n: number) => (x >>> n) | (x << (32 - n));
  for (let off = 0; off < padded.length; off += 64) {
    for (let i = 0; i < 16; i++) w[i] = view.getUint32(off + 4 * i);
    for (let i = 16; i < 64; i++) {
      const s0 = rotr(w[i - 15], 7) ^ rotr(w[i - 15], 18) ^ (w[i - 15] >>> 3);
      const s1 = rotr(w[i - 2], 17) ^ rotr(w[i - 2], 19) ^ (w[i - 2] >>> 10);
      w[i] = w[i - 16] + s0 + w[i - 7] + s1;
    }
    let [a, b, c, d, e, f, g, hh] = h;
    for (let i = 0; i < 64; i++) {
      const t1 = (hh + (rotr(e, 6) ^ rotr(e, 11) ^ rotr(e, 25)) + ((e & f) ^ (~e & g)) + sha256K[i] + w[i]) >>> 0;
      const t2 = ((rotr(a, 2) ^ rotr(a, 13) ^ rotr(a, 22)) + ((a & b) ^ (a & c) ^ (b & c))) >>> 0;
      hh = g;
      g = f;
      f = e;
      e = (d + t1) >>> 0;
      d = c;
      c = b;
      b = a;
      a = (t1 + t2) >>> 0;
    }
    h[0] += a;
    h[1] += b;
    h[2] += c;
    h[3] += d;
    h[4] += e;
    h[5] += f;
    h[6] += g;
    h[7] += hh;
  }
  const out = new Uint8Array(32);
  const ov = new DataView(out.buffer);
  for (let i = 0; i < 8; i++) ov.setUint32(4 * i, h[i]);
  return out;
}

/** Returns the Jetcast-Origin value of events caused by a socket, as the server computes it. */
export function originTag(socket: string): string {
  const digest = sha256(new TextEncoder().encode(`jetcast-origin:${socket}`));
  let hex = "";
  for (const b of digest.subarray(0, 16)) hex += b.toString(16).padStart(2, "0");
  return hex;
}
