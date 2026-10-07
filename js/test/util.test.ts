import { headers, type Msg } from "@nats-io/nats-core";
import { completeBatch } from "../src/subscription.js";
import { describe, expect, it } from "vitest";
import { channelNameError, decodePayload, header, isSocketId, matchPattern, newSocketId, parseSeq, parseTime } from "../src/util.js";

describe("newSocketId", () => {
  it("returns 22 base62 characters", () => {
    const seen = new Set<string>();
    for (let i = 0; i < 1000; i++) {
      const id = newSocketId();
      expect(id).toMatch(/^[0-9A-Za-z]{22}$/);
      expect(isSocketId(id)).toBe(true);
      seen.add(id);
    }
    expect(seen.size).toBe(1000);
  });

  it("rejects malformed socket IDs", () => {
    expect(isSocketId("short")).toBe(false);
    expect(isSocketId("a".repeat(21) + "-")).toBe(false);
  });
});

describe("matchPattern", () => {
  const cases: [string, string, boolean][] = [
    ["orders.42", "orders.42", true],
    ["orders.42", "orders.43", false],
    ["orders.*", "orders.42", true],
    ["orders.*", "orders.42.items", false],
    ["orders.*", "orders", false],
    ["orders.>", "orders.42", true],
    ["orders.>", "orders.42.items", true],
    ["orders.>", "orders", false],
    ["*.42", "orders.42", true],
    [">", "a", true],
    ["a.*.c", "a.b.c", true],
    ["a.*.c", "a.b.d", false],
    ["a.b", "a.b.c", false],
  ];
  it.each(cases)("%s matches %s: %s", (p, n, want) => {
    expect(matchPattern(p, n)).toBe(want);
  });
});

describe("channelNameError", () => {
  it("accepts literal names", () => {
    expect(channelNameError("orders.42")).toBeUndefined();
    expect(channelNameError("a-b_c.D9")).toBeUndefined();
    expect(channelNameError("1.2.3.4.5.6.7.8")).toBeUndefined();
  });
  it("rejects wildcards, empty tokens and long names", () => {
    for (const bad of ["", "a.*", "a.>", "a..b", ".a", "a b", "1.2.3.4.5.6.7.8.9", "x".repeat(201)]) {
      expect(channelNameError(bad), bad).toBeDefined();
    }
  });
});

describe("headers", () => {
  it("reads headers exactly and ignoring case", () => {
    const h = headers();
    h.set("Jetcast-Event", "a");
    h.set("nats-sequence", "12");
    expect(header({ headers: h }, "Jetcast-Event")).toBe("a");
    expect(header({ headers: h }, "Nats-Sequence")).toBe("12");
    expect(header({ headers: h }, "Missing")).toBe("");
    expect(header({ headers: undefined }, "Jetcast-Event")).toBe("");
  });

  it("parses sequences", () => {
    expect(parseSeq("42")).toBe(42);
    expect(parseSeq("")).toBe(0);
    expect(parseSeq("-1")).toBe(0);
    expect(parseSeq("x")).toBe(0);
  });

  it("parses nanosecond timestamps", () => {
    expect(parseTime("2026-10-07T05:06:07.123456789Z")?.toISOString()).toBe("2026-10-07T05:06:07.123Z");
    expect(parseTime("2026-10-07T05:06:07Z")?.toISOString()).toBe("2026-10-07T05:06:07.000Z");
    expect(parseTime("")).toBeUndefined();
    expect(parseTime("nope")).toBeUndefined();
  });
});

describe("decodePayload", () => {
  const enc = new TextEncoder();
  it("decodes JSON and falls back to the raw string", () => {
    expect(decodePayload(enc.encode('{"a":1}'))).toEqual({ a: 1 });
    expect(decodePayload(enc.encode("null"))).toBeNull();
    expect(decodePayload(enc.encode("not json"))).toBe("not json");
    expect(decodePayload(new Uint8Array())).toBe("");
  });
});

describe("completeBatch", () => {
  const ev = (seq: number, prev: number): Msg => {
    const h = headers();
    h.set("Nats-Sequence", String(seq));
    h.set("Nats-Last-Sequence", String(prev));
    return { headers: h } as unknown as Msg;
  };
  const full = [ev(12, 10), ev(15, 12), ev(20, 15)];
  it("accepts an intact batch", () => expect(completeBatch(full, 3, 10)).toBe(true));
  it("rejects a missing tail", () => expect(completeBatch(full.slice(0, 2), 3, 10)).toBe(false));
  it("rejects a hole", () => expect(completeBatch([full[0], full[2]], 2, 10)).toBe(false));
  it("rejects a wrong start", () => expect(completeBatch(full, 3, 9)).toBe(false));
  it("accepts an empty batch", () => expect(completeBatch([], 0, 10)).toBe(true));
});
