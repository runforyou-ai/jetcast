import { afterEach, describe, expect, it } from "vitest";
import { connect, UnauthorizedError, type Echo } from "../src/index.js";
import { broadcast, client, control, maxAgeMs, Recorder, uniq, waitFor, wsUrl, type TestClient } from "./helpers.js";

const clients: TestClient[] = [];

/** Connects a client that is closed after the test. */
async function open(user?: string, headsInterval?: number): Promise<TestClient> {
  const c = await client(user, headsInterval);
  clients.push(c);
  return c;
}

afterEach(async () => {
  await Promise.all(clients.splice(0).map((c) => c.echo.close()));
});

/** Internal delivery path of a channel, for assertions. */
function pathOf(ch: unknown): string {
  return (ch as { path: string }).path;
}

describe("connection", () => {
  it("connects with a socket ID and reports info", async () => {
    const c = await open();
    expect(c.echo.status).toBe("connected");
    expect(c.echo.socketId).toMatch(/^[0-9A-Za-z]{22}$/);
    expect(c.echo.info).toEqual({ name: c.user });
  });

  it("rejects connect when getToken throws UnauthorizedError", async () => {
    await expect(
      connect({ servers: wsUrl, logger: {}, getToken: () => Promise.reject(new UnauthorizedError()) }),
    ).rejects.toBeInstanceOf(UnauthorizedError);
  });

  it("keeps working when listeners throw", async () => {
    const c = await open();
    const name = uniq("news");
    const rec = new Recorder();
    const ch = c.echo
      .channel(name)
      .listen("boom", () => {
        throw new Error("listener failure");
      })
      .listen("boom", rec.listener);
    c.echo.onStatus(() => {
      throw new Error("status listener failure");
    });
    await ch.ready();
    await broadcast("boom", [`pub.${name}`], 1);
    await broadcast("boom", [`pub.${name}`], 2);
    await waitFor(() => rec.events.length === 2, "events");
    expect(rec.events.map((e) => e.data)).toEqual([1, 2]);
  });
});

describe("public channels", () => {
  it("delivers events to several clients", async () => {
    const [a, b] = await Promise.all([open(), open()]);
    const name = uniq("news");
    const ra = new Recorder();
    const rb = new Recorder();
    const all = new Recorder();
    const cha = a.echo.channel(name).listen("article.published", ra.listener).listenAll(all.listener);
    cha.onState(ra.onState);
    const chb = b.echo.channel(name).listen("article.published", rb.listener);
    expect(a.echo.channel(name)).toBe(cha);
    await Promise.all([cha.ready(), chb.ready()]);
    expect(ra.states).toEqual([{ state: "subscribed", recovered: false, reason: "initial" }]);

    const res = await broadcast("article.published", [`pub.${name}`], { title: "hello" });
    await broadcast("other", [`pub.${name}`], "plain");
    await waitFor(() => ra.events.length === 1 && rb.events.length === 1 && all.events.length === 2, "events");
    const { data, meta } = ra.events[0];
    expect(data).toEqual({ title: "hello" });
    expect(meta.event).toBe("article.published");
    expect(meta.channel).toBe(name);
    expect(meta.id).toBe(res.id);
    expect(meta.sequence).toBe(res.sequences[0]);
    expect(meta.time).toBeInstanceOf(Date);
    expect(new TextDecoder().decode(meta.raw)).toBe('{"title":"hello"}');
    expect(all.events[1].data).toBe("plain");

    cha.stopListening("article.published", ra.listener);
    await broadcast("article.published", [`pub.${name}`], 2);
    await waitFor(() => rb.events.length === 2, "second event");
    expect(ra.events).toHaveLength(1);
  });

  it("stops delivery after leave", async () => {
    const c = await open();
    const name = uniq("news");
    const rec = new Recorder();
    const ch = c.echo.channel(name).listenAll(rec.listener);
    ch.onState(rec.onState);
    await ch.ready();
    ch.leave();
    await waitFor(() => rec.lastState()?.state === "left", "left");
    expect(c.echo.channel(name)).not.toBe(ch);
    await broadcast("x", [`pub.${name}`], 1);
    await new Promise((r) => setTimeout(r, 200));
    expect(rec.events).toHaveLength(0);
  });

  it("subscribes ephemeral channels without recovery", async () => {
    const c = await open();
    const name = `typing.${uniq("t")}`;
    const rec = new Recorder();
    const ch = c.echo.channel(name).listenAll(rec.listener);
    ch.onState(rec.onState);
    await ch.ready();
    expect(rec.states).toEqual([{ state: "subscribed", recovered: false, reason: "ephemeral" }]);
    await broadcast("typing", [`pub.${name}`], { who: "x" });
    await waitFor(() => rec.events.length === 1, "event");
    expect(rec.events[0].meta.sequence).toBe(0);
    expect(rec.events[0].data).toEqual({ who: "x" });
  });

  it("denies invalid channel names", async () => {
    const c = await open();
    await expect(c.echo.channel("bad.*").ready()).rejects.toThrow(/invalid channel/);
  });
});

describe("private channels", () => {
  it("subscribes granted channels directly", async () => {
    const user = uniq("g");
    await control("/grant", { user, patterns: [`${user}.>`] });
    const c = await open(user);
    const name = `${user}.feed`;
    const rec = new Recorder();
    const ch = c.echo.private(name).listen("e", rec.listener);
    await ch.ready();
    expect(pathOf(ch)).toBe("direct");
    await broadcast("e", [`prv.${name}`], 1);
    await waitFor(() => rec.events.length === 1, "event");
  });

  it("relays channels the authorizer allows", async () => {
    const c = await open();
    const name = `orders.${uniq("o")}`;
    await control("/allow", { user: c.user, channel: name });
    const rec = new Recorder();
    const ch = c.echo.private(name).listen("order.shipped", rec.listener);
    await ch.ready();
    expect(pathOf(ch)).toBe("relay");
    for (let i = 1; i <= 3; i++) await broadcast("order.shipped", [`prv.${name}`], i);
    await waitFor(() => rec.events.length === 3, "events");
    expect(rec.events.map((e) => e.data)).toEqual([1, 2, 3]);
    // Renewals every 500ms keep the relay alive.
    await new Promise((r) => setTimeout(r, 2000));
    await broadcast("order.shipped", [`prv.${name}`], 4);
    await waitFor(() => rec.events.length === 4, "event after renewals");
    expect(ch.state).toBe("subscribed");
  });

  it("rejects channels the authorizer denies", async () => {
    const c = await open();
    const name = `orders.${uniq("o")}`;
    const rec = new Recorder();
    const ch = c.echo.private(name);
    ch.onState(rec.onState);
    await expect(ch.ready()).rejects.toThrow(/denied/);
    await waitFor(() => rec.lastState()?.state === "denied", "denied state");
  });

  it("reports denied when the server removes the subscription", async () => {
    const c = await open();
    const name = `orders.${uniq("o")}`;
    await control("/allow", { user: c.user, channel: name });
    const rec = new Recorder();
    const ch = c.echo.private(name);
    ch.onState(rec.onState);
    await ch.ready();
    await control("/leave", { channel: `prv.${name}`, user: c.user });
    await waitFor(() => rec.lastState()?.state === "denied", "denied state");
    expect(rec.lastState()?.reason).toBe("leave");
  });
});

describe("toOthers", () => {
  it("skips the origin's listeners and keeps its cursor", async () => {
    const [a, b] = await Promise.all([open(), open()]);
    const name = uniq("news");
    const ra = new Recorder();
    const rb = new Recorder();
    const cha = a.echo.channel(name).listenAll(ra.listener);
    cha.onState(ra.onState);
    const chb = b.echo.channel(name).listenAll(rb.listener);
    await Promise.all([cha.ready(), chb.ready()]);

    await broadcast("mine", [`pub.${name}`], 1, a.echo.socketId);
    await waitFor(() => rb.events.length === 1, "event at b");
    const next = await broadcast("next", [`pub.${name}`], 2);
    await waitFor(() => ra.events.length === 1 && rb.events.length === 2, "next event");
    expect(ra.names()).toEqual(["next"]);
    expect(ra.events[0].meta.sequence).toBe(next.sequences[0]);
    expect(rb.names()).toEqual(["mine", "next"]);
    // No recovery happened: the only state is the initial subscription.
    expect(ra.states).toHaveLength(1);
  });
});

describe("reconnection", () => {
  it("recovers events broadcast while disconnected", async () => {
    const c = await open();
    const name = uniq("news");
    const rec = new Recorder();
    const ch = c.echo.channel(name).listenAll(rec.listener);
    ch.onState(rec.onState);
    await ch.ready();
    await broadcast("before", [`pub.${name}`], 0);
    await waitFor(() => rec.events.length === 1, "first event");

    const socket = c.echo.socketId;
    c.pause();
    await control("/kick", { socket });
    await waitFor(() => c.echo.status === "reconnecting", "reconnecting");
    for (let i = 1; i <= 3; i++) await broadcast("missed", [`pub.${name}`], i);
    c.resume();

    await waitFor(() => rec.events.length === 4, "recovered events");
    await waitFor(() => rec.lastState()?.state === "subscribed", "subscribed");
    expect(rec.events.map((e) => e.data)).toEqual([0, 1, 2, 3]);
    expect(rec.states.map((s) => s.state)).toEqual(["subscribed", "interrupted", "recovering", "subscribed"]);
    expect(rec.lastState()?.recovered).toBe(true);
    expect(c.echo.socketId).not.toBe(socket);
    expect(c.statuses).toEqual(["reconnecting", "connected"]);

    await broadcast("after", [`pub.${name}`], 4);
    await waitFor(() => rec.events.length === 5, "live event");
  });

  it("recovers relayed channels after a reconnect", async () => {
    const c = await open();
    const name = `orders.${uniq("o")}`;
    await control("/allow", { user: c.user, channel: name });
    const rec = new Recorder();
    const ch = c.echo.private(name).listenAll(rec.listener);
    ch.onState(rec.onState);
    await ch.ready();

    c.pause();
    await control("/kick", { socket: c.echo.socketId });
    await waitFor(() => c.echo.status === "reconnecting", "reconnecting");
    await broadcast("missed", [`prv.${name}`], 1);
    c.resume();
    await waitFor(() => rec.events.length === 1, "recovered event");
    await waitFor(() => rec.lastState()?.recovered === true, "recovered");
    await broadcast("live", [`prv.${name}`], 2);
    await waitFor(() => rec.events.length === 2, "live event");
  });

  it("reports expired when history is gone", async () => {
    const c = await open();
    const name = uniq("news");
    const rec = new Recorder();
    const ch = c.echo.channel(name).listenAll(rec.listener);
    ch.onState(rec.onState);
    await ch.ready();

    c.pause();
    await control("/kick", { socket: c.echo.socketId });
    await waitFor(() => c.echo.status === "reconnecting", "reconnecting");
    await broadcast("lost", [`pub.${name}`], 1);
    // Let the stream evict the event; eviction cannot be observed from here.
    await new Promise((r) => setTimeout(r, 2 * maxAgeMs + 1000));
    c.resume();

    await waitFor(() => rec.states.length > 1 && rec.lastState()?.state === "subscribed", "resubscribed");
    expect(rec.lastState()).toEqual({ state: "subscribed", recovered: false, reason: "expired" });
    expect(rec.events).toHaveLength(0);
    await broadcast("live", [`pub.${name}`], 2);
    await waitFor(() => rec.events.length === 1, "live event");
  });
});

describe("heads", () => {
  it("advances idle cursors to the end of the stream", async () => {
    const c = await open(undefined, 1000);
    const pubName = uniq("idle");
    const prvName = `orders.${uniq("o")}`;
    await control("/allow", { user: c.user, channel: prvName });
    const pub = c.echo.channel(pubName);
    const prv = c.echo.private(prvName);
    const rec = new Recorder();
    pub.onState(rec.onState);
    prv.onState(rec.onState);
    await Promise.all([pub.ready(), prv.ready()]);
    const res = await broadcast("elsewhere", [`pub.${uniq("other")}`], 1);
    const seq = res.sequences[0];
    const posOf = (ch: unknown) => (ch as { pos: number }).pos;
    await waitFor(() => posOf(pub) >= seq && posOf(prv) >= seq, "advanced cursors", 10_000);
    expect(rec.states.every((s) => s.state === "subscribed")).toBe(true);
  });
});

describe("server control", () => {
  it("stops after disconnect", async () => {
    const c = await open();
    const stopped = new Promise<void>((r) => c.echo.onStatus((s) => s === "stopped" && r()));
    c.revoke();
    await control("/disconnect", { user: c.user });
    await stopped;
    expect(c.echo.status).toBe("stopped");
    expect(c.echo.socketId).toBe("");
  });

  it("stops when getToken throws UnauthorizedError on reconnect", async () => {
    const c = await open();
    c.revoke();
    await control("/kick", { socket: c.echo.socketId });
    await waitFor(() => c.echo.status === "stopped", "stopped");
    expect(c.statuses).toEqual(["reconnecting", "stopped"]);
  });

  it("switches to a fresh connection on refresh and uses new grants", async () => {
    const c = await open();
    const pubName = uniq("news");
    const pubRec = new Recorder();
    const pubCh = c.echo.channel(pubName).listenAll(pubRec.listener);
    pubCh.onState(pubRec.onState);
    await pubCh.ready();

    await control("/grant", { user: c.user, patterns: [`${c.user}.>`] });
    const socket = c.echo.socketId;
    await control("/refresh", { user: c.user });
    await waitFor(() => c.echo.socketId !== socket, "new socket", 20_000);
    expect(c.statuses).toEqual([]);

    const name = `${c.user}.feed`;
    const rec = new Recorder();
    const ch = c.echo.private(name).listenAll(rec.listener);
    await ch.ready();
    expect(pathOf(ch)).toBe("direct");
    await broadcast("e", [`prv.${name}`, `pub.${pubName}`], 1);
    await waitFor(() => rec.events.length === 1 && pubRec.events.length === 1, "events");
    await waitFor(() => pubRec.lastState()?.state === "subscribed", "resubscribed");
    expect(pubRec.states.map((s) => s.state)).toEqual(["subscribed", "interrupted", "recovering", "subscribed"]);
    expect(pubRec.lastState()?.recovered).toBe(true);
  });
});

describe("close", () => {
  it("stops and fails new channels", async () => {
    const c = await open();
    const echo: Echo = c.echo;
    await echo.close();
    expect(echo.status).toBe("stopped");
    await expect(echo.channel(uniq("x")).ready()).rejects.toThrow(/closed/);
  });
});
