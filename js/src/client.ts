// The jetcast client: connection lifecycle, control messages and periodic
// checks. Mirrors client/client.go.

import { NoRespondersError, RequestError, wsconnect, type Msg } from "@nats-io/nats-core";
import { Connection, connNamespace } from "./connection.js";
import { CodeDenied, Header, type Control, type HeadsResponse, type HelloResponse, type RenewResponse } from "./protocol.js";
import { Channel, type SubscriptionHost } from "./subscription.js";
import { channelNameError, deferred, header, newSocketId, randInt } from "./util.js";

/** Thrown by `getToken` to stop the client for good, for example after logout. */
export class UnauthorizedError extends Error {
  constructor(message = "jetcast: unauthorized") {
    super(message);
    this.name = "UnauthorizedError";
  }
}

/** Minimal logger; `console` fits. */
export interface Logger {
  debug?(message: string, ...args: unknown[]): void;
  warn?(message: string, ...args: unknown[]): void;
}

/** Options of `connect`. */
export interface ConnectOptions {
  /** NATS WebSocket URLs, such as "wss://example.com/nats". */
  servers: string | string[];
  /**
   * Returns the credential for a new connection. It is called for every
   * connection, including reconnections and credential refreshes. Throw
   * `UnauthorizedError` to stop the client; other errors are retried with
   * backoff.
   */
  getToken: () => Promise<string> | string;
  /** Must match the server's prefix, "jetcast" by default. */
  prefix?: string;
  /** How often idle channels are checked for missed events, in ms; 30000 by default. */
  headsInterval?: number;
  /** Receives debug and warning messages; warnings go to `console.warn` by default. */
  logger?: Logger;
}

/** Connection status of a client. */
export type Status = "connecting" | "connected" | "reconnecting" | "stopped";

const defaultLogger: Logger = { warn: (m, ...a) => console.warn(m, ...a) };

/**
 * Renewal periods without a successful renewal after which a node's relays
 * are rebuilt elsewhere. The node drops relays not renewed for more than three
 * periods, checking once per period.
 */
const renewGrace = 4;

/** Relay renewal period stated in hello, in ms. */
function renewPeriod(conn: Connection): number {
  return conn.hello.renewMs && conn.hello.renewMs > 0 ? conn.hello.renewMs : 20_000;
}

/** Connects and resolves once the first connection is established. */
export async function connect(opts: ConnectOptions): Promise<Echo> {
  const echo = new Echo(opts);
  await echo.started;
  return echo;
}

/**
 * A jetcast client. It keeps one NATS connection at a time; every connection
 * uses a fresh socket ID. When a connection drops or its credentials near
 * expiry, the client opens a new one and resubscribes every channel,
 * recovering missed events.
 */
export class Echo {
  private readonly servers: string[];
  private readonly getToken: () => Promise<string> | string;
  private readonly prefix: string;
  private readonly headsInterval: number;
  private readonly log: Logger;

  private conn: Connection | null = null;
  private statusValue: Status = "connecting";
  private statusFns: ((s: Status) => void)[] = [];
  private subs = new Map<string, Channel>();
  /**
   * Jetcast-Origin values of this client's connections, for toOthers, with
   * when each connection was replaced (0 for the current one). Events of
   * replaced connections can arrive until they leave the retention window.
   */
  private origins = new Map<string, number>();
  private closed = false;
  private connecting = false;
  private readyD = deferred<void>();
  private queue: (() => void)[] = [];
  private flushing = false;
  private ticker: ReturnType<typeof setInterval>;
  private lastRenew = 0;
  private lastHeads = 0;
  private lastHeadsSoon = 0;
  private wake: (() => void) | null = null;
  private readonly host: SubscriptionHost;
  private readonly onVisibility = () => this.visible();

  /** @internal Use `connect`. */
  constructor(opts: ConnectOptions) {
    const servers = typeof opts.servers === "string" ? [opts.servers] : [...(opts.servers ?? [])];
    if (servers.length === 0 || typeof opts.getToken !== "function") {
      throw new Error("jetcast: servers and getToken are required");
    }
    this.servers = servers;
    this.getToken = opts.getToken;
    this.prefix = opts.prefix || "jetcast";
    this.headsInterval = opts.headsInterval && opts.headsInterval > 0 ? opts.headsInterval : 30_000;
    this.log = opts.logger ?? defaultLogger;
    this.readyD.promise.catch(() => {});
    this.host = {
      call: (f) => this.call(f),
      ownOrigin: (o) => this.origins.has(o),
      forget: (s) => {
        if (this.subs.get(s.key) === s) this.subs.delete(s.key);
      },
      current: () => this.conn,
      headsSoon: () => this.headsSoon(),
    };
    this.ticker = setInterval(() => this.tick(), 1000);
    if (typeof document !== "undefined") document.addEventListener("visibilitychange", this.onVisibility);
    this.startConnecting(false);
  }

  /** @internal Resolves on the first connection; rejects if the client stops first. */
  get started(): Promise<void> {
    return this.readyD.promise;
  }

  /**
   * Socket ID of the current connection. Send it with application requests,
   * for example as the X-Socket-ID header, so broadcasts can exclude this
   * client. It changes when the client switches connections.
   */
  get socketId(): string {
    return this.conn?.socket ?? "";
  }

  /** Current connection status. */
  get status(): Status {
    return this.statusValue;
  }

  /** User info returned by the server for the current connection. */
  get info(): unknown {
    return this.conn?.hello.info;
  }

  /** Registers a status listener and returns a function that removes it. */
  onStatus(cb: (s: Status) => void): () => void {
    this.statusFns.push(cb);
    return () => {
      this.statusFns = this.statusFns.filter((f) => f !== cb);
    };
  }

  /** Subscribes to a public channel. The same channel returns the same instance. */
  channel(name: string): Channel {
    return this.subscribe("pub", name);
  }

  /** Subscribes to a private channel. The same channel returns the same instance. */
  private(name: string): Channel {
    return this.subscribe("prv", name);
  }

  /** Closes the client. */
  async close(): Promise<void> {
    if (this.closed) return;
    this.closed = true;
    const conn = this.conn;
    this.conn = null;
    this.setStatus("stopped");
    this.readyD.reject(new Error("jetcast: client closed"));
    clearInterval(this.ticker);
    this.wake?.();
    if (typeof document !== "undefined") document.removeEventListener("visibilitychange", this.onVisibility);
    for (const s of this.subs.values()) {
      s.sendLeave(conn);
      s.closed();
    }
    if (conn && !conn.closed) {
      try {
        await conn.nc.flush();
      } catch {
        // Closing anyway.
      }
      await conn.nc.close().catch(() => {});
    }
  }

  private subscribe(kind: "pub" | "prv", name: string): Channel {
    const key = `${kind}.${name}`;
    const existing = this.subs.get(key);
    if (existing) return existing;
    const s = new Channel(this.host, kind, name);
    const invalid = channelNameError(name);
    if (invalid) {
      s.fail(new Error(`jetcast: ${invalid}`));
      return s;
    }
    if (this.closed) {
      s.fail(new Error("jetcast: client closed"));
      return s;
    }
    this.subs.set(key, s);
    if (this.conn) s.resubscribe(this.conn);
    return s;
  }

  private setStatus(s: Status): void {
    if (this.statusValue === s) return;
    this.statusValue = s;
    for (const f of this.statusFns) this.call(() => f(s));
  }

  /** Queues an application callback; callbacks run one at a time in order, and their errors are logged. */
  private call(f: () => void): void {
    this.queue.push(f);
    if (this.flushing) return;
    this.flushing = true;
    queueMicrotask(() => {
      while (this.queue.length > 0) {
        const g = this.queue.shift()!;
        try {
          g();
        } catch (e) {
          this.log.warn?.("jetcast: listener failed", e);
        }
      }
      this.flushing = false;
    });
  }

  /** Stops the client because it may not continue, such as after revocation. */
  private stop(err: Error): void {
    this.log.warn?.("jetcast: client stopped", err.message);
    this.readyD.reject(err);
    void this.close();
  }

  /** Waits ms milliseconds or until the client closes. */
  private sleep(ms: number): Promise<void> {
    return new Promise((resolve) => {
      const t = setTimeout(done, ms);
      const self = this;
      function done() {
        clearTimeout(t);
        if (self.wake === done) self.wake = null;
        resolve();
      }
      this.wake = done;
    });
  }

  /**
   * Opens a new connection in the background, retrying with backoff. With
   * refresh set, the current connection stays in use until the new one is
   * ready.
   */
  private startConnecting(refresh: boolean): void {
    if (this.closed) return;
    if (!refresh && this.conn) this.setStatus("reconnecting");
    if (this.connecting) return;
    this.connecting = true;
    void (async () => {
      let delay = 250;
      for (;;) {
        let conn: Connection;
        try {
          conn = await this.dial();
        } catch (e) {
          if (this.closed) {
            this.connecting = false;
            return;
          }
          if (e instanceof UnauthorizedError) {
            this.connecting = false;
            this.stop(e);
            return;
          }
          this.log.debug?.("jetcast: connect failed", e);
          await this.sleep(delay + randInt(delay / 2));
          if (this.closed) {
            this.connecting = false;
            return;
          }
          delay = Math.min(2 * delay, 15_000);
          continue;
        }
        this.connecting = false;
        this.adopt(conn);
        return;
      }
    })();
  }

  /** Opens and greets a new connection. */
  private async dial(): Promise<Connection> {
    const token = await withTimeout(Promise.resolve(this.getToken()), 15_000, "jetcast: getToken timed out");
    if (this.closed) throw new Error("jetcast: client closed");
    const socket = newSocketId();
    const nc = await wsconnect({
      servers: this.servers,
      name: socket,
      token,
      inboxPrefix: `${connNamespace(this.prefix, socket)}.r`,
      reconnect: false,
      timeout: 15_000,
    });
    const conn = new Connection(nc, this.prefix, socket);
    void nc.closed().then(() => this.lost(conn));
    try {
      nc.subscribe(`${conn.ns}.ctl`, {
        callback: (err, m) => {
          if (!err) this.onControl(conn, m);
        },
      });
      nc.subscribe(`${conn.ns}.ev`, {
        callback: (err, m) => {
          if (!err) this.onRelayed(conn, m);
        },
      });
      const hello = await conn.request<HelloResponse>(conn.requestSubject("hello"), {});
      if (hello.error) throw new Error(`jetcast: hello: ${hello.error.code}`);
      if (this.closed) throw new Error("jetcast: client closed");
      conn.hello = hello;
      if (hello.expiresAt) {
        const exp = hello.expiresAt;
        conn.refreshAt = exp - Math.max((exp - Date.now()) / 10, 30_000);
      }
      return conn;
    } catch (e) {
      await nc.close().catch(() => {});
      throw e;
    }
  }

  /** Makes a new connection current and resubscribes every channel. */
  private adopt(conn: Connection): void {
    if (this.closed) {
      void conn.nc.close().catch(() => {});
      return;
    }
    const old = this.conn;
    this.conn = conn;
    // Events carry the digest of their origin; servers before 0.2, which may
    // still publish during a rolling upgrade, carry the socket ID itself.
    const now = Date.now();
    if (old) {
      this.origins.set(old.socket, now);
      if (old.hello.origin) this.origins.set(old.hello.origin, now);
    }
    this.origins.set(conn.socket, 0);
    if (conn.hello.origin) this.origins.set(conn.hello.origin, 0);
    this.setStatus("connected");
    this.readyD.resolve();
    for (const s of [...this.subs.values()]) s.resubscribe(conn);
    if (old) void old.nc.close().catch(() => {});
  }

  /** Handles the loss of a connection. */
  private lost(conn: Connection): void {
    if (this.conn === conn && !this.closed) this.startConnecting(false);
  }

  private onControl(conn: Connection, m: Msg): void {
    let ctl: Control;
    try {
      ctl = m.json<Control>();
    } catch {
      return;
    }
    if (this.conn !== conn) return;
    const s = ctl.channel ? this.subs.get(ctl.channel) : undefined;
    switch (ctl.type) {
      case "refresh":
        setTimeout(() => this.startConnecting(true), randInt(5000));
        break;
      case "disconnect":
        this.stop(new Error("jetcast: disconnected by server"));
        break;
      case "leave":
        s?.removed(conn, "", "leave");
        break;
      case "denied":
        s?.removed(conn, ctl.sid ?? "", "denied");
        break;
      case "interrupted":
        s?.resubscribe(conn, ctl.sid ?? "");
        break;
    }
  }

  private onRelayed(conn: Connection, m: Msg): void {
    this.subs.get(header(m, Header.Channel))?.onMessage(conn, m, true);
  }

  /** Drives credential refresh, relay renewal and heads checks. */
  private tick(): void {
    const conn = this.conn;
    if (!conn || conn.closed) return;
    const now = Date.now();
    if (now >= conn.refreshAt) this.startConnecting(true);
    // Events of replaced connections can only arrive while retained.
    const keep = (conn.hello.maxAgeMs ?? 0) + 60_000;
    for (const [origin, replaced] of this.origins) {
      if (replaced !== 0 && now - replaced > keep) this.origins.delete(origin);
    }
    const renew = renewPeriod(conn);
    if (now - this.lastRenew >= renew - renew / 10 + randInt(renew / 10)) {
      this.lastRenew = now;
      void this.renew(conn);
    }
    if (now - this.lastHeads >= this.headsInterval) {
      this.lastHeads = now;
      void this.heads(conn, now - this.headsInterval);
    }
  }

  /** Checks credentials and every channel at once, when a page becomes visible again. */
  private visible(): void {
    if (typeof document === "undefined" || document.visibilityState !== "visible") return;
    const conn = this.conn;
    if (!conn || conn.closed) return;
    if (Date.now() >= conn.refreshAt) this.startConnecting(true);
    this.lastHeads = Date.now();
    void this.heads(conn, Number.POSITIVE_INFINITY);
  }

  /** Runs a heads check of every channel soon, at most every 5 seconds. */
  private headsSoon(): void {
    const now = Date.now();
    if (now - this.lastHeadsSoon < 5000) return;
    this.lastHeadsSoon = now;
    setTimeout(() => {
      const conn = this.conn;
      if (conn && !conn.closed) void this.heads(conn, Number.POSITIVE_INFINITY);
    }, 0);
  }

  /** Renews relay leases, grouped by node. */
  private async renew(conn: Connection): Promise<void> {
    const byNode = new Map<string, [Channel, string][]>();
    for (const s of this.subs.values()) {
      const lease = s.relayLease(conn);
      if (!lease) continue;
      const [node, sid] = lease;
      const list = byNode.get(node) ?? [];
      list.push([s, sid]);
      byNode.set(node, list);
    }
    await Promise.all(
      [...byNode].map(async ([node, subs]) => {
        // One renewal per node at a time; leases run while it waits, so it
        // goes before other requests.
        if (conn.renewing.has(node)) return;
        conn.renewing.add(node);
        let resp: RenewResponse | undefined;
        let noResponders = false;
        try {
          resp = await conn.request<RenewResponse>(
            conn.nodeRequestSubject(node, "renew"),
            { sids: subs.map(([, sid]) => sid) },
            { urgent: true },
          );
        } catch (e) {
          noResponders = e instanceof NoRespondersError || (e instanceof RequestError && e.isNoResponders());
        } finally {
          conn.renewing.delete(node);
        }
        if (this.conn !== conn) return;
        if (!resp || resp.error) {
          // No responders means the node is gone, and denied that the
          // connection is no longer registered. Other failures, such as an
          // overloaded node, are retried by the next renewals until the node
          // must have dropped the relays; then they are rebuilt elsewhere.
          const now = Date.now();
          const last = conn.renewedAt.get(node) ?? now;
          conn.renewedAt.set(node, last);
          const gone = noResponders || resp?.error?.code === CodeDenied || now - last > renewGrace * renewPeriod(conn);
          if (!gone) return;
          conn.renewedAt.delete(node);
          for (const [s, sid] of subs) s.resubscribe(conn, sid);
          return;
        }
        conn.renewedAt.set(node, Date.now());
        const missing = new Set(resp.missing ?? []);
        for (const [s, sid] of subs) if (missing.has(sid)) s.resubscribe(conn, sid);
      }),
    );
  }

  /** Checks channels without events since idleSince for missed events. */
  private async heads(conn: Connection, idleSince: number): Promise<void> {
    const groups = new Map<string, Channel[]>(); // node, "" for any node
    for (const s of this.subs.values()) {
      const node = s.headsTarget(conn, idleSince);
      if (node === undefined) continue;
      const list = groups.get(node) ?? [];
      list.push(s);
      groups.set(node, list);
    }
    // Batches stay below the server's default limit of channels per request.
    const chunk = 100;
    const batches: [string, Channel[]][] = [];
    for (const [node, subs] of groups) {
      for (let i = 0; i < subs.length; i += chunk) batches.push([node, subs.slice(i, i + chunk)]);
    }
    await Promise.all(
      batches.map(async ([node, subs]) => {
        const epoch = subs[0].cursorEpoch;
        const subject = node === "" ? conn.requestSubject("heads") : conn.nodeRequestSubject(node, "heads");
        let resp: HeadsResponse;
        try {
          resp = await conn.request<HeadsResponse>(subject, { epoch, channels: subs.map((s) => s.key) });
        } catch {
          return;
        }
        if (resp.error) return;
        const denied = new Set(resp.denied ?? []);
        for (const s of subs) {
          const head = resp.heads?.[s.key];
          if (denied.has(s.key)) s.resubscribe(conn);
          else if (resp.epoch !== epoch || head === undefined) s.checkHead(conn, resp, 0, false);
          else s.checkHead(conn, resp, head, true);
        }
      }),
    );
  }
}

/** Rejects when promise does not settle within ms. */
function withTimeout<T>(promise: Promise<T>, ms: number, message: string): Promise<T> {
  let timer: ReturnType<typeof setTimeout> | undefined;
  const timeout = new Promise<never>((_, reject) => {
    timer = setTimeout(() => reject(new Error(message)), ms);
  });
  return Promise.race([promise, timeout]).finally(() => clearTimeout(timer));
}
