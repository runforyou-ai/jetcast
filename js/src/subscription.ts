// A client's subscription to one channel: delivery path, gap detection and
// recovery. Mirrors client/subscription.go.

import { nuid, type Msg, type Subscription as NatsSubscription } from "@nats-io/nats-core";
import type { Connection } from "./connection.js";
import {
  CodeDenied,
  Header,
  PathDirect,
  PathRelay,
  StatusDone,
  type HeadsResponse,
  type Reason,
  type RecoverRequest,
  type RecoverResult,
  type SubResponse,
} from "./protocol.js";
import { decodePayload, deferred, header, matchPattern, parseSeq, parseTime, randInt } from "./util.js";

/** Subscription states. */
export type ChannelStateName =
  | "subscribing"
  | "recovering"
  | "subscribed"
  | "interrupted"
  | "denied"
  | "left";

/**
 * A subscription state change. `recovered` and `reason` are set when entering
 * "subscribed": `recovered` is false when events may have been missed
 * (`reason` says why), and the application should reload its data. `reason`
 * is also set on "denied" when the server removed the subscription ("leave"
 * or "denied").
 */
export interface ChannelState {
  state: ChannelStateName;
  recovered?: boolean;
  reason?: Reason | "leave" | "denied" | (string & {});
  error?: Error;
}

/** Metadata of a received event. */
export interface EventMeta {
  /** Event name. */
  event: string;
  /** Channel name, without the kind. */
  channel: string;
  /** Event ID set by the broadcaster. */
  id: string;
  /** Stream sequence of the event, 0 for ephemeral channels. */
  sequence: number;
  /** When the event was stored, if known. */
  time?: Date;
  /** Raw payload bytes. */
  raw: Uint8Array;
}

/** Receives an event: `data` is the JSON-decoded payload, or the raw string when it is not JSON. */
export type Listener = (data: any, meta: EventMeta) => void;

/** What a subscription needs from its client. */
export interface SubscriptionHost {
  /** Queues an application callback; callbacks run one at a time in order. */
  call(f: () => void): void;
  /** Reports whether an event origin is one of this client's connections. */
  ownOrigin(origin: string): boolean;
  /** Removes a subscription from the client. */
  forget(s: Channel): void;
  /** Returns the current connection. */
  current(): Connection | null;
  /** Asks for a heads check of every channel soon (throttled). */
  headsSoon(): void;
}

// Recovery limits of one resumption, beyond which the client gives up and
// reports too_far.
const maxRecoverEvents = 10_000;
const maxRecoverBytes = 16 << 20;
const maxBuffered = 1000;

/** A subscription to one channel. Obtain it from `echo.channel()` or `echo.private()`. */
export class Channel {
  /** "<kind>.<name>", as used on the wire. */
  readonly key: string;

  private listeners = new Map<string, Listener[]>();
  private all: Listener[] = [];
  private stateFns: ((s: ChannelState) => void)[] = [];
  private stateName: ChannelStateName = "subscribing";
  private readyDone = false;
  private readyD = deferred<void>();

  // Current attempt.
  private gen = 0;
  private conn: Connection | null = null;
  private sid = "";
  private path = "";
  private node = "";
  private direct: NatsSubscription | null = null;
  private recoverable = false;
  private recovering = false;
  private buffer: Msg[] = [];
  private lastEventAt = 0;
  /** Consecutive failed attempts, for backoff. */
  private failures = 0;
  /** Last successful renewal or start of the relay. */
  private leaseAt = 0;

  // Cursor.
  private hasCursor = false;
  private resetReason: Reason | "" = "";
  private epoch = "";
  /** Every event of the channel up to pos was delivered. */
  private pos = 0;
  /** Sequence of the last delivered event, 0 if none. */
  private last = 0;

  /** @internal */
  constructor(
    private readonly host: SubscriptionHost,
    readonly kind: "pub" | "prv",
    readonly name: string,
  ) {
    this.key = `${kind}.${name}`;
    // ready() may never be awaited; its rejection must not be unhandled.
    this.readyD.promise.catch(() => {});
  }

  /** Current state. */
  get state(): ChannelStateName {
    return this.stateName;
  }

  /** Registers a listener for one event name. */
  listen(event: string, cb: Listener): this {
    const list = this.listeners.get(event) ?? [];
    list.push(cb);
    this.listeners.set(event, list);
    return this;
  }

  /** Registers a listener for every event. */
  listenAll(cb: Listener): this {
    this.all.push(cb);
    return this;
  }

  /** Removes a listener of an event, or all its listeners when cb is omitted. */
  stopListening(event: string, cb?: Listener): this {
    if (!cb) {
      this.listeners.delete(event);
      return this;
    }
    const list = this.listeners.get(event)?.filter((f) => f !== cb);
    if (list?.length) this.listeners.set(event, list);
    else this.listeners.delete(event);
    return this;
  }

  /** Removes a listenAll listener, or all of them when cb is omitted. */
  stopListeningAll(cb?: Listener): this {
    this.all = cb ? this.all.filter((f) => f !== cb) : [];
    return this;
  }

  /** Registers a state listener and returns a function that removes it. */
  onState(cb: (s: ChannelState) => void): () => void {
    this.stateFns.push(cb);
    return () => {
      this.stateFns = this.stateFns.filter((f) => f !== cb);
    };
  }

  /** Resolves when the channel is first subscribed; rejects when it is denied or left first. */
  ready(): Promise<void> {
    return this.readyD.promise;
  }

  /** Unsubscribes. */
  leave(): void {
    this.host.forget(this);
    const conn = this.conn;
    this.gen++;
    this.teardown();
    this.setState({ state: "left" });
    this.sendLeave(conn);
  }

  /** Releases the relay of the current attempt, if any. @internal */
  sendLeave(conn: Connection | null): void {
    if (!conn || this.path !== PathRelay) return;
    conn.notify(conn.nodeRequestSubject(this.node, "leave"), { sid: this.sid });
  }

  /** Ends the channel when the client closes: it enters "left", and `ready()` rejects if it was never subscribed. @internal */
  closed(): void {
    this.gen++;
    this.teardown();
    this.setState({ state: "left", error: new Error("jetcast: client closed") });
  }

  /** Fails the subscription before it starts. @internal */
  fail(error: Error): void {
    this.setState({ state: "denied", error });
  }

  private setState(st: ChannelState): void {
    this.stateName = st.state;
    for (const f of this.stateFns) this.host.call(() => f(st));
    if (!this.readyDone && (st.state === "subscribed" || st.state === "denied" || st.state === "left")) {
      this.readyDone = true;
      // Settled after the state listeners run.
      const d = this.readyD;
      const err = st.error ?? new Error(`jetcast: channel ${this.key} ${st.reason ?? st.state}`);
      this.host.call(() => (st.state === "subscribed" ? d.resolve() : d.reject(err)));
    }
  }

  /** Drops the direct subscription and pending messages. */
  private teardown(): void {
    if (this.direct) {
      this.direct.unsubscribe();
      this.direct = null;
    }
    this.buffer = [];
    this.recovering = false;
  }

  /**
   * Starts a new subscription attempt on conn. A non-empty sid restricts it
   * to the attempt with that sid, ignoring stale signals. @internal
   */
  resubscribe(conn: Connection, sid = ""): void {
    if (this.stateName === "denied" || this.stateName === "left" || (sid !== "" && sid !== this.sid)) return;
    if (this.conn === conn && this.path === PathRelay) this.sendLeave(conn);
    const gen = ++this.gen;
    this.teardown();
    this.conn = conn;
    const newSid = nuid.next();
    this.sid = newSid;
    this.path = "";
    this.node = "";
    if (this.stateName === "subscribed" || this.stateName === "recovering") {
      this.setState({ state: "interrupted" });
    }
    void this.attempt(conn, gen, newSid);
  }

  /** Schedules a new attempt after a failure, backing off exponentially from one second up to 30 seconds. */
  private retryLater(conn: Connection, gen: number): void {
    const base = 1000 * 2 ** Math.min(this.failures, 5);
    this.failures++;
    setTimeout(
      () => {
        if (this.gen === gen && this.host.current() === conn) this.resubscribe(conn, this.sid);
      },
      Math.min(base + randInt(base), 30_000),
    );
  }

  /** Subscribes on conn with the sid bound to generation gen: sets up delivery, then asks the server. */
  private async attempt(conn: Connection, gen: number, sid: string): Promise<void> {
    let path = PathRelay;
    if (this.kind === "pub" || (conn.hello.grants ?? []).some((p) => matchPattern(p, this.name))) {
      path = PathDirect;
    }
    if (path === PathDirect) {
      let sub: NatsSubscription | undefined;
      try {
        sub = conn.nc.subscribe(conn.eventSubject(this.key), {
          callback: (err, m) => {
            if (!err && this.gen === gen) this.onMessage(conn, m, false);
          },
        });
        await conn.nc.flush();
      } catch {
        sub?.unsubscribe();
        if (this.gen === gen) this.retryLater(conn, gen);
        return;
      }
      if (this.gen !== gen) {
        sub.unsubscribe();
        return;
      }
      this.direct = sub;
    }

    if (this.gen !== gen) return;
    let resp: SubResponse | undefined;
    let failed = false;
    try {
      resp = await conn.request<SubResponse>(
        conn.requestSubject("sub"),
        { channel: this.key, sid, path },
        { ready: () => this.gen === gen },
      );
    } catch {
      failed = true;
    }

    if (this.gen !== gen) {
      if (resp && !resp.error && resp.path === PathRelay && resp.node) {
        conn.notify(conn.nodeRequestSubject(resp.node, "leave"), { sid });
      }
      return;
    }
    if (failed || !resp || (resp.error && resp.error.code !== CodeDenied)) {
      this.teardown();
      this.retryLater(conn, gen);
      return;
    }
    if (resp.error) {
      this.gen++;
      this.teardown();
      this.setState({ state: "denied", reason: "denied", error: new Error(`jetcast: channel ${this.key} denied`) });
      this.host.forget(this);
      return;
    }
    if (resp.path === PathRelay && this.direct) {
      this.direct.unsubscribe();
      this.direct = null;
    }
    this.path = resp.path ?? "";
    this.node = resp.node ?? "";
    this.recoverable = resp.recoverable;
    this.failures = 0;
    if (resp.path === PathRelay) this.leaseAt = Date.now();
    if (!resp.recoverable) {
      this.setState({ state: "subscribed", recovered: false, reason: "ephemeral" });
      this.drain();
      return;
    }
    const epoch = resp.epoch ?? "";
    if (!this.hasCursor || this.epoch !== epoch) {
      let reason: Reason = "initial";
      if (this.resetReason !== "") {
        reason = this.resetReason;
        this.resetReason = "";
      } else if (this.hasCursor) {
        reason = "epoch";
      }
      this.hasCursor = true;
      this.epoch = epoch;
      this.pos = resp.position;
      this.last = resp.head;
      this.lastEventAt = Date.now();
      this.setState({ state: "subscribed", recovered: false, reason });
      this.drain();
      return;
    }
    this.startRecovery(conn, 0);
  }

  /** Handles a delivered event, direct or relayed. @internal */
  onMessage(conn: Connection, m: Msg, relayed: boolean): void {
    if (this.conn !== conn || (relayed && header(m, Header.Sid) !== this.sid)) return;
    if (this.stateName !== "subscribed" || this.recovering) {
      if (this.buffer.length < maxBuffered) this.buffer.push(m);
      else if (this.recovering) this.giveUp(conn, "too_far");
      return;
    }
    this.live(conn, m);
  }

  /** Processes buffered messages in order. */
  private drain(): void {
    const buf = this.buffer;
    this.buffer = [];
    for (let i = 0; i < buf.length; i++) {
      if (this.recovering) {
        this.buffer.push(...buf.slice(i));
        return;
      }
      this.live(this.conn!, buf[i]);
    }
  }

  /** Applies the gap rules to a live event. */
  private live(conn: Connection, m: Msg): void {
    this.lastEventAt = Date.now();
    if (!this.recoverable) {
      this.deliver(m, 0);
      return;
    }
    const seq = parseSeq(header(m, Header.Sequence));
    const prev = parseSeq(header(m, Header.LastSequence));
    if (seq === 0) {
      this.deliver(m, 0);
    } else if (seq <= this.pos) {
      // Duplicate. A sequence below the last delivered one hints at a
      // recreated stream, which a heads check confirms.
      if (seq < this.last) this.host.headsSoon();
    } else if ((this.last !== 0 && prev === this.last) || this.pos + 1 >= seq) {
      this.deliver(m, seq);
      this.pos = this.last = seq;
    } else {
      this.buffer.unshift(m);
      this.startRecovery(conn, seq);
    }
  }

  /** Hands an event to the listeners, except for events this client caused itself. */
  private deliver(m: Msg, seq: number): void {
    const origin = header(m, Header.Origin);
    if (origin !== "" && this.host.ownOrigin(origin)) return;
    const event = header(m, Header.Event);
    const meta: EventMeta = {
      event,
      channel: this.name,
      id: header(m, Header.ID),
      sequence: seq,
      time: parseTime(header(m, Header.TimeStamp)),
      raw: m.data,
    };
    const handlers = [...(this.listeners.get(event) ?? []), ...this.all];
    if (handlers.length === 0) return;
    const data = decodePayload(m.data);
    for (const h of handlers) this.host.call(() => h(data, meta));
  }

  /** Abandons a recovery that cannot keep up and starts over with a fresh cursor. */
  private giveUp(conn: Connection, reason: Reason): void {
    this.recovering = false;
    this.buffer = [];
    this.hasCursor = false;
    this.resetReason = reason;
    const sid = this.sid;
    queueMicrotask(() => this.resubscribe(conn, sid));
  }

  /** Recovers events after the cursor and before upTo (0 for the end of the stream), then processes buffered events. */
  private startRecovery(conn: Connection, upTo: number): void {
    this.recovering = true;
    if (this.stateName !== "subscribed") this.setState({ state: "recovering" });
    void this.recoverLoop(conn, this.gen, this.epoch, this.pos, upTo);
  }

  private async recoverLoop(conn: Connection, gen: number, epoch: string, pos: number, upTo: number): Promise<void> {
    let events = 0;
    let bytes = 0;
    let broken = 0;
    for (;;) {
      const req: RecoverRequest = { channel: this.key, epoch, pos };
      if (upTo > 0) req.upTo = upTo;
      let res: RecoverResult | undefined;
      let msgs: Msg[] = [];
      try {
        [res, msgs] = await this.recoverBatch(conn, gen, req);
      } catch {
        res = undefined;
      }
      if (this.gen !== gen) return;
      if (!res || res.error) {
        if (res?.error?.code === CodeDenied) {
          this.removed(conn, "", "denied");
          return;
        }
        this.recovering = false;
        this.retryLater(conn, gen);
        return;
      }
      if (!res.recovered) {
        this.reset(res.epoch ?? "", res.position, res.head, res.reason ?? "expired");
        return;
      }
      if (!completeBatch(msgs, res.count, pos)) {
        // Events of the batch were lost on the way: retry from the same
        // position rather than skipping them.
        if (++broken > 3) {
          this.recovering = false;
          this.retryLater(conn, gen);
          return;
        }
        continue;
      }
      for (const m of msgs) {
        const seq = parseSeq(header(m, Header.Sequence));
        if (seq > this.pos) {
          this.deliver(m, seq);
          this.pos = this.last = seq;
          events++;
          bytes += m.data.length;
        }
      }
      if (res.next > this.pos) this.pos = res.next;
      pos = this.pos;
      if (events > maxRecoverEvents || bytes > maxRecoverBytes) {
        this.reset(res.epoch ?? "", res.position, 0, "too_far");
        return;
      }
      if (!res.more) {
        this.recovering = false;
        this.lastEventAt = Date.now();
        if (this.stateName !== "subscribed") this.setState({ state: "subscribed", recovered: true });
        this.drain();
        return;
      }
    }
  }

  /** Restarts the cursor at the stream's current position after a failed recovery and reports recovered=false. */
  private reset(epoch: string, position: number, head: number, reason: Reason): void {
    if (epoch !== "") this.epoch = epoch;
    this.pos = position;
    this.last = head;
    this.recovering = false;
    this.lastEventAt = Date.now();
    this.setState({ state: "subscribed", recovered: false, reason });
    this.drain();
  }

  /** Requests one recovery batch in a request slot and collects its events until the done message. */
  private async recoverBatch(conn: Connection, gen: number, req: RecoverRequest): Promise<[RecoverResult, Msg[]]> {
    await conn.acquire();
    try {
      if (this.gen !== gen) throw new Error("jetcast: request no longer needed");
      return await this.recoverInSlot(conn, req);
    } finally {
      conn.release();
    }
  }

  /** Sends one recovery request and collects its events until the done message. */
  private async recoverInSlot(conn: Connection, req: RecoverRequest): Promise<[RecoverResult, Msg[]]> {
    const d = deferred<[RecoverResult, Msg[]]>();
    const msgs: Msg[] = [];
    const inbox = conn.newInbox();
    const sub = conn.nc.subscribe(inbox, {
      callback: (err, m) => {
        if (err) {
          d.reject(err);
        } else if (header(m, Header.Status) === StatusDone) {
          try {
            d.resolve([m.json<RecoverResult>(), msgs]);
          } catch (e) {
            d.reject(e);
          }
        } else {
          msgs.push(m);
        }
      },
    });
    const timer = setTimeout(() => d.reject(new Error("jetcast: recover timed out")), 15_000);
    const offClose = conn.onClose(() => d.reject(new Error("jetcast: connection closed")));
    try {
      conn.nc.publish(conn.requestSubject("recover"), JSON.stringify(req), { reply: inbox });
    } catch (e) {
      d.reject(e);
    }
    try {
      return await d.promise;
    } finally {
      clearTimeout(timer);
      offClose();
      sub.unsubscribe();
    }
  }

  /** Ends the subscription after the server denied or removed it. @internal */
  removed(conn: Connection, sid: string, reason: "leave" | "denied"): void {
    if (this.conn !== conn || (sid !== "" && sid !== this.sid)) return;
    this.gen++;
    this.teardown();
    this.setState({ state: "denied", reason, error: new Error(`jetcast: channel ${this.key} ${reason}`) });
    this.host.forget(this);
  }

  /** Records a successful renewal of the relay with sid, sent at `at`. @internal */
  leaseRenewed(sid: string, at: number): void {
    if (this.sid === sid && at > this.leaseAt) this.leaseAt = at;
  }

  /** Reports whether the relay with sid was not renewed for longer than ms, so its node must have dropped it. @internal */
  leaseExpired(sid: string, ms: number): boolean {
    return this.sid === sid && Date.now() - this.leaseAt > ms;
  }

  /** Returns the node and sid of a relayed subscription on conn. @internal */
  relayLease(conn: Connection): [string, string] | undefined {
    if (
      this.conn !== conn ||
      this.path !== PathRelay ||
      (this.stateName !== "subscribed" && this.stateName !== "recovering")
    ) {
      return undefined;
    }
    return [this.node, this.sid];
  }

  /**
   * Reports whether the subscription needs a heads check and which node
   * answers it ("" for any node). @internal
   */
  headsTarget(conn: Connection, idleSince: number): string | undefined {
    if (
      this.conn !== conn ||
      this.stateName !== "subscribed" ||
      !this.recoverable ||
      this.recovering ||
      this.lastEventAt > idleSince
    ) {
      return undefined;
    }
    return this.path === PathRelay ? this.node : "";
  }

  /** Epoch of the cursor. @internal */
  get cursorEpoch(): string {
    return this.epoch;
  }

  /** Current sid, for tests and diagnostics. @internal */
  get currentSid(): string {
    return this.sid;
  }

  /** Applies a heads response to the cursor. @internal */
  checkHead(conn: Connection, resp: HeadsResponse, head: number, ok: boolean): void {
    if (this.conn !== conn || this.stateName !== "subscribed" || this.recovering) return;
    if (resp.epoch !== this.epoch) {
      this.startRecovery(conn, 0);
    } else if (!ok) {
      // Unknown head: nothing to check.
    } else if (head > this.last) {
      this.startRecovery(conn, 0);
    } else if (resp.first <= this.pos + 1) {
      if (resp.last > this.pos) this.pos = resp.last;
    } else {
      this.reset(resp.epoch ?? "", resp.last, head, "expired");
    }
  }
}

/**
 * Reports whether a recovery batch arrived intact: as many events as the
 * server sent, each continuing the previous one from pos.
 */
export function completeBatch(msgs: Msg[], count: number, pos: number): boolean {
  if (msgs.length !== count) return false;
  let prev = pos;
  for (const m of msgs) {
    if (parseSeq(header(m, Header.LastSequence)) !== prev) return false;
    prev = parseSeq(header(m, Header.Sequence));
  }
  return true;
}
