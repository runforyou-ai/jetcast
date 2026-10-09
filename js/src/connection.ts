// One NATS connection with its own socket ID and subject namespace.

import { createInbox, type NatsConnection } from "@nats-io/nats-core";
import type { HelloResponse } from "./protocol.js";

const encoder = new TextEncoder();

/** Encodes a value as JSON bytes. */
export function encodeJSON(v: unknown): Uint8Array {
  return encoder.encode(JSON.stringify(v));
}

/** Requests in flight when hello does not state the server's limit. */
const defaultMaxRequests = 8;

/** A connection and the subjects of its socket. */
export class Connection {
  hello: HelloResponse = {};
  /** When to switch to a fresh connection, in Unix milliseconds. */
  refreshAt = Number.POSITIVE_INFINITY;
  /** Consecutive failed renewals per node. */
  readonly renewFailures = new Map<string, number>();

  private closeFns = new Set<() => void>();
  private inflight = 0;
  private waiters: { resolve: () => void; reject: (e: Error) => void }[] = [];

  constructor(
    readonly nc: NatsConnection,
    readonly prefix: string,
    readonly socket: string,
  ) {
    void nc.closed().then(() => {
      for (const f of this.closeFns) f();
      this.closeFns.clear();
      const err = new Error("jetcast: connection closed");
      for (const w of this.waiters.splice(0)) w.reject(err);
    });
  }

  /** Jetcast-Origin value of events caused by this connection. */
  get origin(): string {
    // Servers before 0.2 tag events with the socket ID itself.
    return this.hello.origin || this.socket;
  }

  /** Waits for a request slot; the server answers requests beyond its limit with "overloaded". */
  acquire(): Promise<void> {
    if (this.nc.isClosed()) return Promise.reject(new Error("jetcast: connection closed"));
    const max = this.hello.maxRequests && this.hello.maxRequests > 0 ? this.hello.maxRequests : defaultMaxRequests;
    if (this.inflight < max) {
      this.inflight++;
      return Promise.resolve();
    }
    return new Promise((resolve, reject) => this.waiters.push({ resolve, reject }));
  }

  /** Releases a request slot, handing it to the next waiter. */
  release(): void {
    const next = this.waiters.shift();
    if (next) next.resolve();
    else this.inflight--;
  }

  /** Registers a function called once when the connection closes; returns a function that removes it. */
  onClose(f: () => void): () => void {
    if (this.nc.isClosed()) {
      queueMicrotask(f);
      return () => {};
    }
    this.closeFns.add(f);
    return () => this.closeFns.delete(f);
  }

  /** Namespace of this connection: <p>.c.<socket>. */
  get ns(): string {
    return connNamespace(this.prefix, this.socket);
  }

  /** Subject of a request any node handles. */
  requestSubject(op: string): string {
    return `${this.prefix}.rq.${this.socket}.${op}`;
  }

  /** Subject of a request to one node. */
  nodeRequestSubject(node: string, op: string): string {
    return `${this.prefix}.rq.${this.socket}.n.${node}.${op}`;
  }

  /** Event subject of a channel key ("<kind>.<name>"). */
  eventSubject(key: string): string {
    return `${this.prefix}.ev.${key}`;
  }

  /** A fresh reply subject inside this connection's reply namespace. */
  newInbox(): string {
    return createInbox(`${this.ns}.r`);
  }

  /** Sends a JSON request in a request slot and decodes the JSON response. The timeout starts once a slot is free. */
  async request<T>(subject: string, body: unknown, timeout = 10_000): Promise<T> {
    await this.acquire();
    try {
      const m = await this.nc.request(subject, encodeJSON(body), { timeout });
      return m.json<T>();
    } finally {
      this.release();
    }
  }

  /**
   * Sends a JSON message whose response nobody waits for. The server only
   * handles requests with a reply subject in the connection's namespace.
   */
  notify(subject: string, body: unknown): void {
    try {
      this.nc.publish(subject, encodeJSON(body), { reply: this.newInbox() });
    } catch {
      // The connection is closed; the server drops the state on its own.
    }
  }

  /** Reports whether the connection is closed. */
  get closed(): boolean {
    return this.nc.isClosed();
  }
}

/** Namespace of a connection: <p>.c.<socket>. */
export function connNamespace(prefix: string, socket: string): string {
  return `${prefix}.c.${socket}`;
}
