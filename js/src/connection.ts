// One NATS connection with its own socket ID and subject namespace.

import { createInbox, type NatsConnection } from "@nats-io/nats-core";
import type { HelloResponse } from "./protocol.js";

const encoder = new TextEncoder();

/** Encodes a value as JSON bytes. */
export function encodeJSON(v: unknown): Uint8Array {
  return encoder.encode(JSON.stringify(v));
}

/** A connection and the subjects of its socket. */
export class Connection {
  hello: HelloResponse = {};
  /** When to switch to a fresh connection, in Unix milliseconds. */
  refreshAt = Number.POSITIVE_INFINITY;

  private closeFns = new Set<() => void>();

  constructor(
    readonly nc: NatsConnection,
    readonly prefix: string,
    readonly socket: string,
  ) {
    void nc.closed().then(() => {
      for (const f of this.closeFns) f();
      this.closeFns.clear();
    });
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

  /** Sends a JSON request and decodes the JSON response. */
  async request<T>(subject: string, body: unknown, timeout = 10_000): Promise<T> {
    const m = await this.nc.request(subject, encodeJSON(body), { timeout });
    return m.json<T>();
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
