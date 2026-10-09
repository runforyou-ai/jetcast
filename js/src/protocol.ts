// Wire protocol shared with the jetcast Go server. Mirrors protocol.go.

/** Message headers set by jetcast and JetStream. */
export const Header = {
  Event: "Jetcast-Event",
  ID: "Jetcast-Id",
  Origin: "Jetcast-Origin",
  Channel: "Jetcast-Channel",
  Sid: "Jetcast-Sid",
  Status: "Jetcast-Status",
  Sequence: "Nats-Sequence",
  LastSequence: "Nats-Last-Sequence",
  TimeStamp: "Nats-Time-Stamp",
} as const;

/** Value of the Jetcast-Status header that ends a recovery batch. */
export const StatusDone = "done";

/** Delivery paths of a channel. */
export const PathDirect = "direct";
export const PathRelay = "relay";

/** Error codes returned by the server. */
export const CodeDenied = "denied";
export const CodeInvalid = "invalid";
export const CodeUnavailable = "unavailable";
export const CodeOverloaded = "overloaded";

/** Reasons reported with `recovered: false`. */
export type Reason = "initial" | "expired" | "epoch" | "too_far" | "ephemeral";

/** Control message types sent to a connection. */
export type ControlType = "refresh" | "disconnect" | "leave" | "denied" | "interrupted";

export interface ErrorBody {
  code: string;
  message?: string;
}

export interface HelloResponse {
  error?: ErrorBody;
  user?: string;
  info?: unknown;
  grants?: string[];
  /** Unix milliseconds. */
  expiresAt?: number;
  epoch?: string;
  maxAgeMs?: number;
  renewMs?: number;
  node?: string;
  prefix?: string;
  /** Jetcast-Origin value of events caused by this connection. */
  origin?: string;
  /** Requests the connection may have in flight. */
  maxRequests?: number;
}

export interface SubRequest {
  /** "<kind>.<name>" */
  channel: string;
  sid: string;
  path?: string;
}

export interface SubResponse {
  error?: ErrorBody;
  path?: string;
  node?: string;
  epoch?: string;
  recoverable: boolean;
  /** Latest event sequence of the channel, 0 if none. */
  head: number;
  /** Last sequence of the stream. */
  position: number;
}

export interface HeadsRequest {
  epoch: string;
  channels: string[];
}

export interface HeadsResponse {
  error?: ErrorBody;
  epoch?: string;
  first: number;
  last: number;
  heads?: Record<string, number>;
  denied?: string[];
}

export interface RecoverRequest {
  channel: string;
  epoch: string;
  pos: number;
  /** Zero means up to the end of the stream. */
  upTo?: number;
}

export interface RecoverResult {
  error?: ErrorBody;
  recovered: boolean;
  reason?: Reason;
  more?: boolean;
  next: number;
  count: number;
  epoch?: string;
  head: number;
  position: number;
}

export interface RenewRequest {
  sids: string[];
}

export interface RenewResponse {
  error?: ErrorBody;
  missing?: string[];
}

export interface LeaveRequest {
  sid: string;
}

export interface Control {
  type: ControlType;
  sid?: string;
  channel?: string;
  reason?: string;
}
