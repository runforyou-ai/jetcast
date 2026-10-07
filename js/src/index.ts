// Public entry point of the jetcast TypeScript SDK.

export { connect, Echo, UnauthorizedError } from "./client.js";
export type { ConnectOptions, Logger, Status } from "./client.js";
export { Channel } from "./subscription.js";
export type { ChannelState, ChannelStateName, EventMeta, Listener } from "./subscription.js";
export type { Reason } from "./protocol.js";
export { matchPattern } from "./util.js";
