# @runforyou/jetcast

TypeScript client for jetcast: Laravel Broadcasting and Echo
on NATS. Browsers connect directly to NATS over WebSocket and receive events
that the server broadcasts on channels. Missed events are detected and
recovered from a short JetStream history; when recovery is impossible the
client says so, and the application reloads its data.

```sh
npm install @runforyou/jetcast
```

## Usage

```ts
import { connect, UnauthorizedError } from "@runforyou/jetcast";

const echo = await connect({
  servers: "wss://example.com/nats",
  getToken: async () => {
    const r = await fetch("/api/realtime-token", { method: "POST" });
    if (r.status === 401) throw new UnauthorizedError();
    if (!r.ok) throw new Error(`token: ${r.status}`); // retried with backoff
    return (await r.json()).token;
  },
});

echo.channel("news").listen("article.published", (data, meta) => {
  console.log(meta.event, meta.sequence, data);
});

const orders = echo
  .private("orders.42")
  .listen("order.shipped", (data) => render(data));

orders.onState(({ state, recovered, reason }) => {
  if (state === "subscribed" && recovered === false && reason !== "initial") {
    reloadOrder(); // events may have been missed
  }
});

await orders.ready(); // first subscribed; rejects if denied

// Exclude this client from broadcasts it causes (toOthers).
await fetch("/api/orders/42/ship", {
  method: "POST",
  headers: { "X-Socket-ID": echo.socketId },
});

orders.leave();
await echo.close();
```

## API

### `connect(options): Promise<Echo>`

Resolves once the first connection is established; rejects when `getToken`
throws `UnauthorizedError` first.

| Option | Description |
|---|---|
| `servers` | NATS WebSocket URL or URLs (`ws://` or `wss://`). |
| `getToken` | Returns the credential for a new connection. Called for every connection, including reconnections and credential refreshes. |
| `prefix` | Subject prefix, `"jetcast"` by default. Must match the server's `Config.Prefix`. |
| `headsInterval` | How often idle channels are checked for missed events, in milliseconds; 30000 by default. |
| `logger` | Object with optional `debug` and `warn` methods. Warnings go to `console.warn` by default; pass `{}` to silence them. |

### `Echo`

| Member | Description |
|---|---|
| `channel(name)` | Public channel. The same name returns the same `Channel`. |
| `private(name)` | Private channel. The same name returns the same `Channel`. |
| `socketId` | Socket ID of the current connection. It changes whenever the client switches connections, so read it for every request. |
| `status` | `"connecting"`, `"connected"`, `"reconnecting"` or `"stopped"`. |
| `onStatus(cb)` | Calls `cb` on status changes; returns a function that removes it. |
| `info` | User info the server returned for the current connection. |
| `close()` | Leaves every channel and closes the connection. |

Channel names are one to eight dot-separated tokens of ASCII letters, digits,
`-` or `_`, at most 200 bytes. Wildcards are not allowed.

### `Channel`

| Member | Description |
|---|---|
| `listen(event, cb)` | Calls `cb(data, meta)` for events named `event`. Returns the channel. |
| `listenAll(cb)` | Calls `cb` for every event. |
| `stopListening(event, cb?)` | Removes one listener, or every listener of `event`. |
| `stopListeningAll(cb?)` | Removes `listenAll` listeners. |
| `ready()` | Resolves when the channel is first subscribed; rejects when it is denied or left first. |
| `onState(cb)` | Calls `cb({ state, recovered, reason })` on state changes; returns a function that removes it. |
| `state` | Current state. |
| `leave()` | Unsubscribes. The next `channel()` or `private()` call creates a new subscription. |

`data` is the JSON-decoded payload, or the raw string when it is not JSON.
`meta` is `{ event, channel, id, sequence, time, raw }`: `sequence` is the
stream sequence (0 on ephemeral channels), `time` when the event was stored,
and `raw` the payload bytes.

Listener errors are caught and logged; they do not affect the client.
Listeners run asynchronously, in order, after the client has updated its state.

### States

| State | Meaning |
|---|---|
| `subscribing` | First subscription in progress. |
| `subscribed` | Receiving events. On entering it, `recovered` tells whether every event since the previous subscription was delivered. |
| `interrupted` | The connection or relay was lost; the client resubscribes. |
| `recovering` | Fetching missed events before resuming. |
| `denied` | Authorization failed (`reason: "denied"`), or the server removed the subscription (`reason: "leave"`). Final. |
| `left` | `leave()` was called. Final. |

`reason` with `recovered: false`:

| Reason | Meaning |
|---|---|
| `initial` | First subscription; nothing to recover. |
| `expired` | Missed events are no longer retained. |
| `epoch` | The event stream was recreated. |
| `too_far` | Too many missed events to recover. |
| `ephemeral` | The channel is not retained, so gaps cannot be detected. |

Load your initial data, then treat every `subscribed` state with
`recovered: false` and a reason other than `initial` and `ephemeral` as a
signal to reload. jetcast sequences only prove transport continuity; use your
own versions to join snapshots and events.

## Credentials

- Every connection uses a fresh socket ID and a credential from `getToken()`.
- Before the connection's credentials expire (at 10% of the lifetime left, at
  least 30 seconds before), the client calls `getToken()`, opens a new
  connection, moves every channel to it and closes the old one. The status
  stays `connected` during the switch.
- The server's `Refresh` makes clients switch the same way after a random
  delay of up to 5 seconds, picking up newly granted channels.
- When a connection drops, the status becomes `reconnecting` and the client
  reconnects with jittered exponential backoff (250 ms doubling to 15 s).
- If `getToken()` throws `UnauthorizedError`, the client stops: the status
  becomes `stopped` and it never reconnects. Throw it when the user is logged
  out. Any other error is retried with backoff.
- The server's `Disconnect` also stops the client.

## toOthers

Broadcasts can name the socket that caused them, like Laravel's `toOthers()`.
Send `echo.socketId` with your API requests, conventionally in the
`X-Socket-ID` header, and pass it to the broadcast (`jetcast.SocketID(r)` on
the Go side). The client still receives such events and advances its cursor,
but does not call its listeners. This also holds for sockets it used before a
reconnection. It is not a security mechanism.

## Browsers and Node.js

- Browsers: works with any bundler. When the page becomes visible again, the
  client checks credential expiry and every channel for missed events at once.
- Node.js 22 or later, which provides a global `WebSocket`. For non-browser
  clients, the Go client (`github.com/runforyou-ai/jetcast/client`) is also
  available.
- The server must allow WebSocket connections (`User.ConnectionTypes`).

## Development

```sh
npm install
npm run build   # dist/
npm test        # unit and integration tests; needs Go to run cmd/jetcast-dev
```

The integration tests build `cmd/jetcast-dev` from the repository root and
drive it through its HTTP control API.
