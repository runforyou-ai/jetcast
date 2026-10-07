# jetcast

[![CI](https://github.com/runforyou-ai/jetcast/actions/workflows/ci.yml/badge.svg)](https://github.com/runforyou-ai/jetcast/actions/workflows/ci.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/runforyou-ai/jetcast.svg)](https://pkg.go.dev/github.com/runforyou-ai/jetcast)

[中文](README.zh-CN.md)

jetcast pushes server events to browsers that connect **directly to NATS** over WebSocket.
If you know Laravel Broadcasting and Echo, you already know jetcast: the server broadcasts
events on channels, clients listen on channels, and private channels are authorized by
callbacks you write.

- **Browsers on NATS, authorization enforced by NATS.** Clients authenticate through the
  NATS auth callout with your own tokens; every connection gets a JWT that allows exactly
  its channels and its own namespace.
- **Laravel-style channels.** `srv.Channel("orders.{id}", authorize)` decides at subscribe
  time, like `routes/channels.php`. Stable, broad grants (a user's own feed, a team's
  channels) can be given at connect time instead, so NATS routes them without touching
  your servers.
- **Missed events are recovered.** Events are retained in a JetStream stream for a few
  minutes. Clients detect gaps, including a lost last event, and fetch what they missed
  after a reconnect. When recovery is impossible they tell you, so you reload instead of
  silently showing stale data.
- **`toOthers`, revocation and refresh.** Exclude the sender of an action, revoke a user's
  or session's connections (kicking clients that do not cooperate), and let clients pick up
  new grants.
- **No assumptions about deployment.** The library takes a `*nats.Conn`. NATS can be
  embedded in your binary, standalone or clustered; WebSocket can be proxied by your Go
  server, nginx or a load balancer.

Requires nats-server **2.14.4+** (2.15 recommended) with JetStream, and Go 1.26+.

## Install

```sh
go get github.com/runforyou-ai/jetcast
npm install @runforyou/jetcast
```

## Server

```go
srv, err := jetcast.NewServer(nc, jetcast.ServerOptions{
	Account:       "APP",        // the NATS account clients are placed in
	CalloutSigner: issuerKey,    // account key pair configured as auth_callout issuer
	Admin:         jetcast.SystemAdmin(sysConn), // optional: kick clients on Disconnect
	ManageStreams: true,         // create the event stream and registry bucket
})

// Who is connecting? Use your session tokens.
srv.Authenticate(func(ctx context.Context, r jetcast.AuthRequest) (jetcast.User, error) {
	sess, err := sessions.Find(ctx, r.Token)
	if err != nil {
		return jetcast.User{}, err
	}
	return jetcast.User{ID: "u" + sess.UserID, Session: sess.ID, ExpiresAt: sess.ExpiresAt,
		Info: map[string]any{"name": sess.UserName}}, nil
})

// Optional: channels a user may subscribe to directly for the whole connection.
srv.Grants(func(ctx context.Context, u jetcast.User) ([]string, error) {
	return []string{"users." + u.ID + ".>"}, nil
})

// Private channels authorized at subscribe time, like routes/channels.php.
srv.Channel("orders.{id}", func(ctx context.Context, u jetcast.User, p jetcast.Params) (bool, error) {
	return orders.CanView(ctx, u.ID, p["id"])
})

err = srv.Start(ctx)
defer srv.Close()
```

Broadcast from any process that shares the configuration (a `Server` embeds a `Publisher`):

```go
pub, _ := jetcast.NewPublisher(nc, jetcast.Config{})
_, err := pub.Broadcast(ctx, jetcast.Event{
	Name:     "order.shipped",
	Channels: []jetcast.Channel{jetcast.Private("orders.42")},
	Data:     order,                  // JSON-encoded
	Origin:   jetcast.SocketID(r),    // toOthers: the X-Socket-ID of the request
})

// Or with an event type, like ShouldBroadcast:
type OrderShipped struct{ ID int `json:"id"` }
func (e OrderShipped) BroadcastOn() []jetcast.Channel {
	return []jetcast.Channel{jetcast.Private(fmt.Sprintf("orders.%d", e.ID))}
}
func (OrderShipped) BroadcastAs() string { return "order.shipped" }

pub.Dispatch(ctx, OrderShipped{ID: 42}, jetcast.ToOthers(jetcast.SocketID(r)))
```

Revocation and refresh:

```go
srv.Refresh(ctx, jetcast.ByUser("u42"))                 // picked up new grants
srv.Leave(ctx, jetcast.Private("orders.42"), jetcast.ByUser("u42"))
srv.Disconnect(ctx, jetcast.BySession("u42", sessionID)) // after invalidating the session
```

## Browser

```ts
import { connect, UnauthorizedError } from "@runforyou/jetcast"

const echo = await connect({
  servers: "wss://example.com/nats",
  getToken: async () => {
    const res = await fetch("/api/realtime-token")
    if (res.status === 401) throw new UnauthorizedError()
    return res.text()
  },
})

echo.channel("news").listen("article.published", (article) => render(article))

const orders = echo.private("orders.42")
orders.listen("order.shipped", (order) => update(order))
orders.onState(({ state, recovered }) => {
  if (state === "subscribed" && recovered === false) reloadOrder()
})
await orders.ready()

// toOthers: send the socket ID with your API requests.
fetch("/api/orders/42/ship", { method: "POST", headers: { "X-Socket-ID": echo.socketId } })
```

Go programs such as background agents use [`client`](client), which has the same API.

## How it works

| | |
|---|---|
| Connect | The client opens a WebSocket connection with a fresh random socket ID and your token. NATS asks jetcast through the auth callout; jetcast calls `Authenticate` and `Grants`, registers the socket in a JetStream key-value bucket and returns a JWT allowing public channels, granted channels and the socket's own subjects. |
| Subscribe | Public and granted channels are subscribed directly. Other private channels are authorized by your `Channel` callback; the node that authorized them relays their events to the connection. |
| Broadcast | Events are written to a JetStream stream, which republishes them to channel subjects with their sequence and the sequence of the channel's previous event. |
| Recover | Clients track sequences per channel. A sequence that does not continue the previous one, a reconnect, or a periodic head check triggers recovery from the stream. Recovery is proven complete when nothing after the client's position was evicted; otherwise the client reports `recovered: false`. |

Details: [design](docs/design.md) (Chinese) and [deployment](docs/deployment.md).

## Delivery semantics

- Events are delivered at most once per connection and recovered within the retention
  window (5 minutes by default). Use your own data, not jetcast, as the source of truth:
  reload when a subscription reports `recovered: false`.
- Authorization is checked when a connection is made (grants) and when a channel is
  subscribed (authorizers), then periodically for relayed channels. Use `Disconnect` to
  cut access immediately.
- `toOthers` suppresses the sender's listeners; it is not a confidentiality mechanism.

## Roadmap

- Presence channels (`here`, `joining`, `leaving`) and client events (whisper).
- React hooks.
- Queued and after-commit broadcasting with [jetq](https://github.com/runforyou-ai/jetq).
- History queries, a separate browser account, a mode without auth callout, OpenTelemetry.

## License

[MIT](LICENSE)
