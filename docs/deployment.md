# Deploying jetcast

jetcast is a library: your application owns the NATS server (embedded or not) and the
HTTP edge. This page lists what NATS needs and how to expose its WebSocket listener.

## NATS configuration

jetcast needs nats-server 2.14.4 or later (2.15 recommended) with:

- **JetStream** enabled for the application account.
- **An application account**, here `APP`, holding the event stream, the registry
  and the application's connections, with a user that bypasses the callout.
- **A client account**, here `CLIENT`, that the callout places clients in. It
  exchanges only jetcast's subjects with `APP` through stream exports and imports,
  and carries the limits of client connections (see [Limits](#limits)). Set
  `ServerOptions.Account` to it.
- **An auth callout** whose issuer is the public key of the account key pair you pass as
  `ServerOptions.CalloutSigner`.
- **A WebSocket listener.**
- **Optionally a system account** user, for `jetcast.SystemAdmin` to kick connections on
  `Disconnect`.

[`embedded.Accounts`](../embedded/config.go) renders the account and authorization
blocks below for a prefix, credentials and limits; embedded servers can use it directly
and standalone deployments can print it once. With the default prefix `jetcast`:

```hocon
listen: 0.0.0.0:4222
jetstream { store_dir: /var/lib/nats }

websocket {
  listen: 127.0.0.1:8222     # behind a reverse proxy
  no_tls: true               # TLS terminates at the proxy
  # same_origin compares Origin with the Host seen by NATS and the scheme of
  # the NATS listener; behind a TLS-terminating proxy it rejects browsers.
  allowed_origins: [ "https://app.example.com" ]
}

accounts {
  APP {
    jetstream: enabled
    users: [ { user: app, password: $APP_PASSWORD } ]
    exports: [
      { stream: "jetcast.ev.>", accounts: [ CLIENT ] }   # channel events
      { stream: "jetcast.c.>", accounts: [ CLIENT ] }    # replies, relays, control
    ]
    imports: [ { stream: { account: CLIENT, subject: "jetcast.rq.>" } } ]  # requests
  }
  CLIENT {
    # embedded.Accounts leaves max_connections unlimited unless set; size it
    # for your deployment.
    limits: { max_connections: 50000, max_subscriptions: 1000, max_payload: 65536 }
    exports: [ { stream: "jetcast.rq.>", accounts: [ APP ] } ]
    imports: [
      { stream: { account: APP, subject: "jetcast.ev.>" } }
      { stream: { account: APP, subject: "jetcast.c.>" } }
    ]
  }
  SYS { users: [ { user: sys, password: $SYS_PASSWORD } ] }
}
system_account: SYS

authorization {
  # Authenticate + Grants + registry writes must fit in this time.
  timeout: 5s
  auth_callout {
    issuer: ABJHLOVMPA4CI6R5KLNGOB4GSLNIY7IOUPAJC4YFNDLQVIOBYQGUWVLA  # CalloutSigner public key
    account: APP
    auth_users: [ app, sys ]
    # xkey: <public curve key of ServerOptions.CalloutXKey> to encrypt callouts
  }
}
```

Without `ManageStreams`, create the event stream and the registry bucket yourself; the
bucket `<Stream>_CONN` needs direct gets disabled and a TTL of at least
`MaxConnectionTTL` plus one minute (`ManageStreams` uses plus ten minutes).

Generate the issuer with `nsc` or in Go with `nkeys.CreateAccount()`, and keep its seed
secret: it signs every client's permissions.

Application connection permissions, if you restrict the `app` user: publish
`<prefix>.in.>`, `<prefix>.ev.>`, `<prefix>.c.>`, `<prefix>.sys.>` and the JetStream API;
subscribe `<prefix>.rq.>`, `<prefix>.ev.prv.>`, `<prefix>.sys.>`, `$SYS.REQ.USER.AUTH` and
inboxes.

Clients may also be placed in the application account itself (`Account: "APP"`, no
`CLIENT` account), as in jetcast 0.1. Their permissions still confine them to their own
subjects, but account limits would then also apply to the application's connections,
which hold every relayed subscription of a node, so clients cannot be limited.

### Encrypted callouts

The callout request carries the client's token. To encrypt callout requests and
responses, create a curve key pair (`nkeys.CreateCurveKeys()`, or `nsc generate nkey
--curve`), set its public key as `xkey` in the `auth_callout` block (`Accounts.XKey`)
and pass the key pair as `ServerOptions.CalloutXKey` (`nkeys.FromCurveSeed`). Every node
needs the same key. Encryption matters when callout traffic crosses a network, as in a
cluster; on an embedded server it never leaves the process.

### Limits

NATS does not apply the user limits of callout-issued JWTs, so client connections are
limited by the client account:

| Limit | Applies to | `embedded.Accounts` default |
|---|---|---|
| `max_subscriptions` | each connection | 1000 |
| `max_payload` | each message a client publishes | 64 KiB |
| `max_connections` | the whole account | unlimited; size it for your deployment |

A client uses one subscription per directly subscribed channel (public and granted
channels) plus a few for its own subjects and recoveries; relayed channels use none.
NATS rejects a subscription beyond `max_subscriptions` asynchronously, with an error on
the connection and without closing it; the SDKs do not turn it into a denied channel, so
such a channel stays subscribed without receiving events. Choose a limit well above what
your application subscribes.
jetcast adds per-connection limits on relayed channels and concurrent requests
(`ServerOptions.Limits`).

### Tokens

Clients send your token in the NATS CONNECT message, which is limited by
`max_control_line` (4096 bytes by default). Raise it for long JWTs, or issue short opaque
realtime tokens. To keep the token out of JavaScript, set `token_cookie` in the
`websocket` block and have your login set an HttpOnly cookie; NATS then passes the cookie
value to the callout as the token.

## Embedded NATS

See [examples/embedded](../examples/embedded): one binary embeds NATS with the
configuration above, runs jetcast and serves `/nats` on its own HTTP port. Use
`embedded.Admin(server)` as the `ConnectionAdmin`.

## Exposing WebSocket

NATS accepts WebSocket connections on any path, except paths ending in `/leafnode` or
`/mqtt`, so you can serve it under a path such as `/nats`. Browsers keep the connection
open for hours; the NATS client pings every two minutes, so proxy read timeouts must be
longer than that.

### Go

```go
target, _ := url.Parse("http://127.0.0.1:8222")
mux.Handle("/nats", httputil.NewSingleHostReverseProxy(target))
```

`httputil.ReverseProxy` handles the WebSocket upgrade. Do not wrap the handler in
middleware that buffers responses or sets write timeouts.

### nginx

```nginx
map $http_upgrade $connection_upgrade { default upgrade; '' close; }

location /nats {
    proxy_pass http://127.0.0.1:8222;
    proxy_http_version 1.1;
    proxy_set_header Upgrade $http_upgrade;
    proxy_set_header Connection $connection_upgrade;
    proxy_set_header Host $host;
    proxy_set_header X-Forwarded-For $remote_addr;
    proxy_read_timeout 600s;
    proxy_send_timeout 600s;
    proxy_buffering off;
}
```

### Load balancers

Any load balancer that supports WebSocket works; connections need no stickiness because
every connection is independent. Set the idle timeout above two minutes. With several
NATS servers behind one address, either set `websocket.advertise` to the public URL on
every server or have clients ignore cluster gossip (`ignoreClusterUpdates` in nats.js), so
browsers never try internal addresses.

## Clusters

- Run a jetcast `Server` on every application node; nodes share nothing but NATS.
- Set `History.Replicas` to 3 so the event stream and the registry survive a server loss.
- `jetcast.SystemAdmin` kicks connections on any server of the cluster; `embedded.Admin`
  only reaches its own server.

## Revocation failures

Configure `ConnectionAdmin` for forced disconnection. A failed `Disconnect`
can leave directly subscribed clients connected with their existing NATS grants.
The application must retry failed calls until enforcement completes; repeated
calls include already-revoked connection records. Persist the retry intent when
it must survive an application restart. The registry retains records beyond the
maximum connection lifetime, so retries can address live connections throughout
that lifetime.

## Availability

| Failure | Effect |
|---|---|
| Registry bucket unavailable | New connections are rejected; requests answer `unavailable` and clients retry. Established direct subscriptions keep receiving events. |
| Event stream unavailable | Broadcasts to retained channels fail; heads and recovery fail. Ephemeral channels keep working. |
| All application nodes down | New connections are rejected; direct subscriptions keep receiving events published by other processes. |
