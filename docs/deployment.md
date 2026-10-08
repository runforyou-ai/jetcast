# Deploying jetcast

jetcast is a library: your application owns the NATS server (embedded or not) and the
HTTP edge. This page lists what NATS needs and how to expose its WebSocket listener.

## NATS configuration

jetcast needs nats-server 2.14.4 or later (2.15 recommended) with:

- **JetStream** enabled for the application account.
- **An account for clients and the application**, here `APP`. The application connects
  with a user that bypasses the callout; browsers are placed in the same account by the
  callout, with permissions limited to their own subjects.
- **An auth callout** whose issuer is the public key of the account key pair you pass as
  `ServerOptions.CalloutSigner`.
- **A WebSocket listener.**
- **Optionally a system account** user, for `jetcast.SystemAdmin` to kick connections on
  `Disconnect`.

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

Generate the issuer with `nsc` or in Go with `nkeys.CreateAccount()`, and keep its seed
secret: it signs every client's permissions.

Application connection permissions, if you restrict the `app` user: publish
`<prefix>.in.>`, `<prefix>.ev.>`, `<prefix>.c.>`, `<prefix>.sys.>` and the JetStream API;
subscribe `<prefix>.rq.>`, `<prefix>.ev.prv.>`, `<prefix>.sys.>`, `$SYS.REQ.USER.AUTH` and
inboxes.

### Limits

Per-user limits in callout-issued JWTs are not enforced by NATS, so set account limits
for the client account: `max_connections`, `max_subscriptions`, `max_payload`.
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
