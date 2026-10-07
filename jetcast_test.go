package jetcast_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/runforyou-ai/jetcast"
	"github.com/runforyou-ai/jetcast/client"
	"github.com/runforyou-ai/jetcast/embedded"
	"github.com/runforyou-ai/jetcast/internal/devenv"
)

const waitTimeout = 10 * time.Second

// harness runs an embedded NATS server and jetcast server nodes.
type harness struct {
	t   *testing.T
	env *devenv.Env

	mu      sync.Mutex
	grants  map[string][]string // user -> patterns
	allowed map[string]bool     // "user|orders.42" -> allowed
	invalid map[string]bool     // revoked tokens
	nodes   []*jetcast.Server
	cfg     jetcast.Config
}

func newHarness(t *testing.T, cfg jetcast.Config) *harness {
	t.Helper()
	env, err := devenv.Start(devenv.Options{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(env.Close)
	return &harness{t: t, env: env, grants: map[string][]string{}, allowed: map[string]bool{}, invalid: map[string]bool{}, cfg: cfg}
}

// node starts a jetcast server node on its own application connection.
func (h *harness) node(mutate ...func(*jetcast.ServerOptions)) *jetcast.Server {
	h.t.Helper()
	nc, err := h.env.ConnectApp()
	if err != nil {
		h.t.Fatal(err)
	}
	h.t.Cleanup(nc.Close)
	opts := jetcast.ServerOptions{
		Config:        h.cfg,
		Account:       "APP",
		CalloutSigner: h.env.Issuer,
		Admin:         embedded.Admin(h.env.Server),
		ManageStreams: true,
		History:       jetcast.History{MaxAge: time.Minute},
		RenewInterval: 500 * time.Millisecond,
	}
	for _, m := range mutate {
		m(&opts)
	}
	srv, err := jetcast.NewServer(nc, opts)
	if err != nil {
		h.t.Fatal(err)
	}
	// Tokens are "<user>:<session>"; ".std" suffix allows standard
	// connections.
	srv.Authenticate(func(ctx context.Context, r jetcast.AuthRequest) (jetcast.User, error) {
		h.mu.Lock()
		bad := h.invalid[r.Token]
		h.mu.Unlock()
		user, session, ok := strings.Cut(strings.TrimSuffix(r.Token, ".std"), ":")
		if !ok || bad {
			return jetcast.User{}, errors.New("bad token")
		}
		u := jetcast.User{ID: user, Session: session, Info: map[string]string{"name": "User " + user}}
		u.ConnectionTypes = []string{jetcast.ConnectionWebSocket, jetcast.ConnectionStandard}
		return u, nil
	})
	srv.Grants(func(ctx context.Context, u jetcast.User) ([]string, error) {
		h.mu.Lock()
		defer h.mu.Unlock()
		return h.grants[u.ID], nil
	})
	if err := srv.Channel("orders.{id}", func(ctx context.Context, u jetcast.User, p jetcast.Params) (bool, error) {
		h.mu.Lock()
		defer h.mu.Unlock()
		return h.allowed[u.ID+"|orders."+p["id"]], nil
	}); err != nil {
		h.t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), waitTimeout)
	defer cancel()
	if err := srv.Start(ctx); err != nil {
		h.t.Fatal(err)
	}
	h.t.Cleanup(func() { _ = srv.Close() })
	h.nodes = append(h.nodes, srv)
	return srv
}

func (h *harness) allow(user, channel string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.allowed[user+"|"+channel] = true
}

func (h *harness) grant(user string, patterns ...string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.grants[user] = patterns
}

// client connects a client over WebSocket.
func (h *harness) client(token string, opts ...func(*client.Options)) *client.Client {
	h.t.Helper()
	o := client.Options{
		Servers: []string{h.env.WSURL},
		Prefix:  h.cfg.Prefix,
		GetToken: func(context.Context) (string, error) {
			h.mu.Lock()
			defer h.mu.Unlock()
			if h.invalid[token] {
				return "", client.ErrUnauthorized
			}
			return token, nil
		},
		HeadsInterval: time.Second,
	}
	for _, f := range opts {
		f(&o)
	}
	ctx, cancel := context.WithTimeout(context.Background(), waitTimeout)
	defer cancel()
	c, err := client.Connect(ctx, o)
	if err != nil {
		h.t.Fatalf("connect %s: %v", token, err)
	}
	h.t.Cleanup(func() { _ = c.Close() })
	return c
}

func (h *harness) broadcast(srv *jetcast.Server, name string, data string, chans ...jetcast.Channel) {
	h.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), waitTimeout)
	defer cancel()
	if _, err := srv.Broadcast(ctx, jetcast.Event{Name: name, Channels: chans, Data: []byte(data)}); err != nil {
		h.t.Fatal(err)
	}
}

// collector records events and states of a subscription.
type collector struct {
	events chan client.Event
	states chan client.State
}

func collect(s *client.Subscription) *collector {
	c := &collector{events: make(chan client.Event, 1000), states: make(chan client.State, 100)}
	s.ListenAll(func(e client.Event) { c.events <- e })
	s.OnState(func(st client.State) { c.states <- st })
	return c
}

func (c *collector) next(t *testing.T) client.Event {
	t.Helper()
	select {
	case e := <-c.events:
		return e
	case <-time.After(waitTimeout):
		t.Fatal("no event")
		return client.Event{}
	}
}

func (c *collector) expectData(t *testing.T, want ...string) {
	t.Helper()
	for _, w := range want {
		if e := c.next(t); string(e.Data) != w {
			t.Fatalf("event data %q, want %q", e.Data, w)
		}
	}
}

func (c *collector) none(t *testing.T, d time.Duration) {
	t.Helper()
	select {
	case e := <-c.events:
		t.Fatalf("unexpected event %s %q", e.Name, e.Data)
	case <-time.After(d):
	}
}

// waitState waits for a state, returning it.
func (c *collector) waitState(t *testing.T, state string) client.State {
	t.Helper()
	deadline := time.After(waitTimeout)
	for {
		select {
		case st := <-c.states:
			if st.State == state {
				return st
			}
		case <-deadline:
			t.Fatalf("no %s state", state)
			return client.State{}
		}
	}
}

func ready(t *testing.T, s *client.Subscription) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), waitTimeout)
	defer cancel()
	if err := s.Ready(ctx); err != nil {
		t.Fatalf("subscription %s: %v", s.Channel(), err)
	}
}

func TestPublicChannel(t *testing.T) {
	h := newHarness(t, jetcast.Config{})
	srv := h.node()
	a, b := h.client("alice:s1"), h.client("bob:s1")
	ca, cb := collect(a.Channel("news")), collect(b.Channel("news"))
	ready(t, a.Channel("news"))
	ready(t, b.Channel("news"))
	h.broadcast(srv, "article.published", `{"id":1}`, jetcast.Public("news"))
	for _, c := range []*collector{ca, cb} {
		e := c.next(t)
		if e.Name != "article.published" || string(e.Data) != `{"id":1}` || e.Sequence == 0 || e.Channel != jetcast.Public("news") {
			t.Fatalf("event %+v", e)
		}
	}
}

func TestGrantedChannel(t *testing.T) {
	h := newHarness(t, jetcast.Config{})
	srv := h.node()
	h.grant("alice", "users.alice.>")
	a := h.client("alice:s1")
	feed := a.Private("users.alice.feed")
	ca := collect(feed)
	ready(t, feed)
	h.broadcast(srv, "notified", "1", jetcast.Private("users.alice.feed"))
	ca.expectData(t, "1")
	if st := srv.Stats(); st.Relays != 0 {
		t.Fatalf("granted channel was relayed: %+v", st)
	}

	// A channel outside the grant without an authorizer is denied.
	other := a.Private("users.bob.feed")
	ctx, cancel := context.WithTimeout(context.Background(), waitTimeout)
	defer cancel()
	if err := other.Ready(ctx); err == nil {
		t.Fatal("subscription outside grants succeeded")
	}
}

func TestRelayedChannel(t *testing.T) {
	h := newHarness(t, jetcast.Config{})
	srv := h.node()
	h.allow("alice", "orders.42")
	a, b := h.client("alice:s1"), h.client("bob:s1")
	sa := a.Private("orders.42")
	ca := collect(sa)
	ready(t, sa)
	if st := srv.Stats(); st.Relays != 1 {
		t.Fatalf("relays %d", st.Relays)
	}
	sb := b.Private("orders.42")
	ctx, cancel := context.WithTimeout(context.Background(), waitTimeout)
	defer cancel()
	if err := sb.Ready(ctx); err == nil {
		t.Fatal("bob subscribed to a denied channel")
	}
	h.broadcast(srv, "order.shipped", "a", jetcast.Private("orders.42"))
	h.broadcast(srv, "order.shipped", "b", jetcast.Private("orders.42"))
	ca.expectData(t, "a", "b")

	sa.Leave()
	h.broadcast(srv, "order.shipped", "c", jetcast.Private("orders.42"))
	ca.none(t, 300*time.Millisecond)
	waitFor(t, func() bool { return srv.Stats().Relays == 0 })
}

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(waitTimeout)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal("condition not met")
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func TestToOthers(t *testing.T) {
	h := newHarness(t, jetcast.Config{})
	srv := h.node()
	h.allow("alice", "orders.1")
	h.allow("bob", "orders.1")
	a, b := h.client("alice:s1"), h.client("bob:s1")
	for _, path := range []jetcast.Channel{jetcast.Public("room"), jetcast.Private("orders.1")} {
		var sa, sb *client.Subscription
		if path.Kind == jetcast.KindPublic {
			sa, sb = a.Channel(path.Name), b.Channel(path.Name)
		} else {
			sa, sb = a.Private(path.Name), b.Private(path.Name)
		}
		ca, cb := collect(sa), collect(sb)
		ready(t, sa)
		ready(t, sb)
		ctx, cancel := context.WithTimeout(context.Background(), waitTimeout)
		_, err := srv.Broadcast(ctx, jetcast.Event{Name: "typed", Channels: []jetcast.Channel{path}, Data: []byte("x"), Origin: a.SocketID()})
		cancel()
		if err != nil {
			t.Fatal(err)
		}
		h.broadcast(srv, "after", "y", path)
		cb.expectData(t, "x", "y")
		// Alice skips her own event but keeps her cursor continuous.
		ca.expectData(t, "y")
	}
}

func TestRecoveryAfterReconnect(t *testing.T) {
	h := newHarness(t, jetcast.Config{})
	srv := h.node()
	h.allow("alice", "orders.7")
	h.grant("alice", "users.alice.>")
	a := h.client("alice:s1")
	subs := []*client.Subscription{a.Channel("news"), a.Private("users.alice.feed"), a.Private("orders.7")}
	chans := []jetcast.Channel{jetcast.Public("news"), jetcast.Private("users.alice.feed"), jetcast.Private("orders.7")}
	var cols []*collector
	for _, s := range subs {
		cols = append(cols, collect(s))
		ready(t, s)
	}
	for i, c := range chans {
		h.broadcast(srv, "e", fmt.Sprintf("%d-1", i), c)
		cols[i].expectData(t, fmt.Sprintf("%d-1", i))
	}
	// Drop the client's connection; events broadcast meanwhile are recovered.
	old := a.SocketID()
	kickSocket(t, h, old)
	for i, c := range chans {
		h.broadcast(srv, "e", fmt.Sprintf("%d-2", i), c)
		h.broadcast(srv, "e", fmt.Sprintf("%d-3", i), c)
	}
	waitFor(t, func() bool { return a.SocketID() != old && a.Status() == client.StatusConnected })
	for i := range chans {
		cols[i].expectData(t, fmt.Sprintf("%d-2", i), fmt.Sprintf("%d-3", i))
		st := cols[i].waitState(t, client.StateSubscribed)
		if !st.Recovered {
			// The first subscribed state is the initial one; wait for the
			// one after reconnecting.
			st = cols[i].waitState(t, client.StateSubscribed)
		}
		if !st.Recovered {
			t.Fatalf("channel %d not recovered: %+v", i, st)
		}
	}
}

// kickSocket closes a client connection from the server side.
func kickSocket(t *testing.T, h *harness, socket string) {
	t.Helper()
	cz, err := h.env.Server.Connz(nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, ci := range cz.Conns {
		if ci.Name == socket {
			if err := h.env.Server.DisconnectClientByID(ci.Cid); err != nil {
				t.Fatal(err)
			}
			return
		}
	}
	t.Fatalf("socket %s not connected", socket)
}

func jetstreamNew(nc *nats.Conn) (jetstream.JetStream, error) { return jetstream.New(nc) }

func TestLiveGapRecovered(t *testing.T) {
	h := newHarness(t, jetcast.Config{})
	srv := h.node()
	h.allow("alice", "orders.9")
	a := h.client("alice:s1")
	s := a.Private("orders.9")
	c := collect(s)
	ready(t, s)
	h.broadcast(srv, "e", "1", jetcast.Private("orders.9"))
	c.expectData(t, "1")
	resume := srv.SuspendRelays(a.SocketID())
	h.broadcast(srv, "e", "2", jetcast.Private("orders.9"))
	resume()
	h.broadcast(srv, "e", "3", jetcast.Private("orders.9"))
	c.expectData(t, "2", "3")
}

func TestTailLossFoundByHeads(t *testing.T) {
	h := newHarness(t, jetcast.Config{})
	srv := h.node()
	h.allow("alice", "orders.9")
	a := h.client("alice:s1")
	s := a.Private("orders.9")
	c := collect(s)
	ready(t, s)
	resume := srv.SuspendRelays(a.SocketID())
	h.broadcast(srv, "e", "last", jetcast.Private("orders.9"))
	resume()
	c.expectData(t, "last")
}

func TestRelayLostIsRebuilt(t *testing.T) {
	h := newHarness(t, jetcast.Config{})
	srv := h.node()
	h.allow("alice", "orders.9")
	a := h.client("alice:s1")
	s := a.Private("orders.9")
	c := collect(s)
	ready(t, s)
	srv.DropRelays(a.SocketID())
	h.broadcast(srv, "e", "1", jetcast.Private("orders.9"))
	// Renewal reports the sid missing; the client resubscribes and recovers.
	c.expectData(t, "1")
	h.broadcast(srv, "e", "2", jetcast.Private("orders.9"))
	c.expectData(t, "2")
}

func TestExpiredHistory(t *testing.T) {
	h := newHarness(t, jetcast.Config{})
	srv := h.node(func(o *jetcast.ServerOptions) { o.History.MaxAge = time.Second })
	gate := make(chan struct{})
	var mu sync.Mutex
	blocked := false
	a := h.client("alice:s1", func(o *client.Options) {
		o.GetToken = func(ctx context.Context) (string, error) {
			mu.Lock()
			b := blocked
			mu.Unlock()
			if b {
				select {
				case <-gate:
				case <-ctx.Done():
					return "", ctx.Err()
				}
			}
			return "alice:s1", nil
		}
	})
	s := a.Channel("news")
	c := collect(s)
	ready(t, s)
	h.broadcast(srv, "e", "1", jetcast.Public("news"))
	c.expectData(t, "1")
	mu.Lock()
	blocked = true
	mu.Unlock()
	kickSocket(t, h, a.SocketID())
	h.broadcast(srv, "e", "lost", jetcast.Public("news"))
	time.Sleep(2500 * time.Millisecond)
	close(gate)
	st := c.waitState(t, client.StateSubscribed) // initial
	if st.Reason == jetcast.ReasonInitial {
		st = c.waitState(t, client.StateSubscribed)
	}
	if st.Recovered || st.Reason != jetcast.ReasonExpired {
		t.Fatalf("state %+v, want expired", st)
	}
	h.broadcast(srv, "e", "2", jetcast.Public("news"))
	c.expectData(t, "2")
}

func TestEpochChange(t *testing.T) {
	h := newHarness(t, jetcast.Config{})
	srv := h.node()
	a := h.client("alice:s1")
	s := a.Channel("news")
	c := collect(s)
	ready(t, s)
	c.waitState(t, client.StateSubscribed)
	h.broadcast(srv, "e", "1", jetcast.Public("news"))
	c.expectData(t, "1")

	nc, err := h.env.ConnectApp()
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	js, _ := jetstreamNew(nc)
	ctx := context.Background()
	info, err := js.Stream(ctx, "JETCAST")
	if err != nil {
		t.Fatal(err)
	}
	cfg := info.CachedInfo().Config
	if err := js.DeleteStream(ctx, "JETCAST"); err != nil {
		t.Fatal(err)
	}
	if _, err := js.CreateStream(ctx, cfg); err != nil {
		t.Fatal(err)
	}
	st := c.waitState(t, client.StateSubscribed)
	if st.Recovered || st.Reason != jetcast.ReasonEpoch {
		t.Fatalf("state %+v, want epoch", st)
	}
	h.broadcast(srv, "e", "2", jetcast.Public("news"))
	c.expectData(t, "2")
}

func TestMultiNodeFailover(t *testing.T) {
	h := newHarness(t, jetcast.Config{})
	first := h.node()
	h.allow("alice", "orders.5")
	a := h.client("alice:s1")
	s := a.Private("orders.5")
	c := collect(s)
	ready(t, s)
	second := h.node()
	if first.Stats().Relays != 1 {
		t.Fatalf("relay not on the first node")
	}
	h.broadcast(second, "e", "1", jetcast.Private("orders.5"))
	c.expectData(t, "1")
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	h.broadcast(second, "e", "2", jetcast.Private("orders.5"))
	c.expectData(t, "2")
	waitFor(t, func() bool { return second.Stats().Relays == 1 })
	h.broadcast(second, "e", "3", jetcast.Private("orders.5"))
	c.expectData(t, "3")
}

func TestDisconnect(t *testing.T) {
	h := newHarness(t, jetcast.Config{})
	srv := h.node()
	a1 := h.client("alice:s1")
	a2 := h.client("alice:s2")
	b := h.client("bob:s1")
	stopped := make(chan client.Status, 10)
	a1.OnStatus(func(s client.Status) { stopped <- s })
	h.mu.Lock()
	h.invalid["alice:s1"] = true
	h.mu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), waitTimeout)
	defer cancel()
	res, err := srv.Disconnect(ctx, jetcast.BySession("alice", "s1"))
	if err != nil {
		t.Fatal(err)
	}
	if res.Connections != 1 || res.Revoked != 1 || res.Kicked != 1 || !res.Enforced {
		t.Fatalf("result %+v", res)
	}
	waitFor(t, func() bool { return a1.Status() == client.StatusStopped })
	if a2.Status() != client.StatusConnected || b.Status() != client.StatusConnected {
		t.Fatal("other sessions were disconnected")
	}
	res, err = srv.Disconnect(ctx, jetcast.ByUser("alice"))
	if err != nil || res.Revoked != 1 {
		t.Fatalf("by user: %+v %v", res, err)
	}
}

func TestDisconnectNonCooperativeClient(t *testing.T) {
	h := newHarness(t, jetcast.Config{})
	srv := h.node()
	socket := "AAAAAAAAAAAAAAAAAAAAAB"
	nc, err := nats.Connect(h.env.URL, nats.Name(socket), nats.Token("mallory:s1.std"), nats.NoReconnect())
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	closed := make(chan struct{})
	nc.SetClosedHandler(func(*nats.Conn) { close(closed) })
	ctx, cancel := context.WithTimeout(context.Background(), waitTimeout)
	defer cancel()
	if _, err := srv.Disconnect(ctx, jetcast.ByUser("mallory")); err != nil {
		t.Fatal(err)
	}
	select {
	case <-closed:
	case <-time.After(waitTimeout):
		t.Fatal("connection not kicked")
	}
	// The revoked socket cannot be claimed again.
	if _, err := nats.Connect(h.env.URL, nats.Name(socket), nats.Token("mallory:s2.std"), nats.NoReconnect()); err == nil {
		t.Fatal("revoked socket reused")
	}
}

func TestSocketCannotBeReused(t *testing.T) {
	h := newHarness(t, jetcast.Config{})
	h.node()
	socket := "AAAAAAAAAAAAAAAAAAAAAC"
	nc, err := nats.Connect(h.env.URL, nats.Name(socket), nats.Token("alice:s1.std"), nats.NoReconnect())
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	if _, err := nats.Connect(h.env.URL, nats.Name(socket), nats.Token("mallory:s1.std"), nats.NoReconnect()); err == nil {
		t.Fatal("second connection with the same socket accepted")
	}
	if _, err := nats.Connect(h.env.URL, nats.Name("short"), nats.Token("mallory:s1.std"), nats.NoReconnect()); err == nil {
		t.Fatal("malformed socket accepted")
	}
}

func TestClientPermissions(t *testing.T) {
	h := newHarness(t, jetcast.Config{})
	srv := h.node()
	h.grant("mallory", "users.mallory.>")
	socket := "AAAAAAAAAAAAAAAAAAAAAD"
	errs := make(chan error, 10)
	nc, err := nats.Connect(h.env.URL, nats.Name(socket), nats.Token("mallory:s1.std"), nats.NoReconnect(),
		nats.ErrorHandler(func(_ *nats.Conn, _ *nats.Subscription, err error) { errs <- err }))
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	expectViolation := func(what string) {
		t.Helper()
		select {
		case err := <-errs:
			if !strings.Contains(strings.ToLower(err.Error()), "permissions violation") {
				t.Fatalf("%s: %v", what, err)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("%s: no permissions violation", what)
		}
	}
	_ = nc.Publish("jetcast.ev.pub.news", []byte("forged"))
	expectViolation("publish to a channel")
	_ = nc.Publish("jetcast.in.pub.news", []byte("forged"))
	expectViolation("publish to the stream")
	_, _ = nc.Subscribe("jetcast.ev.prv.orders.1", func(*nats.Msg) {})
	expectViolation("subscribe to an ungranted channel")
	_, _ = nc.QueueSubscribe("jetcast.ev.pub.news", "q", func(*nats.Msg) {})
	expectViolation("queue subscribe")
	_, _ = nc.Subscribe("jetcast.rq.>", func(*nats.Msg) {})
	expectViolation("subscribe to requests")

	// A request whose reply subject is outside the connection's namespace
	// is dropped.
	app, err := h.env.ConnectApp()
	if err != nil {
		t.Fatal(err)
	}
	defer app.Close()
	victim := "jetcast.c.AAAAAAAAAAAAAAAAAAAAAE.r.x"
	got, _ := app.SubscribeSync(victim)
	_ = app.Flush()
	_ = nc.PublishRequest("jetcast.rq."+socket+".hello", victim, []byte("{}"))
	if _, err := got.NextMsg(500 * time.Millisecond); err == nil {
		t.Fatal("server replied into another connection's namespace")
	}
	_ = srv
}

func TestRefreshPicksUpGrants(t *testing.T) {
	h := newHarness(t, jetcast.Config{})
	srv := h.node()
	a := h.client("alice:s1")
	old := a.SocketID()
	h.grant("alice", "teams.t1.>")
	ctx, cancel := context.WithTimeout(context.Background(), waitTimeout)
	defer cancel()
	if err := srv.Refresh(ctx, jetcast.ByUser("alice")); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return a.SocketID() != old && a.SocketID() != "" })
	s := a.Private("teams.t1.board")
	c := collect(s)
	ready(t, s)
	h.broadcast(srv, "e", "1", jetcast.Private("teams.t1.board"))
	c.expectData(t, "1")
	if srv.Stats().Relays != 0 {
		t.Fatal("granted channel relayed after refresh")
	}
}

func TestServerLeave(t *testing.T) {
	h := newHarness(t, jetcast.Config{})
	srv := h.node()
	h.allow("alice", "orders.3")
	a := h.client("alice:s1")
	s := a.Private("orders.3")
	c := collect(s)
	ready(t, s)
	ctx, cancel := context.WithTimeout(context.Background(), waitTimeout)
	defer cancel()
	if err := srv.Leave(ctx, jetcast.Private("orders.3"), jetcast.ByUser("alice")); err != nil {
		t.Fatal(err)
	}
	c.waitState(t, client.StateDenied)
	waitFor(t, func() bool { return srv.Stats().Relays == 0 })
}

func TestReauthorization(t *testing.T) {
	h := newHarness(t, jetcast.Config{})
	srv := h.node(func(o *jetcast.ServerOptions) { o.ReauthorizeInterval = 500 * time.Millisecond })
	h.allow("alice", "orders.4")
	a := h.client("alice:s1")
	s := a.Private("orders.4")
	c := collect(s)
	ready(t, s)
	h.mu.Lock()
	delete(h.allowed, "alice|orders.4")
	h.mu.Unlock()
	c.waitState(t, client.StateDenied)
	waitFor(t, func() bool { return srv.Stats().Relays == 0 })
}

func TestEphemeralChannel(t *testing.T) {
	h := newHarness(t, jetcast.Config{Ephemeral: []string{"typing.>"}})
	srv := h.node()
	a := h.client("alice:s1")
	s := a.Channel("typing.room1")
	c := collect(s)
	ready(t, s)
	st := c.waitState(t, client.StateSubscribed)
	if st.Reason != jetcast.ReasonEphemeral {
		t.Fatalf("state %+v", st)
	}
	res, err := srv.Broadcast(context.Background(), jetcast.Event{Name: "typing", Channels: []jetcast.Channel{jetcast.Public("typing.room1")}})
	if err != nil || res.Channels[0].Sequence != 0 {
		t.Fatalf("%+v %v", res, err)
	}
	if e := c.next(t); e.Name != "typing" || e.Sequence != 0 {
		t.Fatalf("event %+v", e)
	}
}

type orderShipped struct {
	ID int `json:"id"`
}

func (o orderShipped) BroadcastOn() []jetcast.Channel {
	return []jetcast.Channel{jetcast.Public(fmt.Sprintf("shipments.%d", o.ID))}
}

func TestDispatch(t *testing.T) {
	h := newHarness(t, jetcast.Config{})
	srv := h.node()
	a := h.client("alice:s1")
	s := a.Channel("shipments.8")
	c := collect(s)
	ready(t, s)
	if _, err := srv.Dispatch(context.Background(), orderShipped{ID: 8}); err != nil {
		t.Fatal(err)
	}
	if e := c.next(t); e.Name != "orderShipped" || string(e.Data) != `{"id":8}` {
		t.Fatalf("event %+v %s", e, e.Data)
	}
}

func TestCredentialRefresh(t *testing.T) {
	h := newHarness(t, jetcast.Config{})
	srv := h.node(func(o *jetcast.ServerOptions) { o.MaxConnectionTTL = 33 * time.Second })
	statuses := make(chan client.Status, 10)
	a := h.client("alice:s1")
	a.OnStatus(func(s client.Status) { statuses <- s })
	old := a.SocketID()
	s := a.Channel("news")
	c := collect(s)
	ready(t, s)
	waitFor(t, func() bool { return a.SocketID() != old })
	select {
	case st := <-statuses:
		t.Fatalf("status changed to %s during refresh", st)
	default:
	}
	h.broadcast(srv, "e", "1", jetcast.Public("news"))
	c.expectData(t, "1")
}

func TestRevocationDuringAuthentication(t *testing.T) {
	h := newHarness(t, jetcast.Config{})
	srv := h.node()
	entered := make(chan struct{})
	release := make(chan struct{})
	slow := h.node()
	_ = slow
	// Replace the authenticator of both nodes with one that blocks for
	// mallory, so the callout is in flight while Disconnect runs.
	for _, n := range h.nodes {
		n.Authenticate(func(ctx context.Context, r jetcast.AuthRequest) (jetcast.User, error) {
			if strings.HasPrefix(r.Token, "mallory") {
				select {
				case entered <- struct{}{}:
				default:
				}
				<-release
			}
			user, session, _ := strings.Cut(strings.TrimSuffix(r.Token, ".std"), ":")
			return jetcast.User{ID: user, Session: session, ConnectionTypes: []string{jetcast.ConnectionStandard}}, nil
		})
	}
	result := make(chan error, 1)
	go func() {
		nc, err := nats.Connect(h.env.URL, nats.Name("AAAAAAAAAAAAAAAAAAAAAF"), nats.Token("mallory:s1.std"), nats.NoReconnect())
		if nc != nil {
			nc.Close()
		}
		result <- err
	}()
	<-entered
	ctx, cancel := context.WithTimeout(context.Background(), waitTimeout)
	defer cancel()
	if _, err := srv.Disconnect(ctx, jetcast.ByUser("mallory")); err != nil {
		t.Fatal(err)
	}
	close(release)
	if err := <-result; err == nil {
		t.Fatal("connection authenticated across a revocation was accepted")
	}
}

func TestClientLeaveStopsRelayImmediately(t *testing.T) {
	h := newHarness(t, jetcast.Config{})
	srv := h.node(func(o *jetcast.ServerOptions) { o.RenewInterval = time.Minute })
	h.allow("alice", "orders.11")
	a := h.client("alice:s1")
	s := a.Private("orders.11")
	ready(t, s)
	s.Leave()
	deadline := time.Now().Add(2 * time.Second)
	for srv.Stats().Relays != 0 {
		if time.Now().After(deadline) {
			t.Fatal("relay not released by leave")
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func TestRequestValidation(t *testing.T) {
	h := newHarness(t, jetcast.Config{})
	srv := h.node()
	sock := "AAAAAAAAAAAAAAAAAAAAAG"
	cases := []struct {
		subject, reply string
		ok             bool
	}{
		{"jetcast.rq." + sock + ".hello", "jetcast.c." + sock + ".r.x", true},
		{"jetcast.rq." + sock + ".hello", "jetcast.in.pub.news", false},
		{"jetcast.rq." + sock + ".hello", "jetcast.c.AAAAAAAAAAAAAAAAAAAAAH.r.x", false},
		{"jetcast.rq." + sock + ".hello", "", false},
		{"jetcast.rq.short.hello", "jetcast.c.short.r.x", false},
	}
	for _, c := range cases {
		if got := srv.ValidRequest(c.subject, c.reply); got != c.ok {
			t.Errorf("%s reply %q: %v", c.subject, c.reply, got)
		}
	}
}

func TestRevocationDuringRelayAuthorization(t *testing.T) {
	h := newHarness(t, jetcast.Config{})
	srv := h.node(func(o *jetcast.ServerOptions) { o.Admin = nil })
	entered, release := make(chan struct{}), make(chan struct{})
	if err := srv.Channel("slow.{id}", func(ctx context.Context, u jetcast.User, p jetcast.Params) (bool, error) {
		close(entered)
		<-release
		return true, nil
	}); err != nil {
		t.Fatal(err)
	}
	a := h.client("alice:s1")
	s := a.Private("slow.1")
	c := collect(s)
	<-entered
	h.mu.Lock()
	h.invalid["alice:s1"] = true
	h.mu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), waitTimeout)
	defer cancel()
	if _, err := srv.Disconnect(ctx, jetcast.BySession("alice", "s1")); err != nil {
		t.Fatal(err)
	}
	close(release)
	time.Sleep(300 * time.Millisecond)
	if n := srv.Stats().Relays; n != 0 {
		t.Fatalf("%d relays after revocation", n)
	}
	h.broadcast(srv, "e", "secret", jetcast.Private("slow.1"))
	c.none(t, 300*time.Millisecond)
}

func TestCloseFromCallback(t *testing.T) {
	h := newHarness(t, jetcast.Config{})
	srv := h.node()
	a := h.client("alice:s1")
	done := make(chan struct{})
	s := a.Channel("news").Listen("bye", func(client.Event) {
		_ = a.Close()
		close(done)
	})
	ready(t, s)
	h.broadcast(srv, "bye", "", jetcast.Public("news"))
	select {
	case <-done:
	case <-time.After(waitTimeout):
		t.Fatal("Close from a callback did not return")
	}
}
