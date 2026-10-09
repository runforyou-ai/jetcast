package jetcast_test

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/runforyou-ai/jetcast"
	"github.com/runforyou-ai/jetcast/client"
)

func TestClientStaysWithinRequestLimit(t *testing.T) {
	h := newHarness(t, jetcast.Config{})
	srv := h.node(func(o *jetcast.ServerOptions) { o.Limits.ConcurrentRequests = 2 })
	h.grant("alice", "users.alice.>")
	a := h.client("alice:s1")
	var subs []*client.Subscription
	for i := range 40 {
		subs = append(subs, a.Private(fmt.Sprintf("users.alice.n%d", i)))
	}
	for _, s := range subs {
		ready(t, s)
	}
	if n := srv.Stats().Overloaded; n != 0 {
		t.Fatalf("%d requests answered overloaded", n)
	}
	// Reconnecting resubscribes every channel at once.
	old := a.SocketID()
	ctx, cancel := context.WithTimeout(context.Background(), waitTimeout)
	defer cancel()
	if err := srv.Refresh(ctx, jetcast.ByUser("alice")); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return a.SocketID() != old && a.SocketID() != "" })
	waitFor(t, func() bool {
		for _, s := range subs {
			if s.State() != client.StateSubscribed {
				return false
			}
		}
		return true
	})
	if n := srv.Stats().Overloaded; n != 0 {
		t.Fatalf("%d requests answered overloaded after reconnecting", n)
	}
}

func TestFailedRenewalsKeepRelays(t *testing.T) {
	h := newHarness(t, jetcast.Config{})
	srv := h.node()
	h.allow("alice", "orders.21")
	a := h.client("alice:s1")
	s := a.Private("orders.21")
	c := collect(s)
	ready(t, s)
	c.waitState(t, client.StateSubscribed)

	// The node answers renewals overloaded: the client keeps its relay
	// until the node must have dropped it, instead of rebuilding it at once.
	app, err := h.env.ConnectApp()
	if err != nil {
		t.Fatal(err)
	}
	defer app.Close()
	answer := func(code string) (stop func()) {
		resume := srv.PauseNodeRequests()
		sub, err := app.Subscribe(fmt.Sprintf("jetcast.rq.*.n.%s.renew", srv.Node()), func(m *nats.Msg) {
			_ = m.Respond([]byte(`{"error":{"code":"` + code + `"}}`))
		})
		if err != nil {
			t.Fatal(err)
		}
		_ = app.Flush()
		return func() {
			_ = sub.Unsubscribe()
			resume()
		}
	}
	stop := answer(jetcast.CodeOverloaded)
	start := time.Now()
	c.waitState(t, client.StateInterrupted)
	// Renewals run every 500ms; the node drops relays after more than three
	// periods, so four periods must pass first.
	if d := time.Since(start); d < 1700*time.Millisecond {
		t.Fatalf("relay rebuilt %v after renewals started failing", d)
	}
	stop()
	c.waitState(t, client.StateSubscribed)
	h.broadcast(srv, "e", "1", jetcast.Private("orders.21"))
	c.expectData(t, "1")

	// A connection the node no longer knows is rebuilt at once.
	stop = answer(jetcast.CodeDenied)
	start = time.Now()
	c.waitState(t, client.StateInterrupted)
	if d := time.Since(start); d > 1500*time.Millisecond {
		t.Fatalf("relay of a denied renewal rebuilt after %v", d)
	}
	stop()
	c.waitState(t, client.StateSubscribed)

	// A node without responders is replaced at once.
	resume := srv.PauseNodeRequests()
	defer resume()
	start = time.Now()
	c.waitState(t, client.StateInterrupted)
	if d := time.Since(start); d > 1500*time.Millisecond {
		t.Fatalf("relay of a node without responders rebuilt after %v", d)
	}
}

func TestRenewalsGoFirst(t *testing.T) {
	h := newHarness(t, jetcast.Config{})
	h.channel("slow.{id}", func(context.Context, jetcast.User, jetcast.Params) (bool, error) {
		time.Sleep(200 * time.Millisecond)
		return true, nil
	})
	srv := h.node(func(o *jetcast.ServerOptions) { o.Limits.ConcurrentRequests = 1 })
	h.allow("alice", "orders.22")
	a := h.client("alice:s1")
	s := a.Private("orders.22")
	c := collect(s)
	ready(t, s)
	// Forty slow subscriptions queue for the only request slot for about
	// eight seconds; renewals of the existing relay go first.
	for i := range 40 {
		a.Private(fmt.Sprintf("slow.s%d", i))
	}
	time.Sleep(3 * time.Second)
	h.broadcast(srv, "e", "1", jetcast.Private("orders.22"))
	c.expectData(t, "1")
}

func TestStaleRequestsAreNotSent(t *testing.T) {
	h := newHarness(t, jetcast.Config{})
	var calls atomic.Int64
	h.channel("slow.{id}", func(context.Context, jetcast.User, jetcast.Params) (bool, error) {
		calls.Add(1)
		time.Sleep(200 * time.Millisecond)
		return true, nil
	})
	h.node(func(o *jetcast.ServerOptions) { o.Limits.ConcurrentRequests = 1 })
	a := h.client("alice:s1")
	var subs []*client.Subscription
	for i := range 20 {
		subs = append(subs, a.Private(fmt.Sprintf("slow.s%d", i)))
	}
	for _, s := range subs {
		s.Leave()
	}
	start := time.Now()
	ready(t, a.Channel("news"))
	if d := time.Since(start); d > time.Second {
		t.Fatalf("new channel waited %v behind left ones", d)
	}
	if n := calls.Load(); n > 2 {
		t.Fatalf("%d requests of left subscriptions were sent", n)
	}
}

func TestReadyFailsOnClose(t *testing.T) {
	h := newHarness(t, jetcast.Config{})
	release := make(chan struct{})
	defer close(release)
	h.channel("slow.{id}", func(ctx context.Context, u jetcast.User, p jetcast.Params) (bool, error) {
		select {
		case <-release:
		case <-ctx.Done():
		}
		return true, nil
	})
	h.node()
	a := h.client("alice:s1")
	s := a.Private("slow.1")
	_ = a.Close()
	ctx, cancel := context.WithTimeout(context.Background(), waitTimeout)
	defer cancel()
	if err := s.Ready(ctx); !errors.Is(err, client.ErrClosed) {
		t.Fatalf("Ready after Close: %v", err)
	}
	if st := s.State(); st != client.StateLeft {
		t.Fatalf("state %s after Close", st)
	}
}

func TestOriginHidesSocket(t *testing.T) {
	h := newHarness(t, jetcast.Config{})
	srv := h.node()
	a := h.client("alice:s1")
	b := h.client("bob:s1")
	sa, sb := a.Channel("news"), b.Channel("news")
	ca, cb := collect(sa), collect(sb)
	ready(t, sa)
	ready(t, sb)
	app, err := h.env.ConnectApp()
	if err != nil {
		t.Fatal(err)
	}
	defer app.Close()
	raw, err := app.SubscribeSync("jetcast.ev.pub.news")
	if err != nil {
		t.Fatal(err)
	}
	_ = app.Flush()
	ctx, cancel := context.WithTimeout(context.Background(), waitTimeout)
	defer cancel()
	if _, err := srv.Broadcast(ctx, jetcast.Event{Name: "e", Channels: []jetcast.Channel{jetcast.Public("news")}, Data: []byte("1"), Origin: a.SocketID()}); err != nil {
		t.Fatal(err)
	}
	m, err := raw.NextMsg(waitTimeout)
	if err != nil {
		t.Fatal(err)
	}
	if o := m.Header.Get(jetcast.HeaderOrigin); o != jetcast.OriginTag(a.SocketID()) {
		t.Fatalf("origin header %q, want the digest of the socket", o)
	}
	cb.expectData(t, "1")
	ca.none(t, 300*time.Millisecond)

	// Servers before 0.2 publish the socket ID itself, as during a rolling
	// upgrade; the client still recognizes its own events.
	js, err := jetstream.New(app)
	if err != nil {
		t.Fatal(err)
	}
	old := nats.NewMsg("jetcast.in.pub.news")
	old.Data = []byte("2")
	old.Header.Set(jetcast.HeaderEvent, "e")
	old.Header.Set(jetcast.HeaderID, "old-1")
	old.Header.Set(jetcast.HeaderOrigin, a.SocketID())
	if _, err := js.PublishMsg(ctx, old); err != nil {
		t.Fatal(err)
	}
	cb.expectData(t, "2")
	ca.none(t, 300*time.Millisecond)
}
