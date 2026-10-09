package jetcast_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
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
	// instead of rebuilding it at once.
	app, err := h.env.ConnectApp()
	if err != nil {
		t.Fatal(err)
	}
	defer app.Close()
	resume := srv.PauseNodeRequests()
	busy, err := app.Subscribe(fmt.Sprintf("jetcast.rq.*.n.%s.renew", srv.Node()), func(m *nats.Msg) {
		_ = m.Respond([]byte(`{"error":{"code":"overloaded"}}`))
	})
	if err != nil {
		t.Fatal(err)
	}
	_ = app.Flush()
	select {
	case st := <-c.states:
		t.Fatalf("state %s after one failed renewal", st.State)
	case <-time.After(700 * time.Millisecond):
	}
	// Renewals keep failing: the relay is rebuilt.
	c.waitState(t, client.StateInterrupted)
	_ = busy.Unsubscribe()
	resume()
	c.waitState(t, client.StateSubscribed)
	h.broadcast(srv, "e", "1", jetcast.Private("orders.21"))
	c.expectData(t, "1")

	// A node without responders is replaced at once.
	resume = srv.PauseNodeRequests()
	defer resume()
	start := time.Now()
	c.waitState(t, client.StateInterrupted)
	if d := time.Since(start); d > 2*time.Second {
		t.Fatalf("relay of a node without responders rebuilt after %v", d)
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
	if o := m.Header.Get(jetcast.HeaderOrigin); o == "" || o == a.SocketID() {
		t.Fatalf("origin header %q exposes the socket", o)
	}
	cb.expectData(t, "1")
	ca.none(t, 300*time.Millisecond)
}
