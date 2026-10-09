package client

import (
	"context"
	"testing"
	"time"

	ns "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/runforyou-ai/jetcast"
)

func TestQueuedRenewalLeaseStartsWhenSent(t *testing.T) {
	server, err := ns.NewServer(&ns.Options{Host: "127.0.0.1", Port: -1, NoLog: true, NoSigs: true})
	if err != nil {
		t.Fatal(err)
	}
	go server.Start()
	defer server.Shutdown()
	if !server.ReadyForConnections(5 * time.Second) {
		t.Fatal("NATS server not ready")
	}
	nc, err := nats.Connect(server.ClientURL())
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	if _, err := nc.Subscribe("t.rq.socket.n.node.renew", func(m *nats.Msg) { _ = m.Respond([]byte(`{}`)) }); err != nil {
		t.Fatal(err)
	}
	if err := nc.Flush(); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	conn := &connection{nc: nc, socket: "socket", hello: jetcast.HelloResponse{RenewMs: 1000},
		slots: newSlots(1), done: make(chan struct{}), renewing: map[string]bool{}}
	c := &Client{ctx: ctx, conn: conn, sub: subjectsOf{p: "t"}, subs: map[string]*Subscription{}}
	s := newSubscription(c, jetcast.Private("orders.1"))
	s.conn, s.sid, s.node, s.path, s.state, s.leaseAt = conn, "sid", "node", jetcast.PathRelay, StateSubscribed, time.Now()
	c.subs["prv.orders.1"] = s

	// The only slot is taken; the renewal waits for it.
	if err := conn.acquire(ctx, false); err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() {
		c.renew(conn)
		close(done)
	}()
	for deadline := time.Now().Add(5 * time.Second); ; time.Sleep(time.Millisecond) {
		conn.slots.mu.Lock()
		queued := len(conn.slots.urgent) > 0
		conn.slots.mu.Unlock()
		if queued {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("renewal not waiting for a slot")
		}
	}
	time.Sleep(300 * time.Millisecond)
	released := time.Now()
	conn.release()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("renewal not finished")
	}
	s.mu.Lock()
	leaseAt := s.leaseAt
	s.mu.Unlock()
	if leaseAt.Before(released) {
		t.Fatalf("lease restarted %v before the renewal was sent", released.Sub(leaseAt))
	}
}
