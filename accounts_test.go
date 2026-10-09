package jetcast_test

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/runforyou-ai/jetcast"
	"github.com/runforyou-ai/jetcast/embedded"
	"github.com/runforyou-ai/jetcast/internal/devenv"
)

func TestClientAccountLimits(t *testing.T) {
	h := newHarnessWith(t, jetcast.Config{}, devenv.Options{
		Limits: embedded.ClientLimits{MaxSubscriptions: 20, MaxPayload: 1024},
	})
	srv := h.node()
	errs := make(chan error, 100)
	socket := "AAAAAAAAAAAAAAAAAAAAAJ"
	nc, err := nats.Connect(h.env.URL, nats.Name(socket), nats.Token("mallory:s1.std"), nats.NoReconnect(),
		nats.ErrorHandler(func(_ *nats.Conn, _ *nats.Subscription, err error) { errs <- err }))
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	for i := range 30 {
		_, _ = nc.Subscribe(fmt.Sprintf("jetcast.c.%s.x%d", socket, i), func(*nats.Msg) {})
	}
	_ = nc.Flush()
	select {
	case err := <-errs:
		if !strings.Contains(strings.ToLower(err.Error()), "maximum subscriptions") {
			t.Fatalf("subscription limit: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("client exceeded the subscription limit")
	}
	big, err := nats.Connect(h.env.URL, nats.Name("AAAAAAAAAAAAAAAAAAAAAK"), nats.Token("mallory:s2.std"), nats.NoReconnect())
	if err != nil {
		t.Fatal(err)
	}
	defer big.Close()
	_ = big.Publish("jetcast.rq.AAAAAAAAAAAAAAAAAAAAAK.hello", make([]byte, 2048))
	_ = big.Flush()
	waitFor(t, big.IsClosed)
	if e := big.LastError(); e == nil || !strings.Contains(strings.ToLower(e.Error()), "payload") {
		t.Fatalf("payload limit: %v", e)
	}

	// The application's own connection is not limited, and clients in the
	// client account still work end to end.
	app, err := h.env.ConnectApp()
	if err != nil {
		t.Fatal(err)
	}
	defer app.Close()
	for i := range 100 {
		if _, err := app.Subscribe(fmt.Sprintf("app.x%d", i), func(*nats.Msg) {}); err != nil {
			t.Fatal(err)
		}
	}
	if err := app.Flush(); err != nil {
		t.Fatal(err)
	}
	h.allow("alice", "orders.1")
	a := h.client("alice:s1")
	pub, prv := a.Channel("news"), a.Private("orders.1")
	cPub, cPrv := collect(pub), collect(prv)
	ready(t, pub)
	ready(t, prv)
	h.broadcast(srv, "e", "1", jetcast.Public("news"))
	h.broadcast(srv, "e", "2", jetcast.Private("orders.1"))
	cPub.expectData(t, "1")
	cPrv.expectData(t, "2")
}

func TestSharedAccount(t *testing.T) {
	h := newHarnessWith(t, jetcast.Config{}, devenv.Options{SharedAccount: true})
	srv := h.node()
	h.allow("alice", "orders.1")
	a := h.client("alice:s1")
	s := a.Private("orders.1")
	c := collect(s)
	ready(t, s)
	h.broadcast(srv, "e", "1", jetcast.Private("orders.1"))
	c.expectData(t, "1")
}

func TestAccountsConfig(t *testing.T) {
	if _, err := (embedded.Accounts{Issuer: "A"}).Config(); err == nil {
		t.Fatal("missing AppPassword accepted")
	}
	if _, err := (embedded.Accounts{AppPassword: "x", Issuer: "A", Clients: "APP"}).Config(); err == nil {
		t.Fatal("shared App and Clients accounts accepted")
	}
	conf, err := embedded.Accounts{Prefix: "rt", AppPassword: "x", Issuer: "A", XKey: "X", Limits: embedded.ClientLimits{MaxConnections: 10}}.Config()
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{`stream: "rt.ev.>"`, `subject: "rt.rq.>"`, "max_connections: 10", "max_subscriptions: 1000", `xkey: "X"`} {
		if !strings.Contains(conf, want) {
			t.Fatalf("config lacks %q:\n%s", want, conf)
		}
	}
}
