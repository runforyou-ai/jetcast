package jetcast_test

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/runforyou-ai/jetcast"
	"github.com/runforyou-ai/jetcast/client"
)

// rawConn is a standard NATS connection speaking the client protocol by hand.
type rawConn struct {
	nc     *nats.Conn
	socket string
}

// randomSocket returns a fresh socket ID.
func randomSocket() string {
	const base62 = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"
	b := make([]byte, 22)
	for i := range b {
		n, _ := rand.Int(rand.Reader, big.NewInt(62))
		b[i] = base62[n.Int64()]
	}
	return string(b)
}

// dialRaw connects with a standard connection; token must end in ".std".
func (h *harness) dialRaw(token string) (*rawConn, error) {
	socket := randomSocket()
	nc, err := nats.Connect(h.env.URL, nats.Name(socket), nats.Token(token),
		nats.CustomInboxPrefix("jetcast.c."+socket+".r"), nats.NoReconnect())
	if err != nil {
		return nil, err
	}
	h.t.Cleanup(nc.Close)
	return &rawConn{nc: nc, socket: socket}, nil
}

// request sends one client request and decodes the response.
func (c *rawConn) request(t *testing.T, op string, body, resp any) {
	t.Helper()
	b, _ := json.Marshal(body)
	m, err := c.nc.Request("jetcast.rq."+c.socket+"."+op, b, waitTimeout)
	if err != nil {
		t.Fatalf("%s: %v", op, err)
	}
	if err := json.Unmarshal(m.Data, resp); err != nil {
		t.Fatalf("%s: %v", op, err)
	}
}

func TestHeadsDoNotRunAuthorizers(t *testing.T) {
	h := newHarness(t, jetcast.Config{})
	var calls atomic.Int64
	h.channel("runs.{id}", func(context.Context, jetcast.User, jetcast.Params) (bool, error) {
		calls.Add(1)
		return true, nil
	})
	h.node()
	h.grant("mallory", "users.mallory.>")
	c, err := h.dialRaw("mallory:s1.std")
	if err != nil {
		t.Fatal(err)
	}
	var hello jetcast.HelloResponse
	c.request(t, "hello", struct{}{}, &hello)
	names := []string{"pub.news", "prv.users.mallory.feed"}
	for i := range 198 {
		names = append(names, fmt.Sprintf("prv.runs.r%d", i))
	}
	var resp jetcast.HeadsResponse
	c.request(t, "heads", jetcast.HeadsRequest{Epoch: hello.Epoch, Channels: names}, &resp)
	if resp.Error != nil {
		t.Fatalf("heads: %+v", resp.Error)
	}
	if calls.Load() != 0 {
		t.Fatalf("heads ran the authorizer %d times", calls.Load())
	}
	if len(resp.Denied) != 198 {
		t.Fatalf("%d channels denied, want 198", len(resp.Denied))
	}
	for _, name := range names[:2] {
		if _, ok := resp.Heads[name]; !ok {
			t.Fatalf("no head for %s", name)
		}
	}
}

func TestPanickingCallbacks(t *testing.T) {
	h := newHarness(t, jetcast.Config{})
	h.onAuth = func(r jetcast.AuthRequest) {
		if strings.HasPrefix(r.Token, "panic:") {
			panic("authenticate failure")
		}
	}
	h.channel("boom.{id}", func(context.Context, jetcast.User, jetcast.Params) (bool, error) {
		panic("authorizer failure")
	})
	srv := h.node()
	if _, err := h.dialRaw("panic:s1.std"); err == nil {
		t.Fatal("connection accepted although Authenticate panicked")
	}
	c, err := h.dialRaw("alice:s1.std")
	if err != nil {
		t.Fatalf("connect after a panicking Authenticate: %v", err)
	}
	var resp jetcast.SubResponse
	c.request(t, "sub", jetcast.SubRequest{Channel: "prv.boom.1", Sid: "s1", Path: jetcast.PathRelay}, &resp)
	if resp.Error == nil || resp.Error.Code != jetcast.CodeUnavailable {
		t.Fatalf("sub with a panicking authorizer: %+v", resp.Error)
	}
	var hello jetcast.HelloResponse
	c.request(t, "hello", struct{}{}, &hello)
	if hello.Error != nil {
		t.Fatalf("hello after a panicking authorizer: %+v", hello.Error)
	}
	if st := srv.Stats(); st.CalloutRejected != 1 || st.CalloutAccepted != 1 {
		t.Fatalf("stats %+v", st)
	}
}

func TestCalloutConcurrencyAndClose(t *testing.T) {
	h := newHarness(t, jetcast.Config{})
	var running, peak atomic.Int64
	release := make(chan struct{})
	h.onAuth = func(r jetcast.AuthRequest) {
		if !strings.HasPrefix(r.Token, "slow") {
			return
		}
		n := running.Add(1)
		for {
			p := peak.Load()
			if n <= p || peak.CompareAndSwap(p, n) {
				break
			}
		}
		<-release
		running.Add(-1)
	}
	srv := h.node(func(o *jetcast.ServerOptions) { o.Limits.ConcurrentCallouts = 2 })
	var wg sync.WaitGroup
	for i := range 5 {
		wg.Go(func() { _, _ = h.dialRaw(fmt.Sprintf("slow%d:s1.std", i)) })
	}
	waitFor(t, func() bool { return running.Load() == 2 })
	time.Sleep(200 * time.Millisecond)
	if peak.Load() != 2 {
		t.Fatalf("%d callouts ran at once, want 2", peak.Load())
	}
	closed := make(chan struct{})
	go func() {
		_ = srv.Close()
		close(closed)
	}()
	select {
	case <-closed:
		t.Fatal("Close returned while callouts were running")
	case <-time.After(200 * time.Millisecond):
	}
	close(release)
	select {
	case <-closed:
	case <-time.After(waitTimeout):
		t.Fatal("Close did not return after callouts finished")
	}
	wg.Wait()
}

func TestRegistrationAfterStart(t *testing.T) {
	h := newHarness(t, jetcast.Config{})
	srv := h.node()
	if err := srv.Channel("late.{id}", func(context.Context, jetcast.User, jetcast.Params) (bool, error) {
		return true, nil
	}); !errors.Is(err, jetcast.ErrInvalidConfig) {
		t.Fatalf("Channel after Start: %v", err)
	}
	func() {
		defer func() {
			if recover() == nil {
				t.Fatal("Authenticate after Start did not panic")
			}
		}()
		srv.Authenticate(func(context.Context, jetcast.AuthRequest) (jetcast.User, error) {
			return jetcast.User{}, nil
		})
	}()
}

func TestReauthorizationFailure(t *testing.T) {
	h := newHarness(t, jetcast.Config{})
	var failing atomic.Bool
	h.channel("flaky.{id}", func(context.Context, jetcast.User, jetcast.Params) (bool, error) {
		if failing.Load() {
			return false, errors.New("database unavailable")
		}
		return true, nil
	})
	srv := h.node(func(o *jetcast.ServerOptions) { o.ReauthorizeInterval = 500 * time.Millisecond })
	a := h.client("alice:s1")
	s := a.Private("flaky.1")
	c := collect(s)
	ready(t, s)
	failing.Store(true)
	// Failed reauthorizations keep the relay for a while, then remove it and
	// make the client subscribe again instead of relaying indefinitely.
	c.waitState(t, client.StateInterrupted)
	waitFor(t, func() bool { return srv.Stats().Relays == 0 })
	if st := s.State(); st == client.StateDenied {
		t.Fatal("subscription denied after authorizer failures")
	}
	failing.Store(false)
	c.waitState(t, client.StateSubscribed)
	h.broadcast(srv, "e", "1", jetcast.Private("flaky.1"))
	c.expectData(t, "1")
}

func TestRegistryTTLMargin(t *testing.T) {
	h := newHarness(t, jetcast.Config{})
	h.node()
	nc, err := h.env.ConnectApp()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(nc.Close)
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), waitTimeout)
	defer cancel()
	st, err := js.Stream(ctx, "KV_JETCAST_CONN")
	if err != nil {
		t.Fatal(err)
	}
	cfg := st.CachedInfo().Config
	cfg.MaxAge = time.Hour
	if _, err := js.UpdateStream(ctx, cfg); err != nil {
		t.Fatal(err)
	}
	srv, err := jetcast.NewServer(nc, jetcast.ServerOptions{Account: "APP", CalloutSigner: h.env.Issuer})
	if err != nil {
		t.Fatal(err)
	}
	srv.Authenticate(func(context.Context, jetcast.AuthRequest) (jetcast.User, error) { return jetcast.User{}, nil })
	if err := srv.Start(ctx); err == nil || !strings.Contains(err.Error(), "TTL") {
		t.Fatalf("Start with a registry TTL equal to MaxConnectionTTL: %v", err)
	}
	_ = srv.Close()
}
