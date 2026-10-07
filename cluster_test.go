package jetcast_test

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nkeys"
	"github.com/runforyou-ai/jetcast"
	"github.com/runforyou-ai/jetcast/client"
	"github.com/runforyou-ai/jetcast/internal/devenv"
)

func freePort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}

// TestClusterLeaderChange runs three NATS servers with a replicated stream,
// jetcast nodes on different servers, and moves the stream leader while
// events are broadcast. The client must end up with every event.
func TestClusterLeaderChange(t *testing.T) {
	if testing.Short() {
		t.Skip("cluster test")
	}
	issuer, _ := nkeys.CreateAccount()
	routes := []int{freePort(t), freePort(t), freePort(t)}
	var envs []*devenv.Env
	for i := range 3 {
		env, err := devenv.Start(devenv.Options{
			Issuer: issuer, Name: fmt.Sprintf("n%d", i), ClusterName: "jc",
			ClusterPort: routes[i], Routes: routes,
		})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(env.Close)
		envs = append(envs, env)
	}
	// Wait for the JetStream meta leader.
	nc0, err := envs[0].ConnectApp()
	if err != nil {
		t.Fatal(err)
	}
	defer nc0.Close()
	waitFor(t, func() bool {
		_, err := nc0.Request("$JS.API.INFO", nil, time.Second)
		return err == nil
	})

	h := &harness{t: t, env: envs[1], grants: map[string][]string{}, allowed: map[string]bool{}, invalid: map[string]bool{}}
	srvA := h.node(func(o *jetcast.ServerOptions) { o.History.Replicas = 3; o.Admin = nil })
	h.env = envs[2]
	srvB := h.node(func(o *jetcast.ServerOptions) { o.History.Replicas = 3; o.Admin = nil })
	h.allow("alice", "orders.1")

	h.env = envs[0]
	a := h.client("alice:s1")
	pubSub, privSub := a.Channel("news"), a.Private("orders.1")
	cPub, cPriv := collect(pubSub), collect(privSub)
	ready(t, pubSub)
	ready(t, privSub)

	const total = 60
	ctx := context.Background()
	for i := range total {
		srv := srvA
		if i%2 == 1 {
			srv = srvB
		}
		if i == total/3 || i == 2*total/3 {
			// Move the stream leader.
			_, _ = nc0.Request("$JS.API.STREAM.LEADER.STEPDOWN.JETCAST", nil, 2*time.Second)
		}
		for attempt := 0; ; attempt++ {
			bctx, cancel := context.WithTimeout(ctx, 5*time.Second)
			_, err := srv.Broadcast(bctx, jetcast.Event{
				ID: fmt.Sprintf("ev-%d", i), Name: "e", Data: []byte(fmt.Sprint(i)),
				Channels: []jetcast.Channel{jetcast.Public("news"), jetcast.Private("orders.1")},
			})
			cancel()
			if err == nil {
				break
			}
			if attempt > 20 {
				t.Fatalf("broadcast %d: %v", i, err)
			}
			time.Sleep(200 * time.Millisecond)
		}
	}
	for _, c := range []*collector{cPub, cPriv} {
		seen := map[string]bool{}
		deadline := time.After(30 * time.Second)
		for len(seen) < total {
			select {
			case e := <-c.events:
				seen[string(e.Data)] = true
			case st := <-c.states:
				if st.State == client.StateSubscribed && !st.Recovered && st.Reason != jetcast.ReasonInitial {
					t.Fatalf("recovery failed: %+v", st)
				}
			case <-deadline:
				t.Fatalf("received %d of %d events", len(seen), total)
			}
		}
	}
	_ = nats.Connect
}
