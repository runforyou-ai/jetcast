package jetcast_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/runforyou-ai/jetcast"
	"github.com/runforyou-ai/jetcast/embedded"
)

// failOnceAdmin simulates a transient failure of the system connection.
type failOnceAdmin struct {
	delegate jetcast.ConnectionAdmin
	attempts atomic.Int32
}

// Kick fails once and delegates subsequent attempts to the real administrator.
func (a *failOnceAdmin) Kick(ctx context.Context, server string, cid uint64) error {
	if a.attempts.Add(1) == 1 {
		return errors.New("simulated SYS connection unavailable")
	}
	return a.delegate.Kick(ctx, server, cid)
}

// TestDisconnectRetriesFailedKick covers retries against an already revoked record.
func TestDisconnectRetriesFailedKick(t *testing.T) {
	for _, kind := range []string{"embedded", "system"} {
		t.Run(kind, func(t *testing.T) {
			h := newHarness(t, jetcast.Config{})
			h.grant("mallory", "private_updates")
			delegate := embedded.Admin(h.env.Server)
			if kind == "system" {
				sys, err := h.env.ConnectSys()
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(sys.Close)
				delegate = jetcast.SystemAdmin(sys)
			}
			admin := &failOnceAdmin{delegate: delegate}
			srv := h.node(func(opts *jetcast.ServerOptions) { opts.Admin = admin })
			// This connection subscribes directly and ignores all control messages.
			nc, err := nats.Connect(h.env.URL, nats.Name("AAAAAAAAAAAAAAAAAAAAAB"), nats.Token("mallory:s1.std"), nats.NoReconnect())
			if err != nil {
				t.Fatal(err)
			}
			defer nc.Close()
			sub, err := nc.SubscribeSync("jetcast.ev.prv.private_updates")
			if err != nil {
				t.Fatal(err)
			}
			if err := nc.Flush(); err != nil {
				t.Fatal(err)
			}
			h.mu.Lock()
			h.invalid["mallory:s1.std"] = true
			h.mu.Unlock()
			ctx, cancel := context.WithTimeout(context.Background(), waitTimeout)
			defer cancel()
			first, err := srv.Disconnect(ctx, jetcast.ByUser("mallory"))
			if err == nil || first.Revoked != 1 || first.Kicked != 0 || first.Failed != 1 {
				t.Fatalf("first disconnect: %+v, %v", first, err)
			}
			// Revoking the registry record alone does not remove NATS grants.
			h.broadcast(srv, "update", "before retry", jetcast.Private("private_updates"))
			if _, err := sub.NextMsg(time.Second); err != nil {
				t.Fatalf("expected direct subscription to remain live before retry: %v", err)
			}
			second, err := srv.Disconnect(ctx, jetcast.ByUser("mallory"))
			if err != nil || second.Revoked != 0 || second.Kicked != 1 || second.Failed != 0 || admin.attempts.Load() != 2 {
				t.Fatalf("retry: %+v, %v, Kick attempts=%d", second, err, admin.attempts.Load())
			}
			waitFor(t, nc.IsClosed)
			// Retrying again succeeds even when the NATS connection is already gone.
			third, err := srv.Disconnect(ctx, jetcast.ByUser("mallory"))
			if err != nil || third.Revoked != 0 || third.Failed != 0 {
				t.Fatalf("repeat disconnect: %+v, %v", third, err)
			}
		})
	}
}
