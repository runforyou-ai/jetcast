package jetcast_test

import (
	"context"
	"testing"

	"github.com/runforyou-ai/jetcast"
)

// TestCloseAfterStartFailure verifies cleanup when storage initialization fails.
func TestCloseAfterStartFailure(t *testing.T) {
	for _, kind := range []string{"missing_storage", "cancelled_context"} {
		t.Run(kind, func(t *testing.T) {
			h := newHarness(t, jetcast.Config{})
			nc, err := h.env.ConnectApp()
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(nc.Close)
			srv, err := jetcast.NewServer(nc, jetcast.ServerOptions{
				Account: "APP", CalloutSigner: h.env.Issuer,
				ManageStreams: kind == "cancelled_context",
			})
			if err != nil {
				t.Fatal(err)
			}
			srv.Authenticate(func(context.Context, jetcast.AuthRequest) (jetcast.User, error) {
				return jetcast.User{ID: "unused"}, nil
			})
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if kind == "cancelled_context" {
				cancel()
			}
			if err := srv.Start(ctx); err == nil {
				t.Fatal("expected storage initialization failure")
			}
			if err := srv.Close(); err != nil {
				t.Fatalf("close after failed start: %v", err)
			}
			if err := srv.Close(); err != nil {
				t.Fatalf("repeated close: %v", err)
			}
		})
	}
}
