// Package embedded adapts jetcast to a NATS server embedded in the same
// process: it renders the accounts jetcast needs and disconnects clients of
// the embedded server.
package embedded

import (
	"context"
	"fmt"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/runforyou-ai/jetcast"
)

// Admin returns a ConnectionAdmin that disconnects clients of an embedded
// server directly. In a cluster it only reaches connections of this server;
// use jetcast.SystemAdmin there.
func Admin(s *server.Server) jetcast.ConnectionAdmin { return admin{s} }

type admin struct{ s *server.Server }

func (a admin) Kick(_ context.Context, serverID string, cid uint64) error {
	if serverID != a.s.ID() {
		return fmt.Errorf("embedded: connection on server %s, not this server", serverID)
	}
	// An unknown client ID means the connection is already gone.
	_ = a.s.DisconnectClientByID(cid)
	return nil
}
