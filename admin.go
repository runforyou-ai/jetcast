package jetcast

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/nats-io/nats.go"
)

// ConnectionAdmin forcibly disconnects client connections. Disconnect uses
// it to close connections of clients that ignore the disconnect request.
type ConnectionAdmin interface {
	// Kick closes the connection with the given client ID on the NATS server
	// with the given server ID.
	Kick(ctx context.Context, serverID string, cid uint64) error
}

// SystemAdmin returns a ConnectionAdmin that kicks connections through the
// NATS system account: sys must be a connection to the system account.
func SystemAdmin(sys *nats.Conn) ConnectionAdmin { return systemAdmin{sys} }

type systemAdmin struct{ nc *nats.Conn }

func (a systemAdmin) Kick(ctx context.Context, serverID string, cid uint64) error {
	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
	}
	body, _ := json.Marshal(map[string]uint64{"cid": cid})
	resp, err := a.nc.RequestWithContext(ctx, "$SYS.REQ.SERVER."+serverID+".KICK", body)
	if err != nil {
		return fmt.Errorf("jetcast: kick %s/%d: %w", serverID, cid, err)
	}
	var out struct {
		Error *struct {
			Code        int    `json:"code"`
			Description string `json:"description"`
		} `json:"error"`
	}
	if err := json.Unmarshal(resp.Data, &out); err == nil && out.Error != nil {
		// The connection may already be gone.
		if out.Error.Code == 404 || (out.Error.Code == 500 && out.Error.Description == "no such client or leafnode id") {
			return nil
		}
		return fmt.Errorf("jetcast: kick %s/%d: %s", serverID, cid, out.Error.Description)
	}
	return nil
}
