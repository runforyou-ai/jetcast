package jetcast

import (
	"context"
	"time"

	"github.com/nats-io/nats.go"
)

// SuspendRelays stops relaying to a socket without telling the client and
// returns a function restoring the same relays (same sids), simulating lost
// relayed events.
func (s *Server) SuspendRelays(socket string) (resume func()) {
	r := s.relays
	r.mu.Lock()
	var saved []*relayEntry
	for e := range r.bySocket[socket] {
		saved = append(saved, e)
	}
	for _, e := range saved {
		r.removeLocked(e)
	}
	r.mu.Unlock()
	return func() {
		ctx, cancel := context.WithTimeout(s.ctx, 5*time.Second)
		defer cancel()
		for _, e := range saved {
			if _, err := r.add(ctx, e.socket, e.rec, e.channel, e.sid); err != nil {
				panic(err)
			}
			r.activate(e.socket, e.sid)
		}
	}
}

// PauseNodeRequests stops answering requests addressed to this node and
// returns a function resuming them.
func (s *Server) PauseNodeRequests() (resume func()) {
	var sub *nats.Subscription
	for _, x := range s.subs {
		if x.Subject == s.sub.nodeRequests(s.node) {
			sub = x
		}
	}
	_ = sub.Unsubscribe()
	_ = s.nc.Flush()
	return func() {
		next, err := s.nc.Subscribe(sub.Subject, func(m *nats.Msg) {
			select {
			case s.work <- m:
			case <-s.ctx.Done():
			}
		})
		if err != nil {
			panic(err)
		}
		_ = s.nc.Flush()
		for i, x := range s.subs {
			if x == sub {
				s.subs[i] = next
			}
		}
	}
}

// OriginTag exposes the Jetcast-Origin value of a socket.
func OriginTag(socket string) string { return originTag(socket) }

// DropRelays stops relaying to a socket without telling the client.
func (s *Server) DropRelays(socket string) { s.SuspendRelays(socket) }

// ValidRequest exposes request validation, which also guards the overload
// reply path.
func (s *Server) ValidRequest(subject, reply string) bool {
	_, ok := s.validRequest(&nats.Msg{Subject: subject, Reply: reply})
	return ok
}
