package jetcast

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
)

// relayEntry is one relayed subscription of a connection.
type relayEntry struct {
	socket     string
	sid        string
	channel    Channel
	rec        *connRecord
	renewed    time.Time
	authorized time.Time
	// active is false until the subscription is confirmed; inactive entries
	// receive nothing.
	active bool
}

// relayChannel is the node's subscription to a channel's events and the
// connections it relays them to.
type relayChannel struct {
	sub     *nats.Subscription
	entries map[*relayEntry]struct{}
}

// relays holds the relayed subscriptions of this node. They live in memory:
// clients renew them and recreate them on another node when this one is gone.
type relays struct {
	s        *Server
	mu       sync.Mutex
	channels map[string]*relayChannel
	bySid    map[string]*relayEntry
	bySocket map[string]map[*relayEntry]struct{}
	total    int
	closed   bool
}

func newRelays(s *Server) *relays {
	return &relays{
		s:        s,
		channels: map[string]*relayChannel{},
		bySid:    map[string]*relayEntry{},
		bySocket: map[string]map[*relayEntry]struct{}{},
	}
}

func sidKey(socket, sid string) string { return socket + "/" + sid }

var (
	errTooManyRelays = errors.New("too many relayed channels")
	errClosing       = errors.New("server closing")
	errSidReused     = errors.New("sid already used for another channel")
)

// add registers an inactive relay of a channel to a connection; activate
// starts forwarding. Adding an existing sid for the same channel is a no-op.
// It returns once the node's subscription is registered with the NATS server.
func (r *relays) add(ctx context.Context, socket string, rec *connRecord, c Channel, sid string) (string, error) {
	now := time.Now()
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return CodeUnavailable, errClosing
	}
	if e := r.bySid[sidKey(socket, sid)]; e != nil {
		r.mu.Unlock()
		if e.channel != c {
			return CodeInvalid, errSidReused
		}
		return "", r.s.flush(ctx)
	}
	if len(r.bySocket[socket]) >= r.s.opts.Limits.RelaysPerConnection || r.total >= r.s.opts.Limits.RelaysPerNode {
		r.mu.Unlock()
		return CodeOverloaded, errTooManyRelays
	}
	key := c.String()
	rc := r.channels[key]
	if rc == nil {
		sub, err := r.s.nc.Subscribe(r.s.sub.ev(c), func(m *nats.Msg) { r.deliver(key, m) })
		if err != nil {
			r.mu.Unlock()
			return CodeUnavailable, err
		}
		rc = &relayChannel{sub: sub, entries: map[*relayEntry]struct{}{}}
		r.channels[key] = rc
	}
	e := &relayEntry{socket: socket, sid: sid, channel: c, rec: rec, renewed: now, authorized: now}
	rc.entries[e] = struct{}{}
	r.bySid[sidKey(socket, sid)] = e
	if r.bySocket[socket] == nil {
		r.bySocket[socket] = map[*relayEntry]struct{}{}
	}
	r.bySocket[socket][e] = struct{}{}
	r.total++
	r.mu.Unlock()
	if err := r.s.flush(ctx); err != nil {
		r.remove(socket, sid)
		return CodeUnavailable, err
	}
	return "", nil
}

// deliver forwards a channel event to every relayed connection.
func (r *relays) deliver(key string, m *nats.Msg) {
	r.mu.Lock()
	rc := r.channels[key]
	if rc == nil {
		r.mu.Unlock()
		return
	}
	targets := make([]*relayEntry, 0, len(rc.entries))
	for e := range rc.entries {
		if e.active {
			targets = append(targets, e)
		}
	}
	r.mu.Unlock()
	for _, e := range targets {
		out := nats.NewMsg(r.s.sub.connEvents(e.socket))
		out.Data = m.Data
		for k, v := range m.Header {
			out.Header[k] = v
		}
		out.Header.Set(HeaderChannel, key)
		out.Header.Set(HeaderSid, e.sid)
		if err := r.s.nc.PublishMsg(out); err != nil {
			r.s.log.Warn("jetcast: relay event", "error", err)
			continue
		}
		r.s.relayed.Add(1)
	}
}

// activate starts forwarding a registered relay.
func (r *relays) activate(socket, sid string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if e := r.bySid[sidKey(socket, sid)]; e != nil {
		e.active = true
	}
}

// has reports whether the node relays the channel to the connection.
func (r *relays) has(socket string, c Channel) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	for e := range r.bySocket[socket] {
		if e.channel == c && e.active {
			return true
		}
	}
	return false
}

// renew extends the leases of a connection's sids and returns those the node
// no longer relays. Subscriptions due for reauthorization are authorized
// again; denied ones are removed and the client is told.
func (r *relays) renew(ctx context.Context, socket string, rec *connRecord, sids []string) []string {
	now := time.Now()
	var missing []string
	var reauth []*relayEntry
	r.mu.Lock()
	for _, sid := range sids {
		e := r.bySid[sidKey(socket, sid)]
		if e == nil {
			missing = append(missing, sid)
			continue
		}
		e.renewed, e.rec = now, rec
		if now.Sub(e.authorized) >= r.s.opts.ReauthorizeInterval {
			reauth = append(reauth, e)
		}
	}
	r.mu.Unlock()
	for _, e := range reauth {
		ok, err := r.s.authorize(ctx, rec, e.channel)
		if err != nil {
			r.s.log.Warn("jetcast: reauthorize relay", "channel", e.channel.Name, "error", err)
			continue
		}
		if !ok {
			r.remove(socket, e.sid)
			r.s.control(socket, Control{Type: CtlDenied, Sid: e.sid, Channel: e.channel.String()})
			missing = append(missing, e.sid)
			continue
		}
		r.mu.Lock()
		e.authorized = now
		r.mu.Unlock()
	}
	return missing
}

// removeLocked removes an entry and drops the channel subscription when it
// was the last one. r.mu must be held.
func (r *relays) removeLocked(e *relayEntry) {
	key := e.channel.String()
	if rc := r.channels[key]; rc != nil {
		delete(rc.entries, e)
		if len(rc.entries) == 0 {
			_ = rc.sub.Unsubscribe()
			delete(r.channels, key)
		}
	}
	delete(r.bySid, sidKey(e.socket, e.sid))
	if set := r.bySocket[e.socket]; set != nil {
		delete(set, e)
		if len(set) == 0 {
			delete(r.bySocket, e.socket)
		}
	}
	r.total--
}

// remove stops relaying one sid.
func (r *relays) remove(socket, sid string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if e := r.bySid[sidKey(socket, sid)]; e != nil {
		r.removeLocked(e)
	}
}

// removeSockets stops relaying to the given connections.
func (r *relays) removeSockets(sockets []string, _ string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, socket := range sockets {
		for e := range r.bySocket[socket] {
			r.removeLocked(e)
		}
	}
}

// removeChannel stops relaying a channel to the given connections.
func (r *relays) removeChannel(c Channel, sockets []string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, socket := range sockets {
		for e := range r.bySocket[socket] {
			if e.channel == c {
				r.removeLocked(e)
			}
		}
	}
}

func (r *relays) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.total
}

// interruptAll tells every relayed connection to resubscribe and recover.
func (r *relays) interruptAll() {
	r.mu.Lock()
	entries := make([]*relayEntry, 0, len(r.bySid))
	for _, e := range r.bySid {
		entries = append(entries, e)
	}
	r.mu.Unlock()
	for _, e := range entries {
		r.s.control(e.socket, Control{Type: CtlInterrupted, Sid: e.sid, Channel: e.channel.String()})
	}
}

// closeAll interrupts every relay and stops relaying.
func (r *relays) closeAll() {
	r.mu.Lock()
	r.closed = true
	r.mu.Unlock()
	r.interruptAll()
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, e := range r.bySid {
		r.removeLocked(e)
	}
}

// janitor removes relays whose leases or connections expired.
func (r *relays) janitor() {
	interval := r.s.opts.RenewInterval
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-r.s.ctx.Done():
			return
		case now := <-t.C:
			r.mu.Lock()
			for _, e := range r.bySid {
				if now.Sub(e.renewed) > 3*interval || !e.rec.valid(now) {
					r.removeLocked(e)
				}
			}
			r.mu.Unlock()
		}
	}
}
