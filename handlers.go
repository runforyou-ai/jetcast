package jetcast

import (
	"context"
	"encoding/json"
	"fmt"
	"runtime/debug"
	"strconv"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// requestTimeout bounds handling one client request.
const requestTimeout = 10 * time.Second

func (s *Server) worker() {
	for {
		select {
		case <-s.ctx.Done():
			return
		case m := <-s.work:
			s.handleRequest(m)
		}
	}
}

// respond sends a JSON response, marked done for recovery replies.
func (s *Server) respond(m *nats.Msg, v any) {
	b, err := json.Marshal(v)
	if err != nil {
		s.log.Error("jetcast: encode response", "error", err)
		return
	}
	resp := nats.NewMsg(m.Reply)
	resp.Header.Set(HeaderStatus, statusDone)
	resp.Data = b
	if err := s.nc.PublishMsg(resp); err != nil {
		s.log.Warn("jetcast: respond", "error", err)
	}
}

func (s *Server) respondError(m *nats.Msg, code, message string) {
	if m.Reply == "" {
		return
	}
	s.respond(m, struct {
		Error ErrorBody `json:"error"`
	}{ErrorBody{Code: code, Message: message}})
}

// parseRequest splits <p>.rq.<socket>.<op> and <p>.rq.<socket>.n.<node>.<op>.
func (s *Server) parseRequest(subject string) (socket, op string, node bool, ok bool) {
	rest, found := strings.CutPrefix(subject, s.sub.p+".rq.")
	if !found {
		return "", "", false, false
	}
	parts := strings.Split(rest, ".")
	switch {
	case len(parts) == 2:
		return parts[0], parts[1], false, true
	case len(parts) == 4 && parts[1] == "n" && parts[2] == s.node:
		return parts[0], parts[3], true, true
	}
	return "", "", false, false
}

// validRequest returns the requesting socket when the subject is a request
// and its reply subject lies in the socket's own namespace. Requests failing
// it are dropped without any reply, so that clients cannot make the server
// publish elsewhere.
func (s *Server) validRequest(m *nats.Msg) (string, bool) {
	socket, _, _, ok := s.parseRequest(m.Subject)
	if !ok || ValidateSocketID(socket) != nil || !strings.HasPrefix(m.Reply, s.sub.connReplies(socket)+".") {
		return "", false
	}
	return socket, true
}

func (s *Server) handleRequest(m *nats.Msg) {
	if _, ok := s.validRequest(m); !ok {
		return
	}
	socket, op, node, _ := s.parseRequest(m.Subject)
	// A panicking application callback fails the request, not the process.
	defer func() {
		if r := recover(); r != nil {
			s.log.Error("jetcast: request handler panicked", "op", op, "panic", r, "stack", string(debug.Stack()))
			s.respondError(m, CodeUnavailable, "internal error")
		}
	}()
	// Leaving only drops the connection's own relay, so it needs neither the
	// connection record nor a request slot: clients can always release relays.
	if op == opLeave && node {
		s.handleLeave(m, socket)
		return
	}
	s.inflightMu.Lock()
	if s.inflight[socket] >= s.opts.Limits.ConcurrentRequests {
		s.inflightMu.Unlock()
		s.overloaded.Add(1)
		s.respondError(m, CodeOverloaded, "too many concurrent requests")
		return
	}
	s.inflight[socket]++
	s.inflightMu.Unlock()
	defer func() {
		s.inflightMu.Lock()
		if s.inflight[socket]--; s.inflight[socket] <= 0 {
			delete(s.inflight, socket)
		}
		s.inflightMu.Unlock()
	}()

	ctx, cancel := context.WithTimeout(s.ctx, requestTimeout)
	defer cancel()
	rec, err := s.reg.get(ctx, socket, op == opRenew || op == opHeads)
	if err != nil {
		s.log.Warn("jetcast: read connection record", "error", err)
		s.respondError(m, CodeUnavailable, "registry unavailable")
		return
	}
	if !rec.valid(time.Now()) {
		s.respondError(m, CodeDenied, "connection not registered")
		return
	}
	switch {
	case op == opHello && !node:
		s.handleHello(ctx, m, socket, rec)
	case op == opSub && !node:
		s.handleSub(ctx, m, socket, rec)
	case op == opHeads:
		s.handleHeads(ctx, m, socket, rec, node)
	case op == opRecover:
		s.handleRecover(ctx, m, socket, rec)
	case op == opRenew && node:
		s.handleRenew(ctx, m, socket, rec)
	default:
		s.respondError(m, CodeInvalid, "unknown operation")
	}
}

// userOf rebuilds the authorizer's view of a connection's user. Info is the
// JSON encoding of the authenticated User.Info, as a json.RawMessage.
func userOf(rec *connRecord) User {
	u := User{ID: rec.User, Session: rec.Session, ExpiresAt: time.UnixMilli(rec.ExpiresAt)}
	if len(rec.Info) > 0 {
		u.Info = rec.Info
	}
	return u
}

func (s *Server) handleHello(ctx context.Context, m *nats.Msg, socket string, rec *connRecord) {
	epoch, _, _, err := s.hist.state(ctx)
	if err != nil {
		s.respondError(m, CodeUnavailable, "stream unavailable")
		return
	}
	s.respond(m, HelloResponse{
		User: rec.User, Info: rec.Info, Grants: rec.Grants, ExpiresAt: rec.ExpiresAt,
		Epoch: epoch, MaxAgeMs: s.maxAge.Milliseconds(), RenewMs: s.opts.RenewInterval.Milliseconds(),
		Node: s.node, Prefix: s.sub.p, Origin: originTag(socket), MaxRequests: s.opts.Limits.ConcurrentRequests,
	})
}

// authorize runs the channel authorizer for a private channel. A panicking
// authorizer counts as a failed one.
func (s *Server) authorize(ctx context.Context, rec *connRecord, c Channel) (ok bool, err error) {
	f, params := s.route(c.Name)
	if f == nil {
		return false, nil
	}
	defer func() {
		if r := recover(); r != nil {
			s.log.Error("jetcast: channel authorizer panicked", "channel", c.Name, "panic", r, "stack", string(debug.Stack()))
			ok, err = false, fmt.Errorf("jetcast: channel authorizer panicked: %v", r)
		}
	}()
	return f(ctx, userOf(rec), params)
}

// subscribed reports whether a connection may read a channel's events without
// asking the authorizer: public channels, granted channels and channels this
// node relays to it.
func (s *Server) subscribed(rec *connRecord, socket string, c Channel) bool {
	return c.Kind == KindPublic || rec.granted(c.Name) || s.relays.has(socket, c)
}

// canRead reports whether a connection may read a channel's events.
func (s *Server) canRead(ctx context.Context, rec *connRecord, socket string, c Channel) (bool, error) {
	if s.subscribed(rec, socket, c) {
		return true, nil
	}
	return s.authorize(ctx, rec, c)
}

func (s *Server) handleSub(ctx context.Context, m *nats.Msg, socket string, rec *connRecord) {
	var req SubRequest
	if err := json.Unmarshal(m.Data, &req); err != nil {
		s.respondError(m, CodeInvalid, "malformed request")
		return
	}
	c, err := ParseChannel(req.Channel)
	if err != nil || req.Sid == "" || len(req.Sid) > 64 {
		s.respondError(m, CodeInvalid, "invalid channel or sid")
		return
	}
	resp := SubResponse{Path: PathDirect, Node: s.node, Recoverable: !s.opts.Config.ephemeral(c.Name)}
	if c.Kind == KindPrivate && (req.Path == PathRelay || !rec.granted(c.Name)) {
		ok, err := s.authorize(ctx, rec, c)
		if err != nil {
			s.log.Warn("jetcast: channel authorizer failed", "channel", c.Name, "error", err)
			s.respondError(m, CodeUnavailable, "authorization failed")
			return
		}
		if !ok {
			s.respondError(m, CodeDenied, "channel denied")
			return
		}
		if code, err := s.relays.add(ctx, socket, rec, c, req.Sid); err != nil {
			s.respondError(m, code, err.Error())
			return
		}
		resp.Path = PathRelay
		// A Disconnect may have revoked the connection while the authorizer
		// ran; it marks the record before removing relays, so reading the
		// record after adding the relay closes the race.
		fresh, err := s.reg.get(ctx, socket, false)
		if err != nil || !fresh.valid(time.Now()) {
			s.relays.remove(socket, req.Sid)
			if err != nil {
				s.respondError(m, CodeUnavailable, "registry unavailable")
			} else {
				s.respondError(m, CodeDenied, "connection not registered")
			}
			return
		}
		s.relays.activate(socket, req.Sid)
	}
	if resp.Recoverable {
		epoch, _, last, err := s.hist.state(ctx)
		if err == nil {
			resp.Head, err = s.hist.head(ctx, c)
		}
		if err != nil {
			if resp.Path == PathRelay {
				s.relays.remove(socket, req.Sid)
			}
			s.respondError(m, CodeUnavailable, "stream unavailable")
			return
		}
		resp.Epoch, resp.Position = epoch, last
	}
	s.respond(m, resp)
}

func (s *Server) handleHeads(ctx context.Context, m *nats.Msg, socket string, rec *connRecord, node bool) {
	var req HeadsRequest
	if err := json.Unmarshal(m.Data, &req); err != nil || len(req.Channels) > s.opts.Limits.HeadsChannels {
		s.respondError(m, CodeInvalid, "malformed request")
		return
	}
	// The stream's last sequence is read before the heads and its first
	// sequence after them, so a client advancing to Last never skips an event.
	epoch, _, last, err := s.hist.state(ctx)
	if err != nil {
		s.respondError(m, CodeUnavailable, "stream unavailable")
		return
	}
	resp := HeadsResponse{Epoch: epoch, Last: last, Heads: map[string]uint64{}}
	if req.Epoch == epoch {
		for _, name := range req.Channels {
			c, err := ParseChannel(name)
			if err != nil {
				resp.Denied = append(resp.Denied, name)
				continue
			}
			// Heads never run authorizers: clients only check channels they
			// subscribed to, which are public, granted or relayed. One
			// request could otherwise run hundreds of authorizer calls.
			ok := s.relays.has(socket, c)
			if !node {
				ok = s.subscribed(rec, socket, c)
			}
			if !ok {
				resp.Denied = append(resp.Denied, name)
				continue
			}
			head, err := s.hist.head(ctx, c)
			if err != nil {
				s.respondError(m, CodeUnavailable, "stream unavailable")
				return
			}
			resp.Heads[name] = head
		}
	}
	epoch2, first, _, err := s.hist.state(ctx)
	if err != nil {
		s.respondError(m, CodeUnavailable, "stream unavailable")
		return
	}
	resp.First = first
	if epoch2 != epoch {
		resp.Epoch, resp.Heads = epoch2, nil
	}
	s.respond(m, resp)
}

func (s *Server) handleRecover(ctx context.Context, m *nats.Msg, socket string, rec *connRecord) {
	var req RecoverRequest
	if err := json.Unmarshal(m.Data, &req); err != nil {
		s.respondError(m, CodeInvalid, "malformed request")
		return
	}
	c, err := ParseChannel(req.Channel)
	if err != nil || s.opts.Config.ephemeral(c.Name) || req.Pos == ^uint64(0) {
		s.respondError(m, CodeInvalid, "invalid channel or position")
		return
	}
	ok, err := s.canRead(ctx, rec, socket, c)
	if err != nil {
		s.respondError(m, CodeUnavailable, "authorization failed")
		return
	}
	if !ok {
		s.respondError(m, CodeDenied, "channel denied")
		return
	}
	s.recoveries.Add(1)
	lim := recoverLimits{count: s.opts.Limits.RecoverCount, bytes: s.opts.Limits.RecoverBytes}
	res, err := s.hist.recoverBatch(ctx, c, req, lim, func(msg *jetstream.RawStreamMsg, prev uint64) error {
		out := nats.NewMsg(m.Reply)
		out.Data = msg.Data
		for _, h := range []string{HeaderEvent, HeaderID, HeaderOrigin} {
			if v := msg.Header.Get(h); v != "" {
				out.Header.Set(h, v)
			}
		}
		out.Header.Set(HeaderStatus, statusEvent)
		out.Header.Set(HeaderChannel, c.String())
		out.Header.Set(HeaderSequence, strconv.FormatUint(msg.Sequence, 10))
		out.Header.Set(HeaderLastSequence, strconv.FormatUint(prev, 10))
		out.Header.Set(HeaderTimeStamp, msg.Time.UTC().Format(time.RFC3339Nano))
		return s.nc.PublishMsg(out)
	})
	if err != nil {
		s.recFailures.Add(1)
		s.log.Warn("jetcast: recover", "channel", c.Name, "error", err)
		s.respondError(m, CodeUnavailable, "stream unavailable")
		return
	}
	if !res.Recovered {
		s.recFailures.Add(1)
	}
	s.respond(m, res)
}

func (s *Server) handleRenew(ctx context.Context, m *nats.Msg, socket string, rec *connRecord) {
	var req RenewRequest
	if err := json.Unmarshal(m.Data, &req); err != nil {
		s.respondError(m, CodeInvalid, "malformed request")
		return
	}
	s.respond(m, RenewResponse{Missing: s.relays.renew(ctx, socket, rec, req.Sids)})
}

func (s *Server) handleLeave(m *nats.Msg, socket string) {
	var req LeaveRequest
	if err := json.Unmarshal(m.Data, &req); err != nil {
		s.respondError(m, CodeInvalid, "malformed request")
		return
	}
	s.relays.remove(socket, req.Sid)
	s.respond(m, struct{}{})
}
