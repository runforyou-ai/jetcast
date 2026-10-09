package client

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	mrand "math/rand/v2"
	"strconv"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nuid"
	"github.com/runforyou-ai/jetcast"
)

// Subscription states.
const (
	StateSubscribing = "subscribing"
	StateRecovering  = "recovering"
	StateSubscribed  = "subscribed"
	StateInterrupted = "interrupted"
	StateDenied      = "denied"
	StateLeft        = "left"
)

// State describes a subscription state change. Recovered and Reason are set
// when entering StateSubscribed: Recovered is false when events may have been
// missed (Reason says why), and the application should reload its data.
type State struct {
	State     string
	Recovered bool
	Reason    string
	Err       error
}

// Recovery limits of one resumption, beyond which the client gives up and
// reports too_far.
const (
	maxRecoverEvents = 10_000
	maxRecoverBytes  = 16 << 20
	maxBuffered      = 1000
)

// Subscription is a client's subscription to one channel.
type Subscription struct {
	c  *Client
	ch jetcast.Channel

	mu        sync.Mutex
	listeners map[string][]func(Event)
	all       []func(Event)
	stateFns  []func(State)
	state     string
	ready     chan struct{}
	readyErr  error
	readyDone bool

	// Current attempt.
	gen         int
	conn        *connection
	sid         string
	path        string
	node        string
	direct      *nats.Subscription
	recoverable bool
	recovering  bool
	buffer      []*nats.Msg
	lastEventAt time.Time
	failures    int // consecutive failed attempts, for backoff

	// Cursor.
	hasCursor   bool
	resetReason string // reported when a fresh cursor replaces a lost one
	epoch       string
	pos         uint64 // every event of the channel up to pos was delivered
	last        uint64 // sequence of the last delivered event, 0 if none
}

func newSubscription(c *Client, ch jetcast.Channel) *Subscription {
	return &Subscription{
		c: c, ch: ch, listeners: map[string][]func(Event){},
		state: StateSubscribing, ready: make(chan struct{}),
	}
}

// Channel returns the subscribed channel.
func (s *Subscription) Channel() jetcast.Channel { return s.ch }

// Listen registers a handler for one event name.
func (s *Subscription) Listen(event string, h func(Event)) *Subscription {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.listeners[event] = append(s.listeners[event], h)
	return s
}

// StopListening removes the handlers of one event name.
func (s *Subscription) StopListening(event string) *Subscription {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.listeners, event)
	return s
}

// State returns the current state.
func (s *Subscription) State() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.state
}

// ListenAll registers a handler for every event.
func (s *Subscription) ListenAll(h func(Event)) *Subscription {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.all = append(s.all, h)
	return s
}

// OnState registers a function called on state changes.
func (s *Subscription) OnState(f func(State)) *Subscription {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.stateFns = append(s.stateFns, f)
	return s
}

// Ready waits until the subscription is first established, or fails.
func (s *Subscription) Ready(ctx context.Context) error {
	select {
	case <-s.ready:
		s.mu.Lock()
		defer s.mu.Unlock()
		return s.readyErr
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Leave unsubscribes.
func (s *Subscription) Leave() {
	s.c.forget(s)
	s.mu.Lock()
	conn := s.conn
	s.gen++
	s.teardownLocked()
	s.setStateLocked(State{State: StateLeft})
	s.mu.Unlock()
	s.sendLeave(conn)
}

// sendLeave releases the relay of the current attempt, if any.
func (s *Subscription) sendLeave(conn *connection) {
	s.mu.Lock()
	node, sid, path := s.node, s.sid, s.path
	s.mu.Unlock()
	if conn == nil || path != jetcast.PathRelay {
		return
	}
	sendLeave(conn, s.c.sub.nodeRequest(conn.socket, node, "leave"), sid)
}

// sendLeave asks a node to stop relaying a sid without waiting for the
// answer. The server only accepts requests with a reply subject in the
// connection's namespace.
func sendLeave(conn *connection, subject, sid string) {
	b, _ := json.Marshal(jetcast.LeaveRequest{Sid: sid})
	_ = conn.nc.PublishRequest(subject, conn.nc.NewRespInbox(), b)
}

func (s *Subscription) fail(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.setStateLocked(State{State: StateDenied, Err: err})
}

// setStateLocked records a state change and notifies listeners.
func (s *Subscription) setStateLocked(st State) {
	s.state = st.State
	if !s.readyDone && (st.State == StateSubscribed || st.State == StateDenied || st.State == StateLeft) {
		s.readyDone = true
		if st.State != StateSubscribed {
			s.readyErr = st.Err
			if s.readyErr == nil {
				s.readyErr = fmt.Errorf("jetcast: subscription %s", st.State)
			}
		}
		close(s.ready)
	}
	for _, f := range s.stateFns {
		s.c.call(func() { f(st) })
	}
}

// current returns a function reporting whether attempt gen is still current.
func (s *Subscription) current(gen int) func() bool {
	return func() bool {
		s.mu.Lock()
		defer s.mu.Unlock()
		return s.gen == gen
	}
}

// closed ends the subscription when the client closes: it enters StateLeft,
// and Ready fails with ErrClosed if the subscription was never established.
func (s *Subscription) closed() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.gen++
	s.teardownLocked()
	s.setStateLocked(State{State: StateLeft, Err: ErrClosed})
}

// teardownLocked drops the direct subscription and pending messages.
func (s *Subscription) teardownLocked() {
	if s.direct != nil {
		_ = s.direct.Unsubscribe()
		s.direct = nil
	}
	s.buffer = nil
	s.recovering = false
}

// granted reports whether the connection was granted the channel.
func granted(conn *connection, name string) bool {
	for _, p := range conn.hello.Grants {
		if jetcast.MatchPattern(p, name) {
			return true
		}
	}
	return false
}

// resubscribe starts a new subscription attempt on conn. A non-empty sid
// restricts it to the attempt with that sid, ignoring stale signals.
func (s *Subscription) resubscribe(conn *connection, sid string) {
	s.mu.Lock()
	// Attempts always bind to the current connection: a caller may hold a
	// connection that was replaced meanwhile. Reading it under s.mu orders
	// this with adopt, which resubscribes after switching connections.
	cur := s.c.current()
	if cur == nil {
		s.mu.Unlock()
		return
	}
	if cur != conn {
		if sid != "" {
			s.mu.Unlock()
			return
		}
		conn = cur
	}
	if s.state == StateDenied || s.state == StateLeft || sid != "" && sid != s.sid {
		s.mu.Unlock()
		return
	}
	oldConn, oldPath, oldNode, oldSid := s.conn, s.path, s.node, s.sid
	s.gen++
	gen := s.gen
	s.teardownLocked()
	newSid := nuid.Next()
	s.conn, s.sid, s.path, s.node = conn, newSid, "", ""
	if s.state == StateSubscribed || s.state == StateRecovering {
		s.setStateLocked(State{State: StateInterrupted})
	}
	s.mu.Unlock()
	if oldConn == conn && oldPath == jetcast.PathRelay {
		sendLeave(conn, s.c.sub.nodeRequest(conn.socket, oldNode, "leave"), oldSid)
	}
	go s.attempt(conn, gen, newSid)
}

// retryLater schedules a new attempt after a failure, backing off
// exponentially from one second up to 30 seconds while attempts keep failing.
func (s *Subscription) retryLater(conn *connection, gen int) {
	s.mu.Lock()
	n := min(s.failures, 5)
	s.failures++
	s.mu.Unlock()
	base := time.Second << n
	delay := base + time.Duration(mrand.Int64N(int64(base)))
	time.AfterFunc(min(delay, 30*time.Second), func() {
		s.mu.Lock()
		stale := s.gen != gen
		sid := s.sid
		s.mu.Unlock()
		if !stale && s.c.current() == conn {
			s.resubscribe(conn, sid)
		}
	})
}

// attempt subscribes on conn with the sid bound to generation gen: it sets
// up delivery, then asks the server. A stale attempt never uses a newer sid.
func (s *Subscription) attempt(conn *connection, gen int, sid string) {
	path := jetcast.PathRelay
	if s.ch.Kind == jetcast.KindPublic || granted(conn, s.ch.Name) {
		path = jetcast.PathDirect
	}
	if path == jetcast.PathDirect {
		sub, err := conn.nc.Subscribe(s.c.sub.ev(s.ch), func(m *nats.Msg) {
			s.mu.Lock()
			current := s.gen == gen
			s.mu.Unlock()
			if current {
				s.onMessage(conn, m, false)
			}
		})
		if err == nil {
			err = conn.nc.Flush()
		}
		if err != nil {
			if sub != nil {
				_ = sub.Unsubscribe()
			}
			s.retryLater(conn, gen)
			return
		}
		s.mu.Lock()
		if s.gen != gen {
			s.mu.Unlock()
			_ = sub.Unsubscribe()
			return
		}
		s.direct = sub
		s.mu.Unlock()
	}

	s.mu.Lock()
	stale := s.gen != gen
	s.mu.Unlock()
	if stale {
		return
	}
	var resp jetcast.SubResponse
	err := conn.request(s.c.ctx, s.c.sub.request(conn.socket, "sub"),
		jetcast.SubRequest{Channel: s.ch.String(), Sid: sid, Path: path}, &resp, false, s.current(gen))

	s.mu.Lock()
	if s.gen != gen {
		s.mu.Unlock()
		if err == nil && resp.Path == jetcast.PathRelay {
			sendLeave(conn, s.c.sub.nodeRequest(conn.socket, resp.Node, "leave"), sid)
		}
		return
	}
	if err != nil || resp.Error != nil && resp.Error.Code != jetcast.CodeDenied {
		s.teardownLocked()
		s.mu.Unlock()
		s.retryLater(conn, gen)
		return
	}
	if resp.Error != nil {
		s.teardownLocked()
		s.setStateLocked(State{State: StateDenied, Reason: jetcast.CodeDenied, Err: errors.New("jetcast: channel denied")})
		s.mu.Unlock()
		s.c.forget(s)
		return
	}
	if resp.Path == jetcast.PathRelay && s.direct != nil {
		_ = s.direct.Unsubscribe()
		s.direct = nil
	}
	s.path, s.node, s.recoverable = resp.Path, resp.Node, resp.Recoverable
	s.failures = 0
	if resp.Path == jetcast.PathRelay {
		conn.relayAt(resp.Node)
	}
	if !resp.Recoverable {
		s.drainLocked()
		s.setStateLocked(State{State: StateSubscribed, Recovered: false, Reason: jetcast.ReasonEphemeral})
		s.mu.Unlock()
		return
	}
	if !s.hasCursor || s.epoch != resp.Epoch {
		reason := jetcast.ReasonInitial
		switch {
		case s.resetReason != "":
			reason, s.resetReason = s.resetReason, ""
		case s.hasCursor:
			reason = jetcast.ReasonEpoch
		}
		s.hasCursor, s.epoch, s.pos, s.last = true, resp.Epoch, resp.Position, resp.Head
		s.lastEventAt = time.Now()
		s.setStateLocked(State{State: StateSubscribed, Recovered: false, Reason: reason})
		s.drainLocked()
		s.mu.Unlock()
		return
	}
	s.startRecoveryLocked(conn, 0)
	s.mu.Unlock()
}

// onMessage handles a delivered event, direct or relayed.
func (s *Subscription) onMessage(conn *connection, m *nats.Msg, relayed bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.conn != conn || relayed && m.Header.Get(jetcast.HeaderSid) != s.sid {
		return
	}
	if s.state != StateSubscribed || s.recovering {
		if len(s.buffer) < maxBuffered {
			s.buffer = append(s.buffer, m)
		} else if s.recovering {
			s.giveUpLocked(conn, jetcast.ReasonTooFar)
		}
		return
	}
	s.liveLocked(conn, m)
}

// drainLocked processes buffered messages in order.
func (s *Subscription) drainLocked() {
	buf := s.buffer
	s.buffer = nil
	for i, m := range buf {
		if s.recovering {
			s.buffer = append(s.buffer, buf[i:]...)
			return
		}
		s.liveLocked(s.conn, m)
	}
}

// liveLocked applies the gap rules to a live event.
func (s *Subscription) liveLocked(conn *connection, m *nats.Msg) {
	s.lastEventAt = time.Now()
	if !s.recoverable {
		s.deliverLocked(m, 0)
		return
	}
	seq := parseUint(m.Header.Get(jetcast.HeaderSequence))
	prev := parseUint(m.Header.Get(jetcast.HeaderLastSequence))
	switch {
	case seq == 0:
		s.deliverLocked(m, 0)
	case seq <= s.pos:
		// Duplicate. A sequence below the last delivered one hints at a
		// recreated stream: check heads soon.
		if seq < s.last {
			s.c.checkHeadsSoon()
		}
	case s.last != 0 && prev == s.last || s.pos+1 >= seq:
		s.deliverLocked(m, seq)
		s.pos, s.last = seq, seq
	default:
		s.buffer = append([]*nats.Msg{m}, s.buffer...)
		s.startRecoveryLocked(conn, seq)
	}
}

// deliverLocked hands an event to the listeners, except for events this
// client caused itself.
func (s *Subscription) deliverLocked(m *nats.Msg, seq uint64) {
	origin := m.Header.Get(jetcast.HeaderOrigin)
	if origin != "" && s.c.ownOrigin(origin) {
		return
	}
	ev := Event{
		Name: m.Header.Get(jetcast.HeaderEvent), Channel: s.ch, Data: m.Data,
		ID: m.Header.Get(jetcast.HeaderID), Sequence: seq,
	}
	if ts := m.Header.Get(jetcast.HeaderTimeStamp); ts != "" {
		ev.Time, _ = time.Parse(time.RFC3339Nano, ts)
	}
	handlers := append(append([]func(Event){}, s.listeners[ev.Name]...), s.all...)
	for _, h := range handlers {
		s.c.call(func() { h(ev) })
	}
}

// giveUpLocked abandons a recovery that cannot keep up and starts over with
// a fresh cursor, reporting reason.
func (s *Subscription) giveUpLocked(conn *connection, reason string) {
	s.recovering = false
	s.buffer = nil
	s.hasCursor = false
	s.resetReason = reason
	go s.resubscribe(conn, s.sid)
}

// startRecoveryLocked recovers events after the cursor and before upTo (0
// for the end of the stream), then processes buffered events.
func (s *Subscription) startRecoveryLocked(conn *connection, upTo uint64) {
	s.recovering = true
	if s.state != StateSubscribed {
		s.setStateLocked(State{State: StateRecovering})
	}
	gen, epoch, pos := s.gen, s.epoch, s.pos
	go s.recoverLoop(conn, gen, epoch, pos, upTo)
}

func (s *Subscription) recoverLoop(conn *connection, gen int, epoch string, pos, upTo uint64) {
	events, bytes, broken := 0, 0, 0
	for {
		res, msgs, err := s.recoverBatch(conn, gen, jetcast.RecoverRequest{
			Channel: s.ch.String(), Epoch: epoch, Pos: pos, UpTo: upTo,
		})
		s.mu.Lock()
		if s.gen != gen {
			s.mu.Unlock()
			return
		}
		if err == nil && res.Error != nil && res.Error.Code == jetcast.CodeDenied {
			s.endLocked(jetcast.CtlDenied)
			s.mu.Unlock()
			s.c.forget(s)
			return
		}
		if err != nil || res.Error != nil {
			s.recovering = false
			s.mu.Unlock()
			s.retryLater(conn, gen)
			return
		}
		if !res.Recovered {
			s.resetLocked(res.Epoch, res.Position, res.Head, res.Reason)
			s.mu.Unlock()
			return
		}
		if !completeBatch(msgs, res, pos) {
			// Events of the batch were lost on the way: retry from the same
			// position rather than skipping them.
			if broken++; broken > 3 {
				s.recovering = false
				s.mu.Unlock()
				s.retryLater(conn, gen)
				return
			}
			s.mu.Unlock()
			continue
		}
		for _, m := range msgs {
			seq := parseUint(m.Header.Get(jetcast.HeaderSequence))
			if seq > s.pos {
				s.deliverLocked(m, seq)
				s.pos, s.last = seq, seq
				events++
				bytes += len(m.Data)
			}
		}
		if res.Next > s.pos {
			s.pos = res.Next
		}
		pos = s.pos
		if events > maxRecoverEvents || bytes > maxRecoverBytes {
			s.resetLocked(res.Epoch, res.Position, 0, jetcast.ReasonTooFar)
			s.mu.Unlock()
			return
		}
		if !res.More {
			s.recovering = false
			s.lastEventAt = time.Now()
			if s.state != StateSubscribed {
				s.setStateLocked(State{State: StateSubscribed, Recovered: true})
			}
			s.drainLocked()
			s.mu.Unlock()
			return
		}
		s.mu.Unlock()
	}
}

// completeBatch reports whether a recovery batch arrived intact: as many
// events as the server sent, each continuing the previous one from pos.
func completeBatch(msgs []*nats.Msg, res jetcast.RecoverResult, pos uint64) bool {
	if len(msgs) != res.Count {
		return false
	}
	prev := pos
	for _, m := range msgs {
		if parseUint(m.Header.Get(jetcast.HeaderLastSequence)) != prev {
			return false
		}
		prev = parseUint(m.Header.Get(jetcast.HeaderSequence))
	}
	return true
}

// resetLocked restarts the cursor at the stream's current position after a
// failed recovery and reports recovered=false.
func (s *Subscription) resetLocked(epoch string, position, head uint64, reason string) {
	if epoch != "" {
		s.epoch = epoch
	}
	s.pos, s.last = position, head
	s.recovering = false
	s.lastEventAt = time.Now()
	s.setStateLocked(State{State: StateSubscribed, Recovered: false, Reason: reason})
	s.drainLocked()
}

// recoverBatch requests one recovery batch and collects its events.
func (s *Subscription) recoverBatch(conn *connection, gen int, req jetcast.RecoverRequest) (jetcast.RecoverResult, []*nats.Msg, error) {
	var res jetcast.RecoverResult
	if err := conn.acquire(s.c.ctx, false); err != nil {
		return res, nil, err
	}
	defer conn.release()
	if !s.current(gen)() {
		return res, nil, errStale
	}
	inbox := conn.nc.NewRespInbox()
	ch := make(chan *nats.Msg, 4096)
	sub, err := conn.nc.ChanSubscribe(inbox, ch)
	if err != nil {
		return res, nil, err
	}
	defer func() { _ = sub.Unsubscribe() }()
	b, _ := json.Marshal(req)
	if err := conn.nc.PublishRequest(s.c.sub.request(conn.socket, "recover"), inbox, b); err != nil {
		return res, nil, err
	}
	timer := time.NewTimer(15 * time.Second)
	defer timer.Stop()
	var msgs []*nats.Msg
	for {
		select {
		case m := <-ch:
			if m.Header.Get(jetcast.HeaderStatus) == "done" {
				err := json.Unmarshal(m.Data, &res)
				return res, msgs, err
			}
			msgs = append(msgs, m)
		case <-timer.C:
			return res, nil, errors.New("jetcast: recover timed out")
		case <-s.c.ctx.Done():
			return res, nil, ErrClosed
		}
	}
}

// removed ends the subscription after the server denied or removed it.
func (s *Subscription) removed(conn *connection, sid, reason string) {
	s.mu.Lock()
	if s.conn != conn || sid != "" && sid != s.sid {
		s.mu.Unlock()
		return
	}
	s.endLocked(reason)
	s.mu.Unlock()
	s.c.forget(s)
}

// endLocked ends the current attempt for good after the server denied or
// removed the subscription.
func (s *Subscription) endLocked(reason string) {
	s.gen++
	s.teardownLocked()
	s.setStateLocked(State{State: StateDenied, Reason: reason, Err: fmt.Errorf("jetcast: channel %s", reason)})
}

// relayLease returns the node and sid of a relayed subscription on conn.
func (s *Subscription) relayLease(conn *connection) (string, string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.conn != conn || s.path != jetcast.PathRelay || s.state != StateSubscribed && s.state != StateRecovering {
		return "", ""
	}
	return s.node, s.sid
}

// headsTarget reports whether the subscription needs a heads check and which
// node answers it ("" for any node).
func (s *Subscription) headsTarget(conn *connection, idleSince time.Time) (string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.conn != conn || s.state != StateSubscribed || !s.recoverable || s.recovering || s.lastEventAt.After(idleSince) {
		return "", false
	}
	if s.path == jetcast.PathRelay {
		return s.node, true
	}
	return "", true
}

func (s *Subscription) cursorEpoch() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.epoch
}

// checkHead applies a heads response to the cursor.
func (s *Subscription) checkHead(conn *connection, resp jetcast.HeadsResponse, head uint64, ok bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.conn != conn || s.state != StateSubscribed || s.recovering {
		return
	}
	switch {
	case resp.Epoch != s.epoch:
		s.startRecoveryLocked(conn, 0)
	case !ok:
	case head > s.last:
		s.startRecoveryLocked(conn, 0)
	case resp.First <= s.pos+1:
		if resp.Last > s.pos {
			s.pos = resp.Last
		}
	default:
		s.resetLocked(resp.Epoch, resp.Last, head, jetcast.ReasonExpired)
	}
}

func (s *Subscription) String() string {
	return s.ch.String() + "#" + strconv.Itoa(s.gen)
}
