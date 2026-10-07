// Package client is the Go client of jetcast, for executors, command-line
// tools and other non-browser clients. Its API mirrors the TypeScript SDK.
//
// Event, state and status callbacks run one at a time on a single goroutine,
// in order; they must return quickly. A Client keeps one NATS connection at a time. Every connection uses a fresh
// socket ID; when it drops or its credentials near expiry, the client opens a
// new one and resubscribes every channel, recovering missed events.
package client

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math/big"
	mrand "math/rand/v2"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/runforyou-ai/jetcast"
)

// ErrUnauthorized returned by GetToken stops the client for good.
var ErrUnauthorized = errors.New("jetcast: unauthorized")

// ErrClosed is returned by operations on a closed client.
var ErrClosed = errors.New("jetcast: client closed")

// Options configure a Client.
type Options struct {
	// Servers lists NATS URLs, such as "wss://example.com/nats" or
	// "nats://localhost:4222".
	Servers []string
	// GetToken returns the credential for a new connection. Returning an
	// error wrapping ErrUnauthorized stops the client.
	GetToken func(ctx context.Context) (string, error)
	// Prefix must match the server's Config.Prefix, "jetcast" by default.
	Prefix string
	// HeadsInterval is how often idle channels are checked for missed
	// events, 30 seconds by default.
	HeadsInterval time.Duration
	// NATSOptions are applied to every connection, after jetcast's own.
	NATSOptions []nats.Option
	Logger      *slog.Logger
}

// Status is the connection status of a client.
type Status string

// Client statuses.
const (
	StatusConnecting   Status = "connecting"
	StatusConnected    Status = "connected"
	StatusReconnecting Status = "reconnecting"
	StatusStopped      Status = "stopped"
)

// Event is a received event.
type Event struct {
	Name     string
	Channel  jetcast.Channel
	Data     []byte
	ID       string
	Sequence uint64 // 0 for ephemeral channels
	Time     time.Time
}

// Client is a jetcast client. It is safe for concurrent use.
type Client struct {
	opts Options
	sub  subjectsOf
	log  *slog.Logger

	mu         sync.Mutex
	conn       *connection
	status     Status
	statusFn   []func(Status)
	subs       map[string]*Subscription
	sockets    map[string]bool // every socket this client used, for toOthers
	closed     bool
	stopErr    error
	connecting bool

	// headsNow requests a heads check of every channel on the next tick;
	// headsAsked throttles such requests.
	headsNow   atomic.Bool
	headsAsked atomic.Int64

	ctx    context.Context
	cancel context.CancelFunc
	// callbacks to the application, run in order by runCalls. The queue is
	// unbounded so that queueing never blocks, even from a callback.
	callsMu sync.Mutex
	calls   []func()
	wake    chan struct{}
	wg      sync.WaitGroup
	ready   chan struct{}
}

// connection is one NATS connection with its own socket.
type connection struct {
	nc        *nats.Conn
	socket    string
	hello     jetcast.HelloResponse
	evSub     *nats.Subscription
	ctlSub    *nats.Subscription
	refreshAt time.Time
}

type subjectsOf struct{ p string }

func (s subjectsOf) conn(socket string) string        { return s.p + ".c." + socket }
func (s subjectsOf) request(socket, op string) string { return s.p + ".rq." + socket + "." + op }
func (s subjectsOf) nodeRequest(socket, node, op string) string {
	return s.p + ".rq." + socket + ".n." + node + "." + op
}
func (s subjectsOf) ev(c jetcast.Channel) string { return s.p + ".ev." + c.String() }

const base62 = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"

// newSocketID returns 22 random base62 characters.
func newSocketID() string {
	b := make([]byte, 22)
	max := big.NewInt(62)
	for i := range b {
		n, _ := rand.Int(rand.Reader, max)
		b[i] = base62[n.Int64()]
	}
	return string(b)
}

// Connect connects and returns once the first connection is established.
func Connect(ctx context.Context, opts Options) (*Client, error) {
	if len(opts.Servers) == 0 || opts.GetToken == nil {
		return nil, errors.New("jetcast: Servers and GetToken are required")
	}
	if opts.Prefix == "" {
		opts.Prefix = "jetcast"
	}
	if opts.HeadsInterval <= 0 {
		opts.HeadsInterval = 30 * time.Second
	}
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}
	c := &Client{
		opts:    opts,
		sub:     subjectsOf{opts.Prefix},
		log:     opts.Logger,
		status:  StatusConnecting,
		subs:    map[string]*Subscription{},
		sockets: map[string]bool{},
		wake:    make(chan struct{}, 1),
		ready:   make(chan struct{}),
	}
	c.ctx, c.cancel = context.WithCancel(context.Background())
	// The callback runner is not waited for by Close, so callbacks may close
	// the client.
	go c.runCalls()
	c.wg.Go(c.runTimers)
	c.startConnecting(false)
	select {
	case <-c.ready:
		return c, nil
	case <-ctx.Done():
		_ = c.Close()
		return nil, ctx.Err()
	case <-c.ctx.Done():
		c.mu.Lock()
		err := c.stopErr
		c.mu.Unlock()
		if err == nil {
			err = ErrClosed
		}
		return nil, err
	}
}

// SocketID returns the socket ID of the current connection. Send it with
// application requests so their broadcasts can exclude this client.
func (c *Client) SocketID() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.conn == nil {
		return ""
	}
	return c.conn.socket
}

// Status returns the connection status.
func (c *Client) Status() Status {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.status
}

// OnStatus registers a function called on status changes.
func (c *Client) OnStatus(f func(Status)) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.statusFn = append(c.statusFn, f)
}

// Info returns the user's public information sent by the server, as JSON.
func (c *Client) Info() json.RawMessage {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.conn == nil {
		return nil
	}
	return c.conn.hello.Info
}

// Close closes the client.
func (c *Client) Close() error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil
	}
	c.closed = true
	conn := c.conn
	c.conn = nil
	subs := make([]*Subscription, 0, len(c.subs))
	for _, s := range c.subs {
		subs = append(subs, s)
	}
	c.setStatusLocked(StatusStopped)
	c.mu.Unlock()
	for _, s := range subs {
		s.sendLeave(conn)
	}
	if conn != nil {
		conn.nc.Close()
	}
	c.cancel()
	c.wg.Wait()
	return nil
}

func (c *Client) setStatusLocked(s Status) {
	if c.status == s {
		return
	}
	c.status = s
	for _, f := range c.statusFn {
		c.call(func() { f(s) })
	}
}

// safeCall runs a callback, logging instead of crashing when it panics.
func (c *Client) safeCall(f func()) {
	defer func() {
		if r := recover(); r != nil {
			c.log.Error("jetcast: callback panicked", "panic", r)
		}
	}()
	f()
}

// call queues an application callback; callbacks run one at a time in order.
func (c *Client) call(f func()) {
	c.callsMu.Lock()
	c.calls = append(c.calls, f)
	c.callsMu.Unlock()
	select {
	case c.wake <- struct{}{}:
	default:
	}
}

func (c *Client) runCalls() {
	for {
		c.callsMu.Lock()
		batch := c.calls
		c.calls = nil
		c.callsMu.Unlock()
		for _, f := range batch {
			c.safeCall(f)
		}
		select {
		case <-c.ctx.Done():
			// Run what was queued before closing, such as the stopped status.
			c.callsMu.Lock()
			rest := c.calls
			c.calls = nil
			c.callsMu.Unlock()
			for _, f := range rest {
				c.safeCall(f)
			}
			return
		case <-c.wake:
		}
	}
}

// stop closes the client because it may not continue, such as after
// revocation.
func (c *Client) stop(err error) {
	c.mu.Lock()
	c.stopErr = err
	c.mu.Unlock()
	c.log.Warn("jetcast: client stopped", "error", err)
	go func() { _ = c.Close() }()
}

// startConnecting opens a new connection in the background, retrying with
// backoff. With refresh set, the current connection stays in use until the
// new one is ready.
func (c *Client) startConnecting(refresh bool) {
	c.mu.Lock()
	if c.connecting || c.closed {
		// A refresh is already dialing; a lost connection still means the
		// client is disconnected until it completes.
		if !refresh && !c.closed && c.conn != nil {
			c.setStatusLocked(StatusReconnecting)
		}
		c.mu.Unlock()
		return
	}
	c.connecting = true
	if !refresh && c.conn != nil {
		c.setStatusLocked(StatusReconnecting)
	}
	c.mu.Unlock()
	c.wg.Go(func() {
		delay := 250 * time.Millisecond
		for {
			conn, err := c.dial()
			if err == nil {
				c.adopt(conn)
				return
			}
			if errors.Is(err, ErrUnauthorized) {
				c.mu.Lock()
				c.connecting = false
				c.mu.Unlock()
				c.stop(err)
				return
			}
			c.log.Debug("jetcast: connect failed", "error", err)
			jitter := time.Duration(mrand.Int64N(int64(delay)/2 + 1))
			select {
			case <-c.ctx.Done():
				return
			case <-time.After(delay + jitter):
			}
			delay = min(2*delay, 15*time.Second)
		}
	})
}

// dial opens and greets a new connection.
func (c *Client) dial() (*connection, error) {
	ctx, cancel := context.WithTimeout(c.ctx, 15*time.Second)
	defer cancel()
	token, err := c.opts.GetToken(ctx)
	if err != nil {
		return nil, err
	}
	socket := newSocketID()
	conn := &connection{socket: socket}
	base := []nats.Option{
		nats.Name(socket),
		nats.Token(token),
		nats.CustomInboxPrefix(c.sub.conn(socket) + ".r"),
		nats.NoReconnect(),
		nats.DisconnectErrHandler(func(*nats.Conn, error) { c.lost(conn) }),
		nats.ClosedHandler(func(*nats.Conn) { c.lost(conn) }),
	}
	nc, err := nats.Connect(strings.Join(c.opts.Servers, ","), append(base, c.opts.NATSOptions...)...)
	if err != nil {
		return nil, err
	}
	conn.nc = nc
	fail := func(err error) (*connection, error) {
		nc.Close()
		return nil, err
	}
	if conn.ctlSub, err = nc.Subscribe(c.sub.conn(socket)+".ctl", func(m *nats.Msg) { c.onControl(conn, m) }); err != nil {
		return fail(err)
	}
	if conn.evSub, err = nc.Subscribe(c.sub.conn(socket)+".ev", func(m *nats.Msg) { c.onRelayed(conn, m) }); err != nil {
		return fail(err)
	}
	var hello jetcast.HelloResponse
	if err := request(ctx, nc, c.sub.request(socket, "hello"), struct{}{}, &hello); err != nil {
		return fail(err)
	}
	if hello.Error != nil {
		return fail(fmt.Errorf("jetcast: hello: %s", hello.Error.Code))
	}
	conn.hello = hello
	exp := time.UnixMilli(hello.ExpiresAt)
	lead := max(time.Until(exp)/10, 30*time.Second)
	conn.refreshAt = exp.Add(-lead)
	return conn, nil
}

// adopt makes a new connection current and resubscribes every channel.
func (c *Client) adopt(conn *connection) {
	c.mu.Lock()
	c.connecting = false
	if c.closed {
		c.mu.Unlock()
		conn.nc.Close()
		return
	}
	old := c.conn
	c.conn = conn
	c.sockets[conn.socket] = true
	subs := make([]*Subscription, 0, len(c.subs))
	for _, s := range c.subs {
		subs = append(subs, s)
	}
	c.setStatusLocked(StatusConnected)
	c.mu.Unlock()
	select {
	case <-c.ready:
	default:
		close(c.ready)
	}
	for _, s := range subs {
		s.resubscribe(conn, "")
	}
	if old != nil {
		old.nc.Close()
	}
}

// lost handles the loss of a connection.
func (c *Client) lost(conn *connection) {
	c.mu.Lock()
	current := c.conn == conn
	closed := c.closed
	c.mu.Unlock()
	if current && !closed {
		c.startConnecting(false)
	}
}

func (c *Client) current() *connection {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.conn
}

func (c *Client) onControl(conn *connection, m *nats.Msg) {
	var ctl jetcast.Control
	if json.Unmarshal(m.Data, &ctl) != nil || c.current() != conn {
		return
	}
	switch ctl.Type {
	case jetcast.CtlRefresh:
		time.AfterFunc(time.Duration(mrand.Int64N(int64(5*time.Second))), func() { c.startConnecting(true) })
	case jetcast.CtlDisconnect:
		c.stop(errors.New("jetcast: disconnected by server"))
	case jetcast.CtlLeave:
		if s := c.subscription(ctl.Channel); s != nil {
			s.removed(conn, "", jetcast.CtlLeave)
		}
	case jetcast.CtlDenied:
		if s := c.subscription(ctl.Channel); s != nil {
			s.removed(conn, ctl.Sid, jetcast.CtlDenied)
		}
	case jetcast.CtlInterrupted:
		if s := c.subscription(ctl.Channel); s != nil {
			s.resubscribe(conn, ctl.Sid)
		}
	}
}

func (c *Client) onRelayed(conn *connection, m *nats.Msg) {
	if s := c.subscription(m.Header.Get(jetcast.HeaderChannel)); s != nil {
		s.onMessage(conn, m, true)
	}
}

func (c *Client) subscription(key string) *Subscription {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.subs[key]
}

// Channel subscribes to a public channel.
func (c *Client) Channel(name string) *Subscription {
	return c.subscribe(jetcast.Public(name))
}

// Private subscribes to a private channel.
func (c *Client) Private(name string) *Subscription {
	return c.subscribe(jetcast.Private(name))
}

func (c *Client) subscribe(ch jetcast.Channel) *Subscription {
	key := ch.String()
	c.mu.Lock()
	if s := c.subs[key]; s != nil {
		c.mu.Unlock()
		return s
	}
	s := newSubscription(c, ch)
	if err := ch.Validate(); err != nil {
		c.mu.Unlock()
		s.fail(err)
		return s
	}
	if c.closed {
		c.mu.Unlock()
		s.fail(ErrClosed)
		return s
	}
	c.subs[key] = s
	conn := c.conn
	c.mu.Unlock()
	if conn != nil {
		s.resubscribe(conn, "")
	}
	return s
}

func (c *Client) forget(s *Subscription) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.subs[s.ch.String()] == s {
		delete(c.subs, s.ch.String())
	}
}

func (c *Client) ownSocket(socket string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.sockets[socket]
}

// checkHeadsSoon schedules a heads check of every channel, at most every
// five seconds.
func (c *Client) checkHeadsSoon() {
	now := time.Now().UnixNano()
	last := c.headsAsked.Load()
	if now-last >= int64(5*time.Second) && c.headsAsked.CompareAndSwap(last, now) {
		c.headsNow.Store(true)
	}
}

// runTimers drives credential refresh, relay renewal and heads checks.
func (c *Client) runTimers() {
	t := time.NewTicker(time.Second)
	defer t.Stop()
	var lastRenew, lastHeads time.Time
	for {
		select {
		case <-c.ctx.Done():
			return
		case now := <-t.C:
			conn := c.current()
			if conn == nil || conn.nc.IsClosed() {
				continue
			}
			if now.After(conn.refreshAt) {
				c.startConnecting(true)
			}
			renew := time.Duration(conn.hello.RenewMs) * time.Millisecond
			if renew <= 0 {
				renew = 20 * time.Second
			}
			if now.Sub(lastRenew) >= renew-renew/10+time.Duration(mrand.Int64N(int64(renew/10)+1)) {
				lastRenew = now
				go c.renew(conn)
			}
			if c.headsNow.Swap(false) {
				lastHeads = now
				go c.heads(conn, now.Add(time.Hour))
			} else if now.Sub(lastHeads) >= c.opts.HeadsInterval {
				lastHeads = now
				go c.heads(conn, now.Add(-c.opts.HeadsInterval))
			}
		}
	}
}

func (c *Client) allSubs() []*Subscription {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]*Subscription, 0, len(c.subs))
	for _, s := range c.subs {
		out = append(out, s)
	}
	return out
}

// renew renews relay leases, grouped by node.
func (c *Client) renew(conn *connection) {
	byNode := map[string][]*Subscription{}
	for _, s := range c.allSubs() {
		if node, sid := s.relayLease(conn); sid != "" {
			byNode[node] = append(byNode[node], s)
		}
	}
	for node, subs := range byNode {
		sids := make([]string, len(subs))
		for i, s := range subs {
			_, sids[i] = s.relayLease(conn)
		}
		ctx, cancel := context.WithTimeout(c.ctx, 10*time.Second)
		var resp jetcast.RenewResponse
		err := request(ctx, conn.nc, c.sub.nodeRequest(conn.socket, node, "renew"), jetcast.RenewRequest{Sids: sids}, &resp)
		cancel()
		if err != nil || resp.Error != nil {
			// The node is gone or unreachable: rebuild its relays elsewhere.
			for i, s := range subs {
				s.resubscribe(conn, sids[i])
			}
			continue
		}
		missing := map[string]bool{}
		for _, sid := range resp.Missing {
			missing[sid] = true
		}
		for i, s := range subs {
			if missing[sids[i]] {
				s.resubscribe(conn, sids[i])
			}
		}
	}
}

// heads checks channels without recent events for missed events.
func (c *Client) heads(conn *connection, idleSince time.Time) {
	groups := map[string][]*Subscription{} // node, "" for any node
	for _, s := range c.allSubs() {
		if node, ok := s.headsTarget(conn, idleSince); ok {
			groups[node] = append(groups[node], s)
		}
	}
	const chunk = 100 // below the server's default HeadsChannels limit
	var batches [][]*Subscription
	var nodes []string
	for node, subs := range groups {
		for len(subs) > chunk {
			batches, nodes = append(batches, subs[:chunk]), append(nodes, node)
			subs = subs[chunk:]
		}
		batches, nodes = append(batches, subs), append(nodes, node)
	}
	for i, subs := range batches {
		node := nodes[i]
		epoch := subs[0].cursorEpoch()
		names := make([]string, 0, len(subs))
		for _, s := range subs {
			names = append(names, s.ch.String())
		}
		subject := c.sub.request(conn.socket, "heads")
		if node != "" {
			subject = c.sub.nodeRequest(conn.socket, node, "heads")
		}
		ctx, cancel := context.WithTimeout(c.ctx, 10*time.Second)
		var resp jetcast.HeadsResponse
		err := request(ctx, conn.nc, subject, jetcast.HeadsRequest{Epoch: epoch, Channels: names}, &resp)
		cancel()
		if err != nil || resp.Error != nil {
			continue
		}
		denied := map[string]bool{}
		for _, d := range resp.Denied {
			denied[d] = true
		}
		for _, s := range subs {
			key := s.ch.String()
			head, ok := resp.Heads[key]
			switch {
			case denied[key]:
				s.resubscribe(conn, "")
			case resp.Epoch != epoch || !ok:
				s.checkHead(conn, resp, 0, false)
			default:
				s.checkHead(conn, resp, head, true)
			}
		}
	}
}

// request sends a JSON request and decodes the JSON response.
func request(ctx context.Context, nc *nats.Conn, subject string, req, resp any) error {
	b, err := json.Marshal(req)
	if err != nil {
		return err
	}
	m, err := nc.RequestWithContext(ctx, subject, b)
	if err != nil {
		return err
	}
	return json.Unmarshal(m.Data, resp)
}

func parseUint(s string) uint64 {
	v, _ := strconv.ParseUint(s, 10, 64)
	return v
}
