package jetcast

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/nats-io/nkeys"
	"github.com/nats-io/nuid"
)

// AuthRequest describes a connection being authenticated.
type AuthRequest struct {
	// Token is the credential the client sent, typically from getToken().
	Token string
	// Type is the connection type: "websocket" for browsers, "nats" for
	// standard clients.
	Type string
	// Host is the client address as seen by the NATS server.
	Host string
}

// Connection types for [User.ConnectionTypes].
const (
	ConnectionWebSocket = "WEBSOCKET"
	ConnectionStandard  = "STANDARD"
)

// User is an authenticated principal.
type User struct {
	// ID identifies the user: 1 to 64 ASCII letters, digits, "-" or "_".
	// Prefix it to separate kinds of principals, such as "u_42" and "v_9f3a".
	ID string
	// Session identifies the login session, with the same syntax as ID.
	// Disconnect can revoke one session. Defaults to ID.
	Session string
	// ExpiresAt bounds the connection lifetime; zero means the server's
	// MaxConnectionTTL.
	ExpiresAt time.Time
	// Info is public data about the user, returned to the client by hello.
	// GrantsFunc receives it as returned by Authenticate; AuthorizeFunc
	// receives its JSON encoding as a json.RawMessage.
	Info any
	// ConnectionTypes lists the allowed connection types; WebSocket only by
	// default.
	ConnectionTypes []string
}

// Params holds the values of the named tokens of a channel pattern.
type Params map[string]string

// AuthenticateFunc authenticates a connection. It returns an error to reject
// it. It must not have side effects that break when called again for the same
// token.
type AuthenticateFunc func(ctx context.Context, req AuthRequest) (User, error)

// GrantsFunc returns the private channel patterns a user may subscribe to
// directly for the lifetime of a connection. Every channel matching a pattern
// must be visible to the user. Revoking a grant requires Disconnect.
type GrantsFunc func(ctx context.Context, u User) ([]string, error)

// AuthorizeFunc decides whether a user may subscribe to a private channel
// matching the pattern it was registered with.
type AuthorizeFunc func(ctx context.Context, u User, p Params) (bool, error)

// Limits bounds what one connection can use on one node.
type Limits struct {
	// RelaysPerConnection caps relayed channels per connection, 100 by default.
	RelaysPerConnection int
	// ConcurrentRequests caps in-flight requests per connection, 8 by
	// default. Clients learn it in hello and wait instead of exceeding it.
	ConcurrentRequests int
	// RelaysPerNode caps relayed subscriptions on the node, 100000 by default.
	RelaysPerNode int
	// RecoverCount and RecoverBytes bound one recovery batch, 100 events and
	// 512 KiB by default; a batch always contains at least one event.
	RecoverCount int
	RecoverBytes int
	// HeadsChannels caps the channels of one heads request, 200 by default.
	HeadsChannels int
	// ConcurrentCallouts caps auth callout requests handled at once on the
	// node, 32 by default. Further requests wait in a queue four times as
	// long; requests that do not fit or wait too long are dropped, and the
	// client retries.
	ConcurrentCallouts int
}

func (l Limits) withDefaults() Limits {
	def := func(v *int, d int) {
		if *v <= 0 {
			*v = d
		}
	}
	def(&l.RelaysPerConnection, 100)
	def(&l.ConcurrentRequests, 8)
	def(&l.RelaysPerNode, 100_000)
	def(&l.RecoverCount, 100)
	def(&l.RecoverBytes, 512<<10)
	def(&l.HeadsChannels, 200)
	def(&l.ConcurrentCallouts, 32)
	return l
}

// ServerOptions configure a [Server].
type ServerOptions struct {
	Config Config
	// Account is the NATS account the auth callout places clients in: in
	// server configuration mode, its name. The application's own connection
	// belongs to the account of the auth_callout block, which holds the event
	// stream and the registry; a separate client account (see
	// embedded.Accounts) exchanges jetcast's subjects with it and limits
	// clients. Clients may also share the application's account.
	Account string
	// CalloutSigner signs the user JWTs and callout responses. It is the
	// account key pair whose public key is the auth_callout issuer.
	CalloutSigner nkeys.KeyPair
	// CalloutXKey decrypts callout requests and encrypts responses when the
	// auth_callout block configures an xkey. Optional.
	CalloutXKey nkeys.KeyPair
	// Admin kicks connections on Disconnect. Optional: without it Disconnect
	// relies on clients to close and on JWT expiry.
	Admin ConnectionAdmin
	// History configures event retention.
	History History
	// ManageStreams creates or updates the event stream and the registry
	// bucket; otherwise Start only validates them.
	ManageStreams bool
	// MaxConnectionTTL caps connection lifetime, 1 hour by default. Clients
	// switch to a fresh connection before it ends.
	MaxConnectionTTL time.Duration
	// RenewInterval is the relay lease renewal period, 20 seconds by default.
	// Relays not renewed for three periods are removed.
	RenewInterval time.Duration
	// ReauthorizeInterval is how often relayed subscriptions are authorized
	// again on renewal, 1 minute by default.
	ReauthorizeInterval time.Duration
	Limits              Limits
	Logger              *slog.Logger
}

// Stats are cumulative counters of a server node.
type Stats struct {
	CalloutAccepted uint64
	CalloutRejected uint64
	// CalloutDropped counts callout requests dropped because the queue was
	// full or they waited too long.
	CalloutDropped    uint64
	CalloutLatencyAvg time.Duration
	Relays            int
	RelayedEvents     uint64
	Recoveries        uint64
	RecoveryFailures  uint64
	// Overloaded counts requests answered overloaded because the
	// connection or the node had too many requests in flight.
	Overloaded uint64
}

// Server serves clients on one application node: it answers the NATS auth
// callout, handles client requests and relays private channels. Run one per
// node; all nodes share the same Config.
type Server struct {
	*Publisher

	nc     *nats.Conn
	js     jetstream.JetStream
	opts   ServerOptions
	sub    subjects
	node   string
	log    *slog.Logger
	reg    *registry
	hist   *history
	maxAge time.Duration // retention of the event stream as configured
	relays *relays

	// regMu orders callback registration with Start.
	regMu        sync.Mutex
	authenticate AuthenticateFunc
	grants       GrantsFunc
	channels     []channelRoute

	ctx      context.Context
	cancel   context.CancelFunc
	subs     []*nats.Subscription
	work     chan *nats.Msg
	callouts chan calloutRequest
	wg       sync.WaitGroup
	started  atomic.Bool
	closed   atomic.Bool

	inflightMu sync.Mutex
	inflight   map[string]int

	calloutAccepted, calloutRejected atomic.Uint64
	calloutDropped, calloutNanos     atomic.Uint64
	relayed, recoveries, recFailures atomic.Uint64
	overloaded                       atomic.Uint64
}

type channelRoute struct {
	tokens    []string
	authorize AuthorizeFunc
}

// NewServer creates a server on nc. Register callbacks, then call Start.
func NewServer(nc *nats.Conn, opts ServerOptions) (*Server, error) {
	opts.Config = opts.Config.withDefaults()
	pub, err := NewPublisher(nc, opts.Config)
	if err != nil {
		return nil, err
	}
	if opts.Account == "" || opts.CalloutSigner == nil {
		return nil, fmt.Errorf("%w: Account and CalloutSigner are required", ErrInvalidConfig)
	}
	if pk, err := opts.CalloutSigner.PublicKey(); err != nil || !nkeys.IsValidPublicAccountKey(pk) {
		return nil, fmt.Errorf("%w: CalloutSigner must be an account key pair", ErrInvalidConfig)
	}
	opts.History = opts.History.withDefaults()
	opts.Limits = opts.Limits.withDefaults()
	if opts.MaxConnectionTTL <= 0 {
		opts.MaxConnectionTTL = time.Hour
	}
	if opts.RenewInterval <= 0 {
		opts.RenewInterval = 20 * time.Second
	}
	if opts.ReauthorizeInterval <= 0 {
		opts.ReauthorizeInterval = time.Minute
	}
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}
	node := nuid.Next()
	s := &Server{
		Publisher: pub,
		nc:        nc,
		js:        pub.js,
		opts:      opts,
		sub:       subjects{opts.Config.Prefix},
		node:      node,
		log:       opts.Logger.With("jetcast_node", node),
		inflight:  map[string]int{},
	}
	s.relays = newRelays(s)
	return s, nil
}

// Node returns the ID of this server node, random per process.
func (s *Server) Node() string { return s.node }

// errStarted reports a registration after Start.
var errStarted = fmt.Errorf("%w: callbacks must be registered before Start", ErrInvalidConfig)

// Authenticate registers the connection authenticator. It is required and
// must be called before Start; it panics afterwards.
func (s *Server) Authenticate(f AuthenticateFunc) {
	s.regMu.Lock()
	defer s.regMu.Unlock()
	if s.started.Load() {
		panic(errStarted)
	}
	s.authenticate = f
}

// Grants registers the function returning directly subscribable private
// channel patterns. Optional; it must be called before Start and panics
// afterwards.
func (s *Server) Grants(f GrantsFunc) {
	s.regMu.Lock()
	defer s.regMu.Unlock()
	if s.started.Load() {
		panic(errStarted)
	}
	s.grants = f
}

// Channel registers the authorizer of private channels matching pattern.
// Pattern tokens are literals or "{name}" placeholders matching one token,
// like "orders.{id}". The first matching pattern decides. Channels must be
// registered before Start; Channel returns an error afterwards.
func (s *Server) Channel(pattern string, f AuthorizeFunc) error {
	s.regMu.Lock()
	defer s.regMu.Unlock()
	if s.started.Load() {
		return errStarted
	}
	tokens := strings.Split(pattern, ".")
	check := make([]string, len(tokens))
	for i, t := range tokens {
		if strings.HasPrefix(t, "{") && strings.HasSuffix(t, "}") && len(t) > 2 {
			check[i] = "x"
		} else {
			check[i] = t
		}
	}
	if err := ValidateChannelName(strings.Join(check, ".")); err != nil {
		return fmt.Errorf("jetcast: channel pattern %q: %w", pattern, err)
	}
	s.channels = append(s.channels, channelRoute{tokens: tokens, authorize: f})
	return nil
}

// route finds the authorizer of a private channel.
func (s *Server) route(name string) (AuthorizeFunc, Params) {
	n := strings.Split(name, ".")
	for _, r := range s.channels {
		if len(r.tokens) != len(n) {
			continue
		}
		p := Params{}
		ok := true
		for i, t := range r.tokens {
			if strings.HasPrefix(t, "{") && strings.HasSuffix(t, "}") && len(t) > 2 {
				p[t[1:len(t)-1]] = n[i]
			} else if t != n[i] {
				ok = false
				break
			}
		}
		if ok {
			return r.authorize, p
		}
	}
	return nil, nil
}

// Start ensures the stream and registry, then serves the auth callout and
// client requests until Close.
func (s *Server) Start(ctx context.Context) error {
	s.regMu.Lock()
	if s.authenticate == nil {
		s.regMu.Unlock()
		return fmt.Errorf("%w: Authenticate is required", ErrInvalidConfig)
	}
	if !s.started.CompareAndSwap(false, true) {
		s.regMu.Unlock()
		return errors.New("jetcast: server already started")
	}
	s.regMu.Unlock()
	stream, kv, err := s.ensureStorage(ctx)
	if err != nil {
		return err
	}
	s.hist = &history{js: s.js, name: s.opts.Config.Stream, stream: stream, sub: s.sub}
	s.maxAge = stream.CachedInfo().Config.MaxAge
	s.reg = newRegistry(kv)
	s.ctx, s.cancel = context.WithCancel(context.Background())
	s.work = make(chan *nats.Msg, 1024)
	for range 64 {
		s.wg.Go(s.worker)
	}
	// Requests queued beyond a few rounds of the workers would expire.
	s.callouts = make(chan calloutRequest, 4*s.opts.Limits.ConcurrentCallouts)
	for range s.opts.Limits.ConcurrentCallouts {
		s.wg.Go(s.calloutWorker)
	}
	subscribe := func(subject, queue string, h nats.MsgHandler) error {
		var sub *nats.Subscription
		var err error
		if queue == "" {
			sub, err = s.nc.Subscribe(subject, h)
		} else {
			sub, err = s.nc.QueueSubscribe(subject, queue, h)
		}
		if err == nil {
			s.subs = append(s.subs, sub)
		}
		return err
	}
	enqueue := func(m *nats.Msg) {
		select {
		case <-s.ctx.Done():
		case s.work <- m:
		default:
			if socket, ok := s.validRequest(m); ok && socket != "" {
				s.overloaded.Add(1)
				s.respondError(m, CodeOverloaded, "server busy")
			}
		}
	}
	queue := s.opts.Config.Prefix
	if err := subscribe("$SYS.REQ.USER.AUTH", queue, s.enqueueCallout); err != nil {
		return err
	}
	if err := subscribe(s.sub.requests(), queue, enqueue); err != nil {
		return err
	}
	if err := subscribe(s.sub.nodeRequests(s.node), "", enqueue); err != nil {
		return err
	}
	if err := subscribe(s.sub.sysAll(), "", s.handleSys); err != nil {
		return err
	}
	if err := s.flush(ctx); err != nil {
		return err
	}
	s.wg.Go(s.relays.janitor)
	s.wg.Go(s.watchConnection)
	s.log.Info("jetcast: server started", "prefix", s.opts.Config.Prefix, "stream", s.opts.Config.Stream)
	return nil
}

// Close stops serving and releases resources after a successful or failed Start.
// Relayed clients are told to resubscribe elsewhere.
func (s *Server) Close() error {
	if !s.started.Load() || !s.closed.CompareAndSwap(false, true) {
		return nil
	}
	for _, sub := range s.subs {
		_ = sub.Unsubscribe()
	}
	s.relays.closeAll()
	if s.cancel != nil {
		s.cancel()
	}
	s.wg.Wait()
	return s.nc.Flush()
}

// Stats returns the node's counters.
func (s *Server) Stats() Stats {
	st := Stats{
		CalloutAccepted:  s.calloutAccepted.Load(),
		CalloutRejected:  s.calloutRejected.Load(),
		CalloutDropped:   s.calloutDropped.Load(),
		Relays:           s.relays.count(),
		RelayedEvents:    s.relayed.Load(),
		Recoveries:       s.recoveries.Load(),
		RecoveryFailures: s.recFailures.Load(),
		Overloaded:       s.overloaded.Load(),
	}
	if n := st.CalloutAccepted + st.CalloutRejected; n > 0 {
		st.CalloutLatencyAvg = time.Duration(s.calloutNanos.Load() / n)
	}
	return st
}

// registrySkew is the minimum margin of the registry TTL over
// MaxConnectionTTL, covering clock differences between NATS servers and
// application nodes, so records outlive their connections.
const registrySkew = time.Minute

// ensureStorage creates, updates or validates the event stream and the
// registry bucket.
func (s *Server) ensureStorage(ctx context.Context) (jetstream.Stream, jetstream.KeyValue, error) {
	cfg := s.opts.Config
	want := streamConfig(cfg, s.opts.History)
	var stream jetstream.Stream
	var err error
	if s.opts.ManageStreams {
		stream, err = s.js.CreateOrUpdateStream(ctx, want)
	} else {
		stream, err = s.js.Stream(ctx, cfg.Stream)
	}
	if err != nil {
		return nil, nil, fmt.Errorf("jetcast: event stream %s: %w", cfg.Stream, err)
	}
	if err := checkStreamConfig(stream.CachedInfo().Config, want); err != nil {
		return nil, nil, fmt.Errorf("jetcast: event stream %s: %w", cfg.Stream, err)
	}

	bucket := cfg.bucket()
	kvStream := "KV_" + bucket
	if s.opts.ManageStreams {
		if _, err := s.js.CreateOrUpdateKeyValue(ctx, jetstream.KeyValueConfig{
			Bucket:      bucket,
			Description: "jetcast connection registry",
			History:     1,
			TTL:         s.opts.MaxConnectionTTL + 10*time.Minute,
			Storage:     s.opts.History.Storage,
			Replicas:    s.opts.History.Replicas,
		}); err != nil {
			return nil, nil, fmt.Errorf("jetcast: registry bucket %s: %w", bucket, err)
		}
		st, err := s.js.Stream(ctx, kvStream)
		if err != nil {
			return nil, nil, err
		}
		if c := st.CachedInfo().Config; c.AllowDirect {
			c.AllowDirect = false
			if _, err := s.js.UpdateStream(ctx, c); err != nil {
				return nil, nil, fmt.Errorf("jetcast: registry bucket %s: disable direct get: %w", bucket, err)
			}
		}
	}
	st, err := s.js.Stream(ctx, kvStream)
	if err != nil {
		return nil, nil, fmt.Errorf("jetcast: registry bucket %s: %w", bucket, err)
	}
	if c := st.CachedInfo().Config; c.AllowDirect || c.MaxAge != 0 && c.MaxAge < s.opts.MaxConnectionTTL+registrySkew {
		return nil, nil, fmt.Errorf("jetcast: registry bucket %s needs direct get disabled and a TTL of at least MaxConnectionTTL plus %v", bucket, registrySkew)
	}
	kv, err := s.js.KeyValue(ctx, bucket)
	if err != nil {
		return nil, nil, err
	}
	return stream, kv, nil
}

// watchConnection tells relayed clients to recover after the server's own
// NATS connection reconnects, since events may have been missed meanwhile.
func (s *Server) watchConnection() {
	ch := s.nc.StatusChanged(nats.RECONNECTING, nats.CONNECTED)
	defer s.nc.RemoveStatusListener(ch)
	disconnected := false
	for {
		select {
		case <-s.ctx.Done():
			return
		case st := <-ch:
			switch st {
			case nats.RECONNECTING:
				disconnected = true
			case nats.CONNECTED:
				if disconnected {
					disconnected = false
					s.relays.interruptAll()
				}
			}
		}
	}
}

// control sends a control message to a connection.
func (s *Server) control(socket string, c Control) {
	b, _ := json.Marshal(c)
	if err := s.nc.Publish(s.sub.connControl(socket), b); err != nil {
		s.log.Warn("jetcast: send control message", "error", err)
	}
}

// Target selects the connections of a user or a session.
type Target struct {
	User    string
	Session string
}

// ByUser targets every connection of a user.
func ByUser(user string) Target { return Target{User: user} }

// BySession targets the connections of one session of a user.
func BySession(user, session string) Target { return Target{User: user, Session: session} }

// sysMsg is broadcast between nodes.
type sysMsg struct {
	Sockets []string `json:"sockets,omitempty"`
	Channel string   `json:"channel,omitempty"`
}

func (s *Server) sys(op string, m sysMsg) error {
	b, _ := json.Marshal(m)
	return s.nc.Publish(s.sub.sys(op), b)
}

func (s *Server) handleSys(m *nats.Msg) {
	var msg sysMsg
	if err := json.Unmarshal(m.Data, &msg); err != nil {
		return
	}
	switch strings.TrimPrefix(m.Subject, s.sub.sys("")) {
	case "revoke":
		s.reg.invalidate(msg.Sockets...)
		s.relays.removeSockets(msg.Sockets, CtlDisconnect)
	case "leave":
		c, err := ParseChannel(msg.Channel)
		if err == nil {
			s.relays.removeChannel(c, msg.Sockets)
		}
	}
}

func (s *Server) targetSockets(ctx context.Context, t Target) ([]string, error) {
	if err := ValidateID(t.User); err != nil {
		return nil, err
	}
	if t.Session != "" {
		if err := ValidateID(t.Session); err != nil {
			return nil, err
		}
	}
	return s.reg.sockets(ctx, t.User, t.Session)
}

// Refresh asks the target's connections to switch to a fresh connection
// after a random delay, so they pick up newly granted channels. It cannot
// revoke grants; use Disconnect.
func (s *Server) Refresh(ctx context.Context, t Target) error {
	sockets, err := s.targetSockets(ctx, t)
	if err != nil {
		return err
	}
	for _, socket := range sockets {
		s.control(socket, Control{Type: CtlRefresh})
	}
	return s.flush(ctx)
}

// Leave removes the target's current subscriptions to a channel: relays stop
// immediately and directly subscribed clients are told to unsubscribe. It
// does not revoke authorization; clients may subscribe again while the
// authorizer allows it.
func (s *Server) Leave(ctx context.Context, c Channel, t Target) error {
	if err := c.Validate(); err != nil {
		return err
	}
	sockets, err := s.targetSockets(ctx, t)
	if err != nil {
		return err
	}
	if len(sockets) == 0 {
		return nil
	}
	if err := s.sys("leave", sysMsg{Sockets: sockets, Channel: c.String()}); err != nil {
		return err
	}
	for _, socket := range sockets {
		s.control(socket, Control{Type: CtlLeave, Channel: c.String()})
	}
	return s.flush(ctx)
}

// DisconnectResult reports what Disconnect did.
type DisconnectResult struct {
	// Connections is the number of registered connections of the target.
	Connections int
	// Revoked is the number of connection records marked revoked.
	Revoked int
	// Kicked is the number of connections kicked through the admin.
	Kicked int
	// Failed counts connections that could not be revoked or kicked.
	Failed int
	// Enforced is false when no ConnectionAdmin is configured: clients were
	// asked to close but not forced.
	Enforced bool
}

// Disconnect revokes the target's connections: it marks the target revoked so
// that authentications in progress fail, marks every connection record
// revoked so their requests are denied, stops their relays, asks clients to
// close and kicks them through the ConnectionAdmin. Repeated calls retry
// enforcement for existing revoked records. Callers must retry errors to
// complete enforcement; a failure to write the revocation mark is reported
// after the existing connections were revoked. Invalidate the session in the
// application first, or clients reconnect with the same credentials.
func (s *Server) Disconnect(ctx context.Context, t Target) (DisconnectResult, error) {
	res := DisconnectResult{Enforced: s.opts.Admin != nil}
	if err := ValidateID(t.User); err != nil {
		return res, err
	}
	var errs []error
	// Without the mark, authentications in progress may still succeed, so the
	// error is returned for a retry; existing connections are revoked anyway.
	if err := s.reg.markRevoked(ctx, t.User, t.Session); err != nil {
		errs = append(errs, err)
	}
	sockets, err := s.targetSockets(ctx, t)
	if err != nil {
		return res, errors.Join(append(errs, err)...)
	}
	res.Connections = len(sockets)
	var revoked []*connRecord
	var revokedSockets []string
	for _, socket := range sockets {
		rec, changed, err := s.reg.revoke(ctx, socket)
		if err != nil {
			res.Failed++
			errs = append(errs, err)
			continue
		}
		if rec == nil {
			continue
		}
		if changed {
			res.Revoked++
		}
		revoked = append(revoked, rec)
		revokedSockets = append(revokedSockets, socket)
		s.control(socket, Control{Type: CtlDisconnect})
	}
	if len(revoked) == 0 {
		return res, errors.Join(errs...)
	}
	if err := s.sys("revoke", sysMsg{Sockets: revokedSockets}); err != nil {
		errs = append(errs, err)
	}
	// Let cooperative clients see the disconnect request before kicking.
	if err := s.flush(ctx); err != nil {
		errs = append(errs, err)
	}
	if s.opts.Admin != nil {
		now := time.Now().UnixMilli()
		for _, rec := range revoked {
			if now >= rec.ExpiresAt {
				continue
			}
			if err := s.opts.Admin.Kick(ctx, rec.Server, rec.CID); err != nil {
				res.Failed++
				errs = append(errs, err)
			} else {
				res.Kicked++
			}
		}
	}
	return res, errors.Join(errs...)
}

// SocketIDHeader is the HTTP header clients use to send their socket ID with
// application requests.
const SocketIDHeader = "X-Socket-ID"

// SocketID returns the client's socket ID from an HTTP request, or "" when it
// is missing or malformed. Pass it as Event.Origin to exclude the sender.
func SocketID(r *http.Request) string {
	id := r.Header.Get(SocketIDHeader)
	if ValidateSocketID(id) != nil {
		return ""
	}
	return id
}

// flush waits for the server to process earlier messages, bounded by ctx or
// ten seconds when ctx has no deadline.
func (s *Server) flush(ctx context.Context) error {
	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, 10*time.Second)
		defer cancel()
	}
	return s.nc.FlushWithContext(ctx)
}
