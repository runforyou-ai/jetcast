package jetcast

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/nats-io/nuid"
)

// Event is a broadcast event.
type Event struct {
	// ID identifies the event; a random ID is used when empty. Retrying a
	// broadcast with the same ID within the stream's duplicate window does not
	// deliver it twice.
	ID string
	// Name is the event name clients listen for, such as "order.shipped".
	Name string
	// Channels lists the channels the event is broadcast on.
	Channels []Channel
	// Data is the payload: []byte and json.RawMessage are sent as is, other
	// values are encoded as JSON.
	Data any
	// Origin is the socket ID of the client that caused the event. Clients
	// with that socket receive the event without invoking their listeners,
	// like Laravel's toOthers. Events carry a digest of it, so subscribers do
	// not learn other connections' socket IDs. The socket ID comes from the
	// client, typically the X-Socket-ID header, and is not verified: toOthers
	// only spares the sender a redundant update and is not a security
	// mechanism.
	Origin string
}

// Broadcastable is implemented by event types, like Laravel's ShouldBroadcast.
// Such types may also implement BroadcastAs() string to name the event (the
// type name by default), BroadcastWith() any to choose the payload (the value
// itself by default) and BroadcastWhen() bool to skip broadcasting.
type Broadcastable interface {
	BroadcastOn() []Channel
}

// BroadcastOption configures [Publisher.Dispatch].
type BroadcastOption func(*Event)

// ToOthers excludes the client with the given socket ID from listeners.
func ToOthers(socketID string) BroadcastOption {
	return func(e *Event) { e.Origin = socketID }
}

// WithEventID sets the event ID, for idempotent retries.
func WithEventID(id string) BroadcastOption {
	return func(e *Event) { e.ID = id }
}

// ChannelResult is the outcome of broadcasting on one channel.
type ChannelResult struct {
	Channel Channel
	// Sequence is the stream sequence of the retained event, 0 for
	// ephemeral channels.
	Sequence uint64
	Err      error
}

// BroadcastResult is the outcome of a broadcast. Broadcasting on several
// channels is not atomic: some channels may fail while others succeed.
type BroadcastResult struct {
	ID       string
	Channels []ChannelResult
}

// Err joins the errors of failed channels.
func (r BroadcastResult) Err() error {
	var errs []error
	for _, c := range r.Channels {
		if c.Err != nil {
			errs = append(errs, fmt.Errorf("%s: %w", c.Channel, c.Err))
		}
	}
	return errors.Join(errs...)
}

// Publisher broadcasts events. It is safe for concurrent use.
type Publisher struct {
	nc  *nats.Conn
	js  jetstream.JetStream
	cfg Config
	sub subjects
}

// NewPublisher returns a publisher on nc. The connection must be allowed to
// publish to the ingress and event subjects of the configured prefix.
func NewPublisher(nc *nats.Conn, cfg Config) (*Publisher, error) {
	cfg = cfg.withDefaults()
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	js, err := jetstream.New(nc)
	if err != nil {
		return nil, err
	}
	return &Publisher{nc: nc, js: js, cfg: cfg, sub: subjects{cfg.Prefix}}, nil
}

// Dispatch broadcasts an event type, like Laravel's broadcast(new Event).
func (p *Publisher) Dispatch(ctx context.Context, v Broadcastable, opts ...BroadcastOption) (BroadcastResult, error) {
	if w, ok := v.(interface{ BroadcastWhen() bool }); ok && !w.BroadcastWhen() {
		return BroadcastResult{}, nil
	}
	ev := Event{Channels: v.BroadcastOn(), Data: any(v)}
	if a, ok := v.(interface{ BroadcastAs() string }); ok {
		ev.Name = a.BroadcastAs()
	} else {
		t := reflect.TypeOf(v)
		for t.Kind() == reflect.Pointer {
			t = t.Elem()
		}
		ev.Name = t.Name()
	}
	if w, ok := v.(interface{ BroadcastWith() any }); ok {
		ev.Data = w.BroadcastWith()
	}
	for _, opt := range opts {
		opt(&ev)
	}
	return p.Broadcast(ctx, ev)
}

// Broadcast publishes ev on each of its channels. Retained channels are
// written to the stream and Broadcast waits for JetStream to acknowledge them;
// ephemeral channels are published directly. The returned error joins the
// errors of failed channels.
func (p *Publisher) Broadcast(ctx context.Context, ev Event) (BroadcastResult, error) {
	if ev.Name == "" {
		return BroadcastResult{}, errors.New("jetcast: event without name")
	}
	if ev.ID == "" {
		ev.ID = nuid.Next()
	}
	if len(ev.ID) > 128 {
		return BroadcastResult{}, errors.New("jetcast: event ID longer than 128 bytes")
	}
	if ev.Origin != "" {
		if err := ValidateSocketID(ev.Origin); err != nil {
			return BroadcastResult{}, err
		}
	}
	for _, c := range ev.Channels {
		if err := c.Validate(); err != nil {
			return BroadcastResult{}, err
		}
	}
	data, err := encodeData(ev.Data)
	if err != nil {
		return BroadcastResult{}, err
	}
	if max := p.nc.MaxPayload(); max > 0 && int64(len(data)+len(ev.Name)+len(ev.ID)+512) > max {
		return BroadcastResult{}, fmt.Errorf("jetcast: event of %d bytes exceeds the server payload limit of %d", len(data), max)
	}
	res := BroadcastResult{ID: ev.ID, Channels: make([]ChannelResult, len(ev.Channels))}
	for i, c := range ev.Channels {
		res.Channels[i] = ChannelResult{Channel: c}
		msg := nats.NewMsg("")
		msg.Data = data
		msg.Header.Set(HeaderEvent, ev.Name)
		msg.Header.Set(HeaderID, ev.ID)
		if ev.Origin != "" {
			msg.Header.Set(HeaderOrigin, originTag(ev.Origin))
		}
		if p.cfg.ephemeral(c.Name) {
			msg.Subject = p.sub.ev(c)
			res.Channels[i].Err = p.nc.PublishMsg(msg)
			continue
		}
		msg.Subject = p.sub.in(c)
		ack, err := p.js.PublishMsg(ctx, msg, jetstream.WithMsgID(ev.ID+":"+c.String()))
		if err != nil {
			res.Channels[i].Err = err
			continue
		}
		res.Channels[i].Sequence = ack.Sequence
	}
	return res, res.Err()
}

func encodeData(v any) ([]byte, error) {
	switch d := v.(type) {
	case nil:
		return nil, nil
	case []byte:
		return d, nil
	case json.RawMessage:
		return d, nil
	default:
		b, err := json.Marshal(v)
		if err != nil {
			return nil, fmt.Errorf("jetcast: encode event data: %w", err)
		}
		return b, nil
	}
}
