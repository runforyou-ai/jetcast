package jetcast

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

// History configures event retention.
type History struct {
	// MaxAge is how long events are retained, 5 minutes by default. Clients
	// that lose events for longer cannot recover them.
	MaxAge time.Duration
	// MaxBytes caps the stream size, 1 GiB by default.
	MaxBytes int64
	// Replicas is the stream replica count, 1 by default.
	Replicas int
	// Storage is the stream storage, file by default.
	Storage jetstream.StorageType
}

func (h History) withDefaults() History {
	if h.MaxAge <= 0 {
		h.MaxAge = 5 * time.Minute
	}
	if h.MaxBytes <= 0 {
		h.MaxBytes = 1 << 30
	}
	if h.Replicas <= 0 {
		h.Replicas = 1
	}
	return h
}

// streamConfig returns the configuration of the event stream.
func streamConfig(cfg Config, h History) jetstream.StreamConfig {
	sub := subjects{cfg.Prefix}
	return jetstream.StreamConfig{
		Name:        cfg.Stream,
		Description: "jetcast channel events",
		Subjects:    []string{sub.inAll()},
		Retention:   jetstream.LimitsPolicy,
		Discard:     jetstream.DiscardOld,
		MaxAge:      h.MaxAge,
		MaxBytes:    h.MaxBytes,
		MaxMsgs:     -1,
		Storage:     h.Storage,
		Replicas:    h.Replicas,
		Duplicates:  min(2*time.Minute, h.MaxAge),
		DenyDelete:  true,
		AllowDirect: false,
		RePublish:   &jetstream.RePublish{Source: sub.inAll(), Destination: sub.evAll()},
	}
}

// checkStreamConfig reports why an existing stream cannot serve as the event
// stream. Recovery relies on oldest-first eviction across the whole stream and
// on leader reads.
func checkStreamConfig(got, want jetstream.StreamConfig) error {
	switch {
	case len(got.Subjects) != 1 || got.Subjects[0] != want.Subjects[0]:
		return fmt.Errorf("subjects %v, want %v", got.Subjects, want.Subjects)
	case got.RePublish == nil || got.RePublish.Source != want.RePublish.Source ||
		got.RePublish.Destination != want.RePublish.Destination || got.RePublish.HeadersOnly:
		return errors.New("republish does not match")
	case got.Retention != jetstream.LimitsPolicy || got.Discard != jetstream.DiscardOld:
		return errors.New("retention must be limits with discard old")
	case got.MaxMsgsPerSubject > 0 || got.MaxMsgs > 0:
		return errors.New("per-subject or message count limits break recovery")
	case got.AllowDirect:
		return errors.New("allow direct must be off")
	case got.AllowRollup || got.AllowMsgTTL || !got.DenyDelete:
		return errors.New("rollup, message TTL and deletes must be disabled")
	case got.Sealed || got.NoAck:
		return errors.New("stream must not be sealed or unacknowledged")
	case got.MaxAge <= 0:
		return errors.New("stream needs a max age")
	}
	return nil
}

// history reads the event stream for heads and recovery. Reads go to the
// stream leader.
type history struct {
	js   jetstream.JetStream
	name string
	// stream reads messages. Its cached info is never refreshed: the
	// handle is not safe for concurrent Info and GetMsg calls.
	stream jetstream.Stream
	sub    subjects
}

func epochOf(info *jetstream.StreamInfo) string {
	return strconv.FormatInt(info.Created.UnixNano(), 10)
}

// state returns the stream's epoch, first and last sequences.
func (h *history) state(ctx context.Context) (epoch string, first, last uint64, err error) {
	st, err := h.js.Stream(ctx, h.name)
	if err != nil {
		return "", 0, 0, err
	}
	info := st.CachedInfo()
	return epochOf(info), info.State.FirstSeq, info.State.LastSeq, nil
}

// head returns the sequence of the latest retained event of a channel, or 0.
func (h *history) head(ctx context.Context, c Channel) (uint64, error) {
	msg, err := h.stream.GetLastMsgForSubject(ctx, h.sub.in(c))
	if errors.Is(err, jetstream.ErrMsgNotFound) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	return msg.Sequence, nil
}

// recoverLimits bounds one recovery batch.
type recoverLimits struct {
	count int
	bytes int
}

// recoverBatch sends the retained events of a channel with sequences in
// (pos, upTo) to send, oldest first, and returns the batch result. The result
// is complete only if nothing after pos was evicted while reading, which holds
// when the stream's first sequence, read after the events, is at most pos+1:
// eviction removes the oldest messages of the whole stream first.
func (h *history) recoverBatch(ctx context.Context, c Channel, req RecoverRequest, lim recoverLimits,
	send func(msg *jetstream.RawStreamMsg, prev uint64) error) (RecoverResult, error) {
	epoch, _, last, err := h.state(ctx)
	if err != nil {
		return RecoverResult{}, err
	}
	if req.Epoch != epoch {
		return RecoverResult{Recovered: false, Reason: ReasonEpoch, Epoch: epoch, Position: last}, nil
	}
	upTo := req.UpTo
	if upTo == 0 || upTo > last+1 {
		upTo = last + 1
	}
	res := RecoverResult{Epoch: epoch, Position: last}
	subject := h.sub.in(c)
	seq, prev, size := req.Pos+1, req.Pos, 0
	next := upTo - 1
	for seq < upTo {
		if res.Count >= lim.count || res.Count > 0 && size >= lim.bytes {
			res.More = true
			next = prev
			break
		}
		msg, err := h.stream.GetMsg(ctx, seq, jetstream.WithGetMsgSubject(subject))
		if errors.Is(err, jetstream.ErrMsgNotFound) {
			break
		}
		if err != nil {
			return RecoverResult{}, err
		}
		if msg.Sequence >= upTo {
			break
		}
		if err := send(msg, prev); err != nil {
			return RecoverResult{}, err
		}
		res.Count++
		size += len(msg.Data) + len(msg.Header)*32
		prev, seq = msg.Sequence, msg.Sequence+1
	}
	if req.Pos > next {
		next = req.Pos
	}
	epoch2, first, last2, err := h.state(ctx)
	if err != nil {
		return RecoverResult{}, err
	}
	switch {
	case epoch2 != epoch:
		// Positions of the old stream mean nothing in the new one.
		head, err := h.head(ctx, c)
		if err != nil {
			return RecoverResult{}, err
		}
		return RecoverResult{Recovered: false, Reason: ReasonEpoch, Epoch: epoch2, Position: last2, Head: head}, nil
	case first > req.Pos+1:
		head, err := h.head(ctx, c)
		if err != nil {
			return RecoverResult{}, err
		}
		return RecoverResult{Recovered: false, Reason: ReasonExpired, Epoch: epoch, Position: last, Head: head}, nil
	}
	res.Recovered = true
	res.Next = next
	return res, nil
}
