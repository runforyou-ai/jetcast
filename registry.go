package jetcast

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

// connRecord is the registry entry of one authenticated connection. It is
// created once per socket and never reused; revocation only marks it.
type connRecord struct {
	User      string          `json:"user"`
	Session   string          `json:"session"`
	Info      json.RawMessage `json:"info,omitempty"`
	Grants    []string        `json:"grants,omitempty"`
	ExpiresAt int64           `json:"exp"` // Unix milliseconds, equal to the JWT expiry
	Server    string          `json:"server"`
	CID       uint64          `json:"cid"`
	Revoked   bool            `json:"revoked,omitempty"`
}

// valid reports whether the connection may still make requests.
func (r *connRecord) valid(now time.Time) bool {
	return r != nil && !r.Revoked && now.UnixMilli() < r.ExpiresAt
}

// granted reports whether the connection was granted the private channel.
func (r *connRecord) granted(name string) bool {
	for _, p := range r.Grants {
		if MatchPattern(p, name) {
			return true
		}
	}
	return false
}

// registryCacheTTL bounds how long a node trusts a cached record. Revocations
// also invalidate caches through a node broadcast.
const registryCacheTTL = 5 * time.Second

// registry stores connection records in a key-value bucket whose stream has
// direct gets disabled, so reads see the latest committed value.
//
// Keys:
//
//	s.<socket>            connection record
//	u.<user>.<socket>     user index, value is the session
//	x.<user>              revocation mark of all sessions of a user
//	x.<user>.<session>    revocation mark of one session
type registry struct {
	kv jetstream.KeyValue

	mu    sync.Mutex
	cache map[string]cachedRecord
}

type cachedRecord struct {
	rec *connRecord
	at  time.Time
}

func newRegistry(kv jetstream.KeyValue) *registry {
	return &registry{kv: kv, cache: map[string]cachedRecord{}}
}

// errSocketTaken reports a socket that already has a record.
var errSocketTaken = errors.New("jetcast: socket already registered")

// create registers a new socket.
func (r *registry) create(ctx context.Context, socket string, rec *connRecord) error {
	b, err := json.Marshal(rec)
	if err != nil {
		return err
	}
	if _, err := r.kv.Create(ctx, "s."+socket, b); err != nil {
		if errors.Is(err, jetstream.ErrKeyExists) {
			return errSocketTaken
		}
		return err
	}
	_, err = r.kv.Put(ctx, "u."+rec.User+"."+socket, []byte(rec.Session))
	return err
}

// get returns the record of a socket, nil if there is none. Cached records
// younger than registryCacheTTL are returned when cached is true.
func (r *registry) get(ctx context.Context, socket string, cached bool) (*connRecord, error) {
	if cached {
		r.mu.Lock()
		c, ok := r.cache[socket]
		r.mu.Unlock()
		if ok && time.Since(c.at) < registryCacheTTL {
			return c.rec, nil
		}
	}
	entry, err := r.kv.Get(ctx, "s."+socket)
	if errors.Is(err, jetstream.ErrKeyNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var rec connRecord
	if err := json.Unmarshal(entry.Value(), &rec); err != nil {
		return nil, fmt.Errorf("jetcast: decode connection record: %w", err)
	}
	r.mu.Lock()
	r.cache[socket] = cachedRecord{rec: &rec, at: time.Now()}
	if len(r.cache) > 100_000 {
		for k, v := range r.cache {
			if time.Since(v.at) >= registryCacheTTL {
				delete(r.cache, k)
			}
		}
	}
	r.mu.Unlock()
	return &rec, nil
}

// invalidate drops cached records.
func (r *registry) invalidate(sockets ...string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, s := range sockets {
		delete(r.cache, s)
	}
}

// revoke marks a socket's record revoked and returns whether it changed.
// Existing revoked records are returned so enforcement can be retried.
func (r *registry) revoke(ctx context.Context, socket string) (*connRecord, bool, error) {
	for range 5 {
		entry, err := r.kv.Get(ctx, "s."+socket)
		if errors.Is(err, jetstream.ErrKeyNotFound) {
			return nil, false, nil
		}
		if err != nil {
			return nil, false, err
		}
		var rec connRecord
		if err := json.Unmarshal(entry.Value(), &rec); err != nil {
			return nil, false, err
		}
		if rec.Revoked {
			return &rec, false, nil
		}
		rec.Revoked = true
		b, _ := json.Marshal(&rec)
		if _, err := r.kv.Update(ctx, "s."+socket, b, entry.Revision()); err == nil {
			r.invalidate(socket)
			return &rec, true, nil
		} else if !errors.Is(err, jetstream.ErrKeyExists) && !errors.Is(err, jetstream.ErrKeyRevisionMismatch) {
			return nil, false, err
		}
	}
	return nil, false, errors.New("jetcast: revoke: too many concurrent updates")
}

// markRevoked writes the revocation mark of a user, or of one session when
// session is not empty.
func (r *registry) markRevoked(ctx context.Context, user, session string) error {
	key := "x." + user
	if session != "" {
		key += "." + session
	}
	_, err := r.kv.Put(ctx, key, nil)
	return err
}

// revokedSince reports whether a revocation mark of the user or session was
// written at or after since.
func (r *registry) revokedSince(ctx context.Context, user, session string, since time.Time) (bool, error) {
	for _, key := range []string{"x." + user, "x." + user + "." + session} {
		entry, err := r.kv.Get(ctx, key)
		if errors.Is(err, jetstream.ErrKeyNotFound) {
			continue
		}
		if err != nil {
			return false, err
		}
		if !entry.Created().Before(since) {
			return true, nil
		}
	}
	return false, nil
}

// sockets lists the sockets of a user, or of one session when session is not
// empty.
func (r *registry) sockets(ctx context.Context, user, session string) ([]string, error) {
	prefix := "u." + user + "."
	lister, err := r.kv.ListKeysFiltered(ctx, prefix+"*")
	if err != nil {
		if errors.Is(err, jetstream.ErrNoKeysFound) {
			return nil, nil
		}
		return nil, err
	}
	// Drain the lister before any read that may fail, so it never blocks.
	var keys []string
	for key := range lister.Keys() {
		keys = append(keys, key)
	}
	var out []string
	for _, key := range keys {
		socket := strings.TrimPrefix(key, prefix)
		if session != "" {
			entry, err := r.kv.Get(ctx, key)
			if errors.Is(err, jetstream.ErrKeyNotFound) {
				continue
			}
			if err != nil {
				return nil, err
			}
			if string(entry.Value()) != session {
				continue
			}
		}
		out = append(out, socket)
	}
	return out, nil
}
