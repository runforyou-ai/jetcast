package client

import (
	"context"
	"errors"
	"slices"
	"sync"

	"github.com/nats-io/nats.go"
)

// errStale reports a request whose subscription attempt was replaced while it
// waited for a slot.
var errStale = errors.New("jetcast: request no longer needed")

// slots is a semaphore bounding the requests a connection has in flight.
// Urgent waiters, such as relay renewals whose leases are running, are served
// before the others.
type slots struct {
	mu     sync.Mutex
	free   int
	urgent []chan struct{}
	normal []chan struct{}
}

func newSlots(n int) *slots { return &slots{free: n} }

// acquire waits for a slot until ctx ends or done is closed.
func (s *slots) acquire(ctx context.Context, done <-chan struct{}, urgent bool) error {
	s.mu.Lock()
	if s.free > 0 && (urgent || len(s.urgent) == 0 && len(s.normal) == 0) {
		s.free--
		s.mu.Unlock()
		return nil
	}
	ch := make(chan struct{})
	if urgent {
		s.urgent = append(s.urgent, ch)
	} else {
		s.normal = append(s.normal, ch)
	}
	s.mu.Unlock()
	var err error
	select {
	case <-ch:
		return nil
	case <-ctx.Done():
		err = ctx.Err()
	case <-done:
		err = nats.ErrConnectionClosed
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if i := slices.Index(s.urgent, ch); i >= 0 {
		s.urgent = slices.Delete(s.urgent, i, i+1)
		return err
	}
	if i := slices.Index(s.normal, ch); i >= 0 {
		s.normal = slices.Delete(s.normal, i, i+1)
		return err
	}
	// The slot was handed over meanwhile: pass it on.
	s.releaseLocked()
	return err
}

// release frees a slot, handing it to the next waiter.
func (s *slots) release() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.releaseLocked()
}

func (s *slots) releaseLocked() {
	var q *[]chan struct{}
	switch {
	case len(s.urgent) > 0:
		q = &s.urgent
	case len(s.normal) > 0:
		q = &s.normal
	default:
		s.free++
		return
	}
	ch := (*q)[0]
	*q = (*q)[1:]
	close(ch)
}
