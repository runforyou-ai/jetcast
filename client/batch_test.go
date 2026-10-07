package client

import (
	"strconv"
	"testing"

	"github.com/nats-io/nats.go"
	"github.com/runforyou-ai/jetcast"
)

func event(seq, prev uint64) *nats.Msg {
	m := nats.NewMsg("x")
	m.Header.Set(jetcast.HeaderSequence, strconv.FormatUint(seq, 10))
	m.Header.Set(jetcast.HeaderLastSequence, strconv.FormatUint(prev, 10))
	return m
}

func TestCompleteBatch(t *testing.T) {
	full := []*nats.Msg{event(12, 10), event(15, 12), event(20, 15)}
	if !completeBatch(full, jetcast.RecoverResult{Count: 3}, 10) {
		t.Error("intact batch rejected")
	}
	if completeBatch(full[:2], jetcast.RecoverResult{Count: 3}, 10) {
		t.Error("batch missing its tail accepted")
	}
	if completeBatch([]*nats.Msg{full[0], full[2]}, jetcast.RecoverResult{Count: 2}, 10) {
		t.Error("batch with a hole accepted")
	}
	if completeBatch(full, jetcast.RecoverResult{Count: 3}, 9) {
		t.Error("batch not starting at pos accepted")
	}
	if !completeBatch(nil, jetcast.RecoverResult{Count: 0}, 10) {
		t.Error("empty batch rejected")
	}
}
