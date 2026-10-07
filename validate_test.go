package jetcast_test

import (
	"testing"

	"github.com/runforyou-ai/jetcast"
)

func TestValidation(t *testing.T) {
	for _, name := range []string{"news", "orders.42", "a-b.c_d.1", "a.b.c.d.e.f.g.h"} {
		if err := jetcast.ValidateChannelName(name); err != nil {
			t.Errorf("%q: %v", name, err)
		}
	}
	for _, name := range []string{"", ".", "a.", "a..b", "a.*", "a.>", "a b", "订单", "a.b.c.d.e.f.g.h.i"} {
		if err := jetcast.ValidateChannelName(name); err == nil {
			t.Errorf("%q accepted", name)
		}
	}
	for _, p := range []string{"users.42.>", "teams.*.board", ">", "*"} {
		if err := jetcast.ValidatePattern(p); err != nil {
			t.Errorf("pattern %q: %v", p, err)
		}
	}
	for _, p := range []string{"a.>.b", "a.**", ""} {
		if err := jetcast.ValidatePattern(p); err == nil {
			t.Errorf("pattern %q accepted", p)
		}
	}
	cases := []struct {
		pattern, name string
		want          bool
	}{
		{">", "a", true}, {">", "a.b", true}, {"a.>", "a", false}, {"a.*", "a.b", true},
		{"a.*", "a.b.c", false}, {"a.b", "a.b", true}, {"*", "a.b", false},
	}
	for _, c := range cases {
		if got := jetcast.MatchPattern(c.pattern, c.name); got != c.want {
			t.Errorf("MatchPattern(%q, %q) = %v", c.pattern, c.name, got)
		}
	}
	for _, id := range []string{"", "a.b", "a b", string(make([]byte, 65))} {
		if jetcast.ValidateID(id) == nil {
			t.Errorf("ID %q accepted", id)
		}
	}
	if jetcast.ValidateSocketID("AAAAAAAAAAAAAAAAAAAAA-") == nil || jetcast.ValidateSocketID("AAAAAAAAAAAAAAAAAAAAAA") != nil {
		t.Error("socket validation")
	}
	if c, err := jetcast.ParseChannel("prv.orders.42"); err != nil || c != jetcast.Private("orders.42") {
		t.Errorf("ParseChannel: %v %v", c, err)
	}
	if _, err := jetcast.ParseChannel("xyz.orders"); err == nil {
		t.Error("unknown kind accepted")
	}
}
