// Package jetcast broadcasts server events to browsers and other clients that
// connect directly to NATS, in the spirit of Laravel Broadcasting and Echo.
//
// Application code broadcasts events on channels; clients listen on channels.
// Public channels need no authorization. Private channels are authorized by the
// application: either at connect time, by granting channel patterns that NATS
// enforces directly, or at subscribe time, by channel authorizers whose
// approved subscriptions the server relays to the client. Events are retained
// in a JetStream stream for a short time, so clients detect missed events and
// recover them, and learn when recovery is impossible.
//
// A [Server] runs on every application node: it answers the NATS auth callout,
// handles client requests and relays private channels. A [Publisher] only
// broadcasts and suits processes that do not serve clients.
//
// See docs/design.md for the protocol.
package jetcast

import (
	"errors"
	"fmt"
	"strings"
)

// Kind distinguishes public from private channels.
type Kind string

const (
	// KindPublic channels can be subscribed by every connection.
	KindPublic Kind = "pub"
	// KindPrivate channels require authorization.
	KindPrivate Kind = "prv"
)

// Channel identifies a channel by kind and name.
type Channel struct {
	Kind Kind
	Name string
}

// Public returns the public channel with the given name.
func Public(name string) Channel { return Channel{Kind: KindPublic, Name: name} }

// Private returns the private channel with the given name.
func Private(name string) Channel { return Channel{Kind: KindPrivate, Name: name} }

// String returns "<kind>.<name>", the form used in headers and keys.
func (c Channel) String() string { return string(c.Kind) + "." + c.Name }

// Validate reports whether c has a valid kind and a literal channel name.
func (c Channel) Validate() error {
	if c.Kind != KindPublic && c.Kind != KindPrivate {
		return fmt.Errorf("%w: unknown kind %q", ErrInvalidChannel, c.Kind)
	}
	return ValidateChannelName(c.Name)
}

// ParseChannel parses the "<kind>.<name>" form returned by [Channel.String].
func ParseChannel(s string) (Channel, error) {
	kind, name, ok := strings.Cut(s, ".")
	if !ok {
		return Channel{}, fmt.Errorf("%w: %q", ErrInvalidChannel, s)
	}
	c := Channel{Kind: Kind(kind), Name: name}
	return c, c.Validate()
}

const (
	maxChannelTokens = 8
	maxChannelLength = 200
	maxIDLength      = 64
)

var (
	// ErrInvalidChannel reports a malformed channel name or pattern.
	ErrInvalidChannel = errors.New("jetcast: invalid channel")
	// ErrInvalidID reports a malformed user, session or socket ID.
	ErrInvalidID = errors.New("jetcast: invalid ID")
	// ErrInvalidConfig reports invalid configuration.
	ErrInvalidConfig = errors.New("jetcast: invalid configuration")
)

func validToken(token string) bool {
	if token == "" {
		return false
	}
	for i := 0; i < len(token); i++ {
		c := token[i]
		ok := c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '-' || c == '_'
		if !ok {
			return false
		}
	}
	return true
}

// ValidateChannelName reports whether name is a literal channel name: one to
// eight dot-separated tokens of ASCII letters, digits, "-" or "_", at most 200
// bytes in total.
func ValidateChannelName(name string) error {
	if name == "" || len(name) > maxChannelLength {
		return fmt.Errorf("%w: %q", ErrInvalidChannel, name)
	}
	tokens := strings.Split(name, ".")
	if len(tokens) > maxChannelTokens {
		return fmt.Errorf("%w: %q has more than %d tokens", ErrInvalidChannel, name, maxChannelTokens)
	}
	for _, t := range tokens {
		if !validToken(t) {
			return fmt.Errorf("%w: %q", ErrInvalidChannel, name)
		}
	}
	return nil
}

// ValidatePattern reports whether pattern is a valid channel pattern: like a
// channel name, but tokens may be "*" and the last token may be ">".
func ValidatePattern(pattern string) error {
	if pattern == "" || len(pattern) > maxChannelLength {
		return fmt.Errorf("%w: pattern %q", ErrInvalidChannel, pattern)
	}
	tokens := strings.Split(pattern, ".")
	if len(tokens) > maxChannelTokens {
		return fmt.Errorf("%w: pattern %q has more than %d tokens", ErrInvalidChannel, pattern, maxChannelTokens)
	}
	for i, t := range tokens {
		if t == "*" || t == ">" && i == len(tokens)-1 {
			continue
		}
		if !validToken(t) {
			return fmt.Errorf("%w: pattern %q", ErrInvalidChannel, pattern)
		}
	}
	return nil
}

// MatchPattern reports whether the literal channel name matches pattern with
// NATS wildcard semantics: "*" matches one token, a final ">" one or more.
func MatchPattern(pattern, name string) bool {
	p := strings.Split(pattern, ".")
	n := strings.Split(name, ".")
	for i, t := range p {
		if t == ">" {
			return len(n) > i
		}
		if i >= len(n) || t != "*" && t != n[i] {
			return false
		}
	}
	return len(p) == len(n)
}

// ValidateID reports whether id is a valid user or session ID: 1 to 64 ASCII
// letters, digits, "-" or "_".
func ValidateID(id string) error {
	if len(id) > maxIDLength || !validToken(id) {
		return fmt.Errorf("%w: %q", ErrInvalidID, id)
	}
	return nil
}

// socketLength is the length of client-generated socket IDs.
const socketLength = 22

// ValidateSocketID reports whether id is a well-formed socket ID: 22 base62
// characters.
func ValidateSocketID(id string) error {
	if len(id) != socketLength {
		return fmt.Errorf("%w: socket %q", ErrInvalidID, id)
	}
	for i := 0; i < len(id); i++ {
		c := id[i]
		ok := c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9'
		if !ok {
			return fmt.Errorf("%w: socket %q", ErrInvalidID, id)
		}
	}
	return nil
}

// Config is the configuration shared by [Server] and [Publisher]. Every
// process of one application uses the same Config.
type Config struct {
	// Prefix is the first token of every subject, "jetcast" by default.
	// Applications sharing a NATS account use distinct prefixes.
	Prefix string
	// Stream is the name of the event stream, "JETCAST" by default. The
	// connection registry is the key-value bucket "<Stream>_CONN".
	Stream string
	// Ephemeral lists channel patterns, applying to both kinds, whose events
	// are not retained: they are published directly, carry no sequence and
	// cannot be recovered. Typing indicators are a typical example.
	Ephemeral []string
}

func (c Config) withDefaults() Config {
	if c.Prefix == "" {
		c.Prefix = "jetcast"
	}
	if c.Stream == "" {
		c.Stream = "JETCAST"
	}
	return c
}

func (c Config) validate() error {
	for i := 0; i < len(c.Prefix); i++ {
		ch := c.Prefix[i]
		valid := ch >= 'a' && ch <= 'z' || ch >= '0' && ch <= '9' || ch == '_'
		if !valid {
			return fmt.Errorf("%w: prefix %q", ErrInvalidConfig, c.Prefix)
		}
	}
	if c.Prefix == "" || !validToken(c.Stream) {
		return fmt.Errorf("%w: prefix %q, stream %q", ErrInvalidConfig, c.Prefix, c.Stream)
	}
	for _, p := range c.Ephemeral {
		if err := ValidatePattern(p); err != nil {
			return err
		}
	}
	return nil
}

// ephemeral reports whether channel name matches an ephemeral pattern.
func (c Config) ephemeral(name string) bool {
	for _, p := range c.Ephemeral {
		if MatchPattern(p, name) {
			return true
		}
	}
	return false
}

// bucket returns the name of the connection registry bucket.
func (c Config) bucket() string { return c.Stream + "_CONN" }
