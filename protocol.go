package jetcast

import "encoding/json"

// Message headers set by jetcast. Applications cannot set them.
const (
	HeaderEvent   = "Jetcast-Event"
	HeaderID      = "Jetcast-Id"
	HeaderOrigin  = "Jetcast-Origin"
	HeaderChannel = "Jetcast-Channel"
	HeaderSid     = "Jetcast-Sid"
	HeaderStatus  = "Jetcast-Status"

	// Headers added by JetStream RePublish and rebuilt on recovery.
	HeaderSequence     = "Nats-Sequence"
	HeaderLastSequence = "Nats-Last-Sequence"
	HeaderTimeStamp    = "Nats-Time-Stamp"
)

// Request operations, the last token of a request subject.
const (
	opHello   = "hello"
	opSub     = "sub"
	opHeads   = "heads"
	opRecover = "recover"
	opRenew   = "renew"
	opLeave   = "leave"
)

// Delivery paths of a private channel.
const (
	PathDirect = "direct"
	PathRelay  = "relay"
)

// Error codes returned to clients.
const (
	CodeDenied      = "denied"
	CodeInvalid     = "invalid"
	CodeUnavailable = "unavailable"
	CodeOverloaded  = "overloaded"
)

// Reasons for recovered=false.
const (
	ReasonInitial   = "initial"
	ReasonExpired   = "expired"
	ReasonEpoch     = "epoch"
	ReasonTooFar    = "too_far"
	ReasonEphemeral = "ephemeral"
)

// Control message types sent to a connection.
const (
	CtlRefresh     = "refresh"
	CtlDisconnect  = "disconnect"
	CtlLeave       = "leave"
	CtlDenied      = "denied"
	CtlInterrupted = "interrupted"
)

// ErrorBody is the error member of a response.
type ErrorBody struct {
	Code    string `json:"code"`
	Message string `json:"message,omitempty"`
}

// HelloResponse answers a hello request.
type HelloResponse struct {
	Error     *ErrorBody      `json:"error,omitempty"`
	User      string          `json:"user,omitempty"`
	Info      json.RawMessage `json:"info,omitempty"`
	Grants    []string        `json:"grants,omitempty"`
	ExpiresAt int64           `json:"expiresAt,omitempty"` // Unix milliseconds
	Epoch     string          `json:"epoch,omitempty"`
	MaxAgeMs  int64           `json:"maxAgeMs,omitempty"`
	RenewMs   int64           `json:"renewMs,omitempty"`
	Node      string          `json:"node,omitempty"`
	Prefix    string          `json:"prefix,omitempty"`
}

// SubRequest subscribes to a channel.
type SubRequest struct {
	Channel string `json:"channel"` // "<kind>.<name>"
	Sid     string `json:"sid"`
	Path    string `json:"path,omitempty"` // the client's preference
}

// SubResponse answers a sub request.
type SubResponse struct {
	Error       *ErrorBody `json:"error,omitempty"`
	Path        string     `json:"path,omitempty"`
	Node        string     `json:"node,omitempty"`
	Epoch       string     `json:"epoch,omitempty"`
	Recoverable bool       `json:"recoverable"`
	Head        uint64     `json:"head"`     // latest event sequence of the channel, 0 if none
	Position    uint64     `json:"position"` // last sequence of the stream
}

// HeadsRequest asks for the latest sequences of channels.
type HeadsRequest struct {
	Epoch    string   `json:"epoch"`
	Channels []string `json:"channels"`
}

// HeadsResponse answers a heads request.
type HeadsResponse struct {
	Error  *ErrorBody        `json:"error,omitempty"`
	Epoch  string            `json:"epoch,omitempty"`
	First  uint64            `json:"first"`
	Last   uint64            `json:"last"`
	Heads  map[string]uint64 `json:"heads,omitempty"`
	Denied []string          `json:"denied,omitempty"`
}

// RecoverRequest asks for the events of a channel after Pos and before UpTo.
// UpTo of zero means up to the end of the stream.
type RecoverRequest struct {
	Channel string `json:"channel"`
	Epoch   string `json:"epoch"`
	Pos     uint64 `json:"pos"`
	UpTo    uint64 `json:"upTo,omitempty"`
}

// RecoverResult ends a recovery batch. Events precede it on the reply subject.
type RecoverResult struct {
	Error     *ErrorBody `json:"error,omitempty"`
	Recovered bool       `json:"recovered"`
	Reason    string     `json:"reason,omitempty"`
	More      bool       `json:"more,omitempty"`
	Next      uint64     `json:"next"` // position reached by this batch
	Count     int        `json:"count"`
	Epoch     string     `json:"epoch,omitempty"`
	Head      uint64     `json:"head"`     // latest event sequence of the channel
	Position  uint64     `json:"position"` // last sequence of the stream
}

// RenewRequest renews relay leases on one node.
type RenewRequest struct {
	Sids []string `json:"sids"`
}

// RenewResponse lists the sids the node no longer relays.
type RenewResponse struct {
	Error   *ErrorBody `json:"error,omitempty"`
	Missing []string   `json:"missing,omitempty"`
}

// LeaveRequest ends a relay.
type LeaveRequest struct {
	Sid string `json:"sid"`
}

// Control is a control message sent to a connection.
type Control struct {
	Type    string `json:"type"`
	Sid     string `json:"sid,omitempty"`
	Channel string `json:"channel,omitempty"`
	Reason  string `json:"reason,omitempty"`
}

// Status header values of recovery messages.
const (
	statusEvent = "event"
	statusDone  = "done"
)
