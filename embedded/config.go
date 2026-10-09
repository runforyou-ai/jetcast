package embedded

import (
	"errors"
	"fmt"
	"strings"
)

// Accounts describes the NATS accounts jetcast runs on: an application
// account holding the event stream, the registry and the application's own
// connections, and a separate client account the auth callout places clients
// in. The client account only exchanges jetcast's subjects with the
// application account and carries per-connection limits, which NATS does not
// apply from callout-issued JWTs.
//
// Config renders the accounts, system_account and authorization blocks of a
// NATS server configuration. It suits embedded servers as well as standalone
// ones: print it once and adapt it.
type Accounts struct {
	// Prefix must match jetcast.Config.Prefix, "jetcast" by default.
	Prefix string
	// App is the application account, "APP" by default.
	App string
	// Clients is the account clients are placed in, "CLIENT" by default.
	// Set jetcast.ServerOptions.Account to it.
	Clients string
	// AppUser and AppPassword are the application's credentials; AppUser is
	// "app" by default. AppPassword is required.
	AppUser     string
	AppPassword string
	// System is the system account, "SYS" by default. A system user is only
	// created when SystemPassword is set; jetcast.SystemAdmin needs one.
	System         string
	SystemUser     string // "sys" by default
	SystemPassword string
	// Issuer is the public key of jetcast.ServerOptions.CalloutSigner.
	Issuer string
	// XKey is the public curve key of jetcast.ServerOptions.CalloutXKey;
	// callouts are encrypted when it is set.
	XKey string
	// AuthTimeout bounds authentication, including the callout, in seconds;
	// 5 by default.
	AuthTimeout int
	// Limits apply to the client account.
	Limits ClientLimits
}

// ClientLimits bound the client account. NATS applies MaxSubscriptions and
// MaxPayload to every connection of the account and MaxConnections to the
// account as a whole. Zero selects the default; -1 means unlimited.
type ClientLimits struct {
	// MaxConnections caps client connections across the account; unlimited
	// by default, since it depends on the deployment.
	MaxConnections int
	// MaxSubscriptions caps the subscriptions of one connection, 1000 by
	// default. Every directly subscribed channel uses one, and a client uses
	// a few more for its own subjects and recoveries.
	MaxSubscriptions int
	// MaxPayload caps the size of messages a client publishes, 64 KiB by
	// default. Clients only publish requests.
	MaxPayload int
}

// Config renders the NATS configuration blocks of the accounts.
func (a Accounts) Config() (string, error) {
	def := func(v *string, d string) {
		if *v == "" {
			*v = d
		}
	}
	def(&a.Prefix, "jetcast")
	def(&a.App, "APP")
	def(&a.Clients, "CLIENT")
	def(&a.AppUser, "app")
	def(&a.System, "SYS")
	def(&a.SystemUser, "sys")
	if a.AuthTimeout <= 0 {
		a.AuthTimeout = 5
	}
	limit := func(v, d int) int {
		if v == 0 {
			return d
		}
		return v
	}
	maxConns := limit(a.Limits.MaxConnections, -1)
	maxSubs := limit(a.Limits.MaxSubscriptions, 1000)
	maxPayload := limit(a.Limits.MaxPayload, 64<<10)
	if a.AppPassword == "" || a.Issuer == "" {
		return "", errors.New("embedded: AppPassword and Issuer are required")
	}
	for _, name := range []string{a.App, a.Clients, a.System} {
		if !account(name) {
			return "", fmt.Errorf("embedded: invalid account name %q", name)
		}
	}
	if a.App == a.Clients || a.App == a.System || a.Clients == a.System {
		return "", errors.New("embedded: App, Clients and System must be distinct accounts")
	}
	if !prefix(a.Prefix) {
		return "", fmt.Errorf("embedded: invalid prefix %q", a.Prefix)
	}

	p := a.Prefix
	var b strings.Builder
	fmt.Fprintf(&b, "accounts {\n")
	fmt.Fprintf(&b, "  %s {\n", a.App)
	fmt.Fprintf(&b, "    jetstream: enabled\n")
	fmt.Fprintf(&b, "    users: [ { user: %q, password: %q } ]\n", a.AppUser, a.AppPassword)
	fmt.Fprintf(&b, "    exports: [\n")
	fmt.Fprintf(&b, "      { stream: %q, accounts: [ %s ] }\n", p+".ev.>", a.Clients)
	fmt.Fprintf(&b, "      { stream: %q, accounts: [ %s ] }\n", p+".c.>", a.Clients)
	fmt.Fprintf(&b, "    ]\n")
	fmt.Fprintf(&b, "    imports: [ { stream: { account: %s, subject: %q } } ]\n", a.Clients, p+".rq.>")
	fmt.Fprintf(&b, "  }\n")
	fmt.Fprintf(&b, "  %s {\n", a.Clients)
	fmt.Fprintf(&b, "    limits: { max_connections: %d, max_subscriptions: %d, max_payload: %d }\n", maxConns, maxSubs, maxPayload)
	fmt.Fprintf(&b, "    exports: [ { stream: %q, accounts: [ %s ] } ]\n", p+".rq.>", a.App)
	fmt.Fprintf(&b, "    imports: [\n")
	fmt.Fprintf(&b, "      { stream: { account: %s, subject: %q } }\n", a.App, p+".ev.>")
	fmt.Fprintf(&b, "      { stream: { account: %s, subject: %q } }\n", a.App, p+".c.>")
	fmt.Fprintf(&b, "    ]\n")
	fmt.Fprintf(&b, "  }\n")
	authUsers := fmt.Sprintf("%q", a.AppUser)
	if a.SystemPassword != "" {
		fmt.Fprintf(&b, "  %s { users: [ { user: %q, password: %q } ] }\n", a.System, a.SystemUser, a.SystemPassword)
		authUsers += fmt.Sprintf(", %q", a.SystemUser)
	} else {
		fmt.Fprintf(&b, "  %s {}\n", a.System)
	}
	fmt.Fprintf(&b, "}\n")
	fmt.Fprintf(&b, "system_account: %s\n", a.System)
	fmt.Fprintf(&b, "authorization {\n")
	fmt.Fprintf(&b, "  timeout: %ds\n", a.AuthTimeout)
	fmt.Fprintf(&b, "  auth_callout {\n")
	fmt.Fprintf(&b, "    issuer: %q\n", a.Issuer)
	fmt.Fprintf(&b, "    account: %s\n", a.App)
	fmt.Fprintf(&b, "    auth_users: [ %s ]\n", authUsers)
	if a.XKey != "" {
		fmt.Fprintf(&b, "    xkey: %q\n", a.XKey)
	}
	fmt.Fprintf(&b, "  }\n")
	fmt.Fprintf(&b, "}\n")
	return b.String(), nil
}

// account reports whether name is a plain account name.
func account(name string) bool {
	if name == "" {
		return false
	}
	for i := 0; i < len(name); i++ {
		c := name[i]
		ok := c >= 'A' && c <= 'Z' || c >= 'a' && c <= 'z' || c >= '0' && c <= '9' || c == '_' || c == '-'
		if !ok {
			return false
		}
	}
	return true
}

// prefix reports whether p is a valid jetcast prefix.
func prefix(p string) bool {
	if p == "" {
		return false
	}
	for i := 0; i < len(p); i++ {
		c := p[i]
		ok := c >= 'a' && c <= 'z' || c >= '0' && c <= '9' || c == '_'
		if !ok {
			return false
		}
	}
	return true
}
