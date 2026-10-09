// Package devenv starts an embedded NATS server configured for jetcast:
// JetStream, WebSocket, a system account and an auth callout. Tests and the
// development server use it.
package devenv

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nkeys"
	"github.com/runforyou-ai/jetcast/embedded"
)

// Env is a running embedded NATS server.
type Env struct {
	Server *server.Server
	// Issuer signs callout responses and user JWTs.
	Issuer nkeys.KeyPair
	// URL and WSURL are the client and WebSocket URLs.
	URL   string
	WSURL string
	// ClientAccount is the account clients are placed in, the value of
	// jetcast.ServerOptions.Account.
	ClientAccount string
	dir           string
	own           bool
}

// Options configure Start.
type Options struct {
	// Dir stores JetStream data; a temporary directory is used when empty.
	Dir string
	// Port and WSPort default to random ports.
	Port   int
	WSPort int
	// Issuer defaults to a new account key pair.
	Issuer nkeys.KeyPair
	// Cluster joins the server to a cluster: ClusterPort is this server's
	// route port and Routes lists every server's route port.
	ClusterName string
	ClusterPort int
	Routes      []int
	// Name defaults to "jetcast-dev".
	Name string
	// SharedAccount places clients in the application account instead of
	// a separate client account.
	SharedAccount bool
	// Limits apply to the client account.
	Limits embedded.ClientLimits
}

// Start runs the server.
func Start(o Options) (*Env, error) {
	e := &Env{dir: o.Dir, Issuer: o.Issuer}
	if e.dir == "" {
		dir, err := os.MkdirTemp("", "jetcast-devenv-")
		if err != nil {
			return nil, err
		}
		e.dir, e.own = dir, true
	}
	if e.Issuer == nil {
		kp, err := nkeys.CreateAccount()
		if err != nil {
			return nil, err
		}
		e.Issuer = kp
	}
	pub, err := e.Issuer.PublicKey()
	if err != nil {
		return nil, err
	}
	port, wsPort := o.Port, o.WSPort
	if port == 0 {
		port = -1
	}
	if wsPort == 0 {
		wsPort = -1
	}
	name := o.Name
	if name == "" {
		name = "jetcast-dev"
	}
	cluster := ""
	if o.ClusterName != "" {
		routes := ""
		for _, p := range o.Routes {
			routes += fmt.Sprintf("    \"nats-route://127.0.0.1:%d\"\n", p)
		}
		cluster = fmt.Sprintf("cluster {\n  name: %s\n  listen: \"127.0.0.1:%d\"\n  routes: [\n%s  ]\n}\n", o.ClusterName, o.ClusterPort, routes)
	}
	accounts, err := embedded.Accounts{
		AppPassword: "app", SystemPassword: "sys", Issuer: pub, Limits: o.Limits,
	}.Config()
	if err != nil {
		return nil, err
	}
	e.ClientAccount = "CLIENT"
	if o.SharedAccount {
		if o.Limits != (embedded.ClientLimits{}) {
			return nil, fmt.Errorf("devenv: Limits need a separate client account")
		}
		e.ClientAccount = "APP"
		accounts = fmt.Sprintf(`
accounts {
  APP { jetstream: enabled, users: [ { user: app, password: app } ] }
  SYS { users: [ { user: sys, password: sys } ] }
}
system_account: SYS
authorization {
  timeout: 5s
  auth_callout { issuer: %q, account: APP, auth_users: [ app, sys ] }
}
`, pub)
	}
	conf := fmt.Sprintf(`
listen: "127.0.0.1:%d"
server_name: %s
%s
jetstream { store_dir: %q }
websocket { listen: "127.0.0.1:%d", no_tls: true }
%s
`, port, name, cluster, filepath.Join(e.dir, "js"), wsPort, accounts)
	confFile := filepath.Join(e.dir, "nats.conf")
	if err := os.WriteFile(confFile, []byte(conf), 0o600); err != nil {
		return nil, err
	}
	opts, err := server.ProcessConfigFile(confFile)
	if err != nil {
		return nil, err
	}
	opts.NoLog, opts.NoSigs = true, true
	s, err := server.NewServer(opts)
	if err != nil {
		return nil, err
	}
	go s.Start()
	if !s.ReadyForConnections(10 * time.Second) {
		s.Shutdown()
		return nil, fmt.Errorf("devenv: server not ready")
	}
	e.Server = s
	e.URL = s.ClientURL()
	info := s.PortsInfo(5 * time.Second)
	if info == nil || len(info.WebSocket) == 0 {
		s.Shutdown()
		return nil, fmt.Errorf("devenv: no websocket port")
	}
	e.WSURL = info.WebSocket[0]
	return e, nil
}

// ConnectApp connects as the application user, which bypasses the callout.
func (e *Env) ConnectApp(opts ...nats.Option) (*nats.Conn, error) {
	return nats.Connect(e.URL, append([]nats.Option{nats.UserInfo("app", "app"), nats.MaxReconnects(-1)}, opts...)...)
}

// ConnectSys connects to the system account.
func (e *Env) ConnectSys(opts ...nats.Option) (*nats.Conn, error) {
	return nats.Connect(e.URL, append([]nats.Option{nats.UserInfo("sys", "sys")}, opts...)...)
}

// Close stops the server and removes temporary data.
func (e *Env) Close() {
	e.Server.Shutdown()
	e.Server.WaitForShutdown()
	if e.own {
		_ = os.RemoveAll(e.dir)
	}
}
