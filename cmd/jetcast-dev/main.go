// Command jetcast-dev runs an embedded NATS server and a jetcast server for
// development and for the TypeScript SDK's tests. It is not for production.
//
// Clients authenticate with tokens of the form "<user>:<session>". An HTTP
// control API changes grants and authorizations and broadcasts events:
//
//	GET  /env                                      URLs and prefix
//	POST /grant      {"user", "patterns": []}      direct grants of a user
//	POST /allow      {"user", "channel"}           allow a relayed private channel
//	POST /deny       {"user", "channel"}           withdraw it
//	POST /invalidate {"token"}                     reject a token from now on
//	POST /broadcast  {"name", "channels": ["pub.news"], "data", "origin"}
//	POST /refresh    {"user"}
//	POST /leave      {"channel", "user"}
//	POST /disconnect {"user", "session"}
//	POST /kick       {"socket"}                    drop a connection without revoking it
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"time"

	"github.com/runforyou-ai/jetcast"
	"github.com/runforyou-ai/jetcast/embedded"
	"github.com/runforyou-ai/jetcast/internal/devenv"
)

type state struct {
	mu      sync.Mutex
	grants  map[string][]string
	allowed map[string]bool
	invalid map[string]bool
}

func main() {
	port := flag.Int("port", 0, "NATS client port (random when 0)")
	wsPort := flag.Int("ws", 0, "NATS WebSocket port (random when 0)")
	httpAddr := flag.String("http", "127.0.0.1:8090", "control API address")
	maxAge := flag.Duration("max-age", time.Minute, "event retention")
	ttl := flag.Duration("ttl", time.Hour, "maximum connection lifetime")
	renew := flag.Duration("renew", 20*time.Second, "relay renewal interval")
	ephemeral := flag.String("ephemeral", "typing.>", "comma-separated ephemeral channel patterns")
	verbose := flag.Bool("v", false, "debug logging")
	flag.Parse()
	if *verbose {
		slog.SetLogLoggerLevel(slog.LevelDebug)
	}

	env, err := devenv.Start(devenv.Options{Port: *port, WSPort: *wsPort})
	if err != nil {
		log.Fatal(err)
	}
	defer env.Close()
	nc, err := env.ConnectApp()
	if err != nil {
		log.Fatal(err)
	}
	defer nc.Close()

	cfg := jetcast.Config{}
	if *ephemeral != "" {
		cfg.Ephemeral = strings.Split(*ephemeral, ",")
	}
	srv, err := jetcast.NewServer(nc, jetcast.ServerOptions{
		Config:           cfg,
		Account:          env.ClientAccount,
		CalloutSigner:    env.Issuer,
		Admin:            embedded.Admin(env.Server),
		ManageStreams:    true,
		History:          jetcast.History{MaxAge: *maxAge},
		MaxConnectionTTL: *ttl,
		RenewInterval:    *renew,
	})
	if err != nil {
		log.Fatal(err)
	}
	st := &state{grants: map[string][]string{}, allowed: map[string]bool{}, invalid: map[string]bool{}}
	srv.Authenticate(func(ctx context.Context, r jetcast.AuthRequest) (jetcast.User, error) {
		st.mu.Lock()
		bad := st.invalid[r.Token]
		st.mu.Unlock()
		user, session, ok := strings.Cut(r.Token, ":")
		if !ok || bad {
			return jetcast.User{}, errors.New("bad token")
		}
		return jetcast.User{
			ID: user, Session: session, Info: map[string]string{"name": user},
			ConnectionTypes: []string{jetcast.ConnectionWebSocket, jetcast.ConnectionStandard},
		}, nil
	})
	srv.Grants(func(ctx context.Context, u jetcast.User) ([]string, error) {
		st.mu.Lock()
		defer st.mu.Unlock()
		return st.grants[u.ID], nil
	})
	authorize := func(ctx context.Context, u jetcast.User, p jetcast.Params) (bool, error) {
		parts := make([]string, 0, len(p))
		for i := range len(p) {
			parts = append(parts, p[fmt.Sprintf("t%d", i)])
		}
		st.mu.Lock()
		defer st.mu.Unlock()
		return st.allowed[u.ID+"|"+strings.Join(parts, ".")], nil
	}
	for n := 1; n <= 8; n++ {
		tokens := make([]string, n)
		for i := range tokens {
			tokens[i] = fmt.Sprintf("{t%d}", i)
		}
		if err := srv.Channel(strings.Join(tokens, "."), authorize); err != nil {
			log.Fatal(err)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	if err := srv.Start(ctx); err != nil {
		log.Fatal(err)
	}
	cancel()
	defer func() { _ = srv.Close() }()

	mux := http.NewServeMux()
	handle := func(path string, f func(ctx context.Context, body map[string]any) (any, error)) {
		mux.HandleFunc(path, func(w http.ResponseWriter, r *http.Request) {
			var body map[string]any
			if r.Method == http.MethodPost {
				if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
					http.Error(w, err.Error(), http.StatusBadRequest)
					return
				}
			}
			out, err := f(r.Context(), body)
			if err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(out)
		})
	}
	str := func(b map[string]any, k string) string { s, _ := b[k].(string); return s }
	strs := func(b map[string]any, k string) []string {
		var out []string
		list, _ := b[k].([]any)
		for _, v := range list {
			if s, ok := v.(string); ok {
				out = append(out, s)
			}
		}
		return out
	}
	handle("/env", func(context.Context, map[string]any) (any, error) {
		return map[string]string{"wsUrl": env.WSURL, "natsUrl": env.URL, "prefix": "jetcast"}, nil
	})
	handle("/grant", func(_ context.Context, b map[string]any) (any, error) {
		st.mu.Lock()
		defer st.mu.Unlock()
		st.grants[str(b, "user")] = strs(b, "patterns")
		return struct{}{}, nil
	})
	handle("/allow", func(_ context.Context, b map[string]any) (any, error) {
		st.mu.Lock()
		defer st.mu.Unlock()
		st.allowed[str(b, "user")+"|"+str(b, "channel")] = true
		return struct{}{}, nil
	})
	handle("/deny", func(_ context.Context, b map[string]any) (any, error) {
		st.mu.Lock()
		defer st.mu.Unlock()
		delete(st.allowed, str(b, "user")+"|"+str(b, "channel"))
		return struct{}{}, nil
	})
	handle("/invalidate", func(_ context.Context, b map[string]any) (any, error) {
		st.mu.Lock()
		defer st.mu.Unlock()
		st.invalid[str(b, "token")] = true
		return struct{}{}, nil
	})
	handle("/broadcast", func(ctx context.Context, b map[string]any) (any, error) {
		var chans []jetcast.Channel
		for _, c := range strs(b, "channels") {
			ch, err := jetcast.ParseChannel(c)
			if err != nil {
				return nil, err
			}
			chans = append(chans, ch)
		}
		data, _ := json.Marshal(b["data"])
		res, err := srv.Broadcast(ctx, jetcast.Event{Name: str(b, "name"), Channels: chans, Data: data, Origin: str(b, "origin")})
		if err != nil {
			return nil, err
		}
		seqs := make([]uint64, len(res.Channels))
		for i, c := range res.Channels {
			seqs[i] = c.Sequence
		}
		return map[string]any{"id": res.ID, "sequences": seqs}, nil
	})
	handle("/refresh", func(ctx context.Context, b map[string]any) (any, error) {
		return struct{}{}, srv.Refresh(ctx, jetcast.ByUser(str(b, "user")))
	})
	handle("/leave", func(ctx context.Context, b map[string]any) (any, error) {
		ch, err := jetcast.ParseChannel(str(b, "channel"))
		if err != nil {
			return nil, err
		}
		return struct{}{}, srv.Leave(ctx, ch, jetcast.ByUser(str(b, "user")))
	})
	handle("/disconnect", func(ctx context.Context, b map[string]any) (any, error) {
		t := jetcast.ByUser(str(b, "user"))
		if s := str(b, "session"); s != "" {
			t = jetcast.BySession(t.User, s)
		}
		return srv.Disconnect(ctx, t)
	})
	handle("/kick", func(_ context.Context, b map[string]any) (any, error) {
		cz, err := env.Server.Connz(nil)
		if err != nil {
			return nil, err
		}
		for _, ci := range cz.Conns {
			if ci.Name == str(b, "socket") {
				return struct{}{}, env.Server.DisconnectClientByID(ci.Cid)
			}
		}
		return nil, errors.New("socket not connected")
	})
	hs := &http.Server{Addr: *httpAddr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		if err := hs.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatal(err)
		}
	}()
	fmt.Printf("jetcast-dev ready ws=%s nats=%s http=%s\n", env.WSURL, env.URL, *httpAddr)

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt)
	<-sig
	_ = hs.Close()
}
