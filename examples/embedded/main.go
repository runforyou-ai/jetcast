// Command embedded is a single-binary application that embeds NATS, serves
// browsers on one port and reverse proxies /nats to the embedded WebSocket
// listener. Run it, then open http://localhost:8080 in two tabs.
//
// Authentication is a toy: the token is the user name. Use your own session
// tokens in a real application.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"path/filepath"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nkeys"
	"github.com/runforyou-ai/jetcast"
	"github.com/runforyou-ai/jetcast/embedded"
)

func main() {
	dir := filepath.Join(os.TempDir(), "jetcast-example")
	issuer, err := loadIssuer(filepath.Join(dir, "issuer.nk"))
	if err != nil {
		log.Fatal(err)
	}
	pub, _ := issuer.PublicKey()

	// Embedded NATS: JetStream, a loopback WebSocket listener, the APP
	// account for the application, the CLIENT account the auth callout
	// places browsers in, with its limits, and the system account. The same
	// configuration works for a standalone nats-server.
	accounts, err := embedded.Accounts{AppPassword: "app", SystemPassword: "sys", Issuer: pub}.Config()
	if err != nil {
		log.Fatal(err)
	}
	conf := fmt.Sprintf(`
listen: "127.0.0.1:-1"
jetstream { store_dir: %q }
websocket { listen: "127.0.0.1:8222", no_tls: true }
%s
`, filepath.Join(dir, "js"), accounts)
	confFile := filepath.Join(dir, "nats.conf")
	if err := os.WriteFile(confFile, []byte(conf), 0o600); err != nil {
		log.Fatal(err)
	}
	opts, err := server.ProcessConfigFile(confFile)
	if err != nil {
		log.Fatal(err)
	}
	ns, err := server.NewServer(opts)
	if err != nil {
		log.Fatal(err)
	}
	ns.Start()
	if !ns.ReadyForConnections(10 * time.Second) {
		log.Fatal("nats not ready")
	}

	nc, err := nats.Connect(ns.ClientURL(), nats.UserInfo("app", "app"))
	if err != nil {
		log.Fatal(err)
	}
	rt, err := jetcast.NewServer(nc, jetcast.ServerOptions{
		Config:        jetcast.Config{Ephemeral: []string{"typing.>"}},
		Account:       "CLIENT",
		CalloutSigner: issuer,
		Admin:         embedded.Admin(ns),
		ManageStreams: true,
	})
	if err != nil {
		log.Fatal(err)
	}
	rt.Authenticate(func(ctx context.Context, r jetcast.AuthRequest) (jetcast.User, error) {
		if jetcast.ValidateID(r.Token) != nil {
			return jetcast.User{}, errors.New("invalid token")
		}
		return jetcast.User{ID: r.Token, Info: map[string]string{"name": r.Token}}, nil
	})
	// Every user may subscribe directly to their own notifications.
	rt.Grants(func(ctx context.Context, u jetcast.User) ([]string, error) {
		return []string{"users." + u.ID + ".>"}, nil
	})
	// Anyone may watch rooms whose name starts with "open".
	if err := rt.Channel("rooms.{room}", func(ctx context.Context, u jetcast.User, p jetcast.Params) (bool, error) {
		return len(p["room"]) >= 4 && p["room"][:4] == "open", nil
	}); err != nil {
		log.Fatal(err)
	}
	if err := rt.Start(context.Background()); err != nil {
		log.Fatal(err)
	}
	defer rt.Close()

	mux := http.NewServeMux()
	// Browsers reach NATS through the application's own port.
	wsTarget, _ := url.Parse("http://127.0.0.1:8222")
	mux.Handle("/nats", httputil.NewSingleHostReverseProxy(wsTarget))
	// POST /rooms/{room}/messages broadcasts to the room, except to the
	// sender's own connection.
	mux.HandleFunc("POST /rooms/{room}/messages", func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Text string `json:"text"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		_, err := rt.Broadcast(r.Context(), jetcast.Event{
			Name:     "message.created",
			Channels: []jetcast.Channel{jetcast.Private("rooms." + r.PathValue("room"))},
			Data:     body,
			Origin:   jetcast.SocketID(r),
		})
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	})
	log.Println("listening on http://localhost:8080 (WebSocket at ws://localhost:8080/nats)")
	log.Fatal(http.ListenAndServe("localhost:8080", mux))
}

// loadIssuer reads or creates the account key that signs connection JWTs.
func loadIssuer(path string) (nkeys.KeyPair, error) {
	if seed, err := os.ReadFile(path); err == nil {
		return nkeys.FromSeed(seed)
	}
	kp, err := nkeys.CreateAccount()
	if err != nil {
		return nil, err
	}
	seed, _ := kp.Seed()
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return nil, err
	}
	return kp, os.WriteFile(path, seed, 0o600)
}
