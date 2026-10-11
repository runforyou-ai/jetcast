package jetcast_test

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nkeys"
	"github.com/runforyou-ai/jetcast"
)

func Example() {
	nc, err := nats.Connect("nats://localhost:4222", nats.UserInfo("app", "app"))
	if err != nil {
		log.Fatal(err)
	}
	issuer, _ := nkeys.FromSeed([]byte("SA...")) // the auth_callout issuer's account seed

	srv, err := jetcast.NewServer(nc, jetcast.ServerOptions{
		Account:       "CLIENT", // the account the callout places clients in
		CalloutSigner: issuer,
		ManageStreams: true,
	})
	if err != nil {
		log.Fatal(err)
	}
	srv.Authenticate(func(ctx context.Context, r jetcast.AuthRequest) (jetcast.User, error) {
		if r.Token != "secret-for-u42" {
			return jetcast.User{}, errors.New("unknown token")
		}
		return jetcast.User{ID: "u42"}, nil
	})
	srv.Grants(func(ctx context.Context, u jetcast.User) ([]string, error) {
		return []string{"users." + u.ID + ".>"}, nil
	})
	_ = srv.Channel("orders.{id}", func(ctx context.Context, u jetcast.User, p jetcast.Params) (bool, error) {
		return p["id"] == "42", nil
	})
	if err := srv.Start(context.Background()); err != nil {
		log.Fatal(err)
	}
	defer srv.Close()

	http.HandleFunc("POST /orders/{id}/ship", func(w http.ResponseWriter, r *http.Request) {
		_, err := srv.Broadcast(r.Context(), jetcast.Event{
			Name:     "order.shipped",
			Channels: []jetcast.Channel{jetcast.Private("orders." + r.PathValue("id"))},
			Data:     map[string]string{"id": r.PathValue("id")},
			Origin:   jetcast.SocketID(r),
		})
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
	})
}

func ExampleMatchPattern() {
	fmt.Println(jetcast.MatchPattern("users.42.>", "users.42.notifications"))
	fmt.Println(jetcast.MatchPattern("teams.*.board", "teams.7.board"))
	fmt.Println(jetcast.MatchPattern("users.42.>", "users.43.notifications"))
	// Output:
	// true
	// true
	// false
}
