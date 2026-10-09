package jetcast

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"runtime/debug"
	"time"

	"github.com/nats-io/jwt/v2"
	"github.com/nats-io/nats.go"
)

const (
	// calloutTimeout bounds handling one auth callout request. Configure the
	// server's authorization timeout above it.
	calloutTimeout = 4 * time.Second
	// revocationSkew tolerates clock differences between NATS servers when
	// comparing revocation marks with the callout request time.
	revocationSkew = 2 * time.Second
	// xkeyHeader carries the NATS server's curve key on encrypted callouts.
	xkeyHeader = "Nats-Server-Xkey"
)

// errRejected is the user-facing rejection; details are only logged.
var errRejected = errors.New("not authorized")

// calloutRequest is a queued auth callout request.
type calloutRequest struct {
	m  *nats.Msg
	at time.Time
}

// enqueueCallout queues an auth callout request for the callout workers. When
// the queue is full the request is dropped: the NATS server times it out and
// the client retries.
func (s *Server) enqueueCallout(m *nats.Msg) {
	select {
	case <-s.ctx.Done():
	case s.callouts <- calloutRequest{m: m, at: time.Now()}:
	default:
		s.calloutDropped.Add(1)
		s.log.Debug("jetcast: callout queue full, request dropped")
	}
}

// calloutWorker answers queued auth callout requests until Close.
func (s *Server) calloutWorker() {
	for {
		select {
		case <-s.ctx.Done():
			return
		case r := <-s.callouts:
			// The NATS server stopped waiting for requests queued too long.
			if time.Since(r.at) >= calloutTimeout {
				s.calloutDropped.Add(1)
				continue
			}
			s.handleCallout(r.m)
		}
	}
}

// handleCallout answers one auth callout request.
func (s *Server) handleCallout(m *nats.Msg) {
	defer func() {
		if r := recover(); r != nil {
			s.log.Error("jetcast: callout handler panicked", "panic", r, "stack", string(debug.Stack()))
		}
	}()
	start := time.Now()
	data := m.Data
	serverXKey := m.Header.Get(xkeyHeader)
	if serverXKey != "" {
		if s.opts.CalloutXKey == nil {
			s.log.Error("jetcast: encrypted callout request but no CalloutXKey configured")
			return
		}
		var err error
		if data, err = s.opts.CalloutXKey.Open(data, serverXKey); err != nil {
			s.log.Warn("jetcast: decrypt callout request", "error", err)
			return
		}
	}
	rc, err := jwt.DecodeAuthorizationRequestClaims(string(data))
	if err != nil {
		s.log.Warn("jetcast: decode callout request", "error", err)
		return
	}
	ctx, cancel := context.WithTimeout(s.ctx, calloutTimeout)
	defer cancel()
	userJWT, err := s.safeAuthorizeConnection(ctx, rc)
	if err != nil {
		s.calloutRejected.Add(1)
		s.log.Debug("jetcast: connection rejected", "socket", rc.ConnectOptions.Name, "host", rc.ClientInformation.Host, "error", err)
	} else {
		s.calloutAccepted.Add(1)
	}
	s.calloutNanos.Add(uint64(time.Since(start)))

	cr := jwt.NewAuthorizationResponseClaims(rc.UserNkey)
	cr.Audience = rc.Server.ID
	if err != nil {
		cr.Error = errRejected.Error()
	} else {
		cr.Jwt = userJWT
	}
	token, err := cr.Encode(s.opts.CalloutSigner)
	if err != nil {
		s.log.Error("jetcast: sign callout response", "error", err)
		return
	}
	payload := []byte(token)
	if serverXKey != "" {
		if payload, err = s.opts.CalloutXKey.Seal(payload, serverXKey); err != nil {
			s.log.Error("jetcast: encrypt callout response", "error", err)
			return
		}
	}
	if err := m.Respond(payload); err != nil {
		s.log.Warn("jetcast: respond to callout", "error", err)
	}
}

// connectionType maps the callout's client type to a JWT connection type.
func connectionType(clientType string) string {
	switch clientType {
	case "websocket":
		return jwt.ConnectionTypeWebsocket
	case "nats":
		return jwt.ConnectionTypeStandard
	}
	return clientType
}

// safeAuthorizeConnection runs authorizeConnection, rejecting the connection
// when an application callback panics.
func (s *Server) safeAuthorizeConnection(ctx context.Context, rc *jwt.AuthorizationRequestClaims) (token string, err error) {
	defer func() {
		if r := recover(); r != nil {
			s.log.Error("jetcast: connection authentication panicked", "panic", r, "stack", string(debug.Stack()))
			token, err = "", fmt.Errorf("authentication panicked: %v", r)
		}
	}()
	return s.authorizeConnection(ctx, rc)
}

// authorizeConnection authenticates a connection, registers its socket and
// returns the signed user JWT.
func (s *Server) authorizeConnection(ctx context.Context, rc *jwt.AuthorizationRequestClaims) (string, error) {
	socket := rc.ConnectOptions.Name
	if err := ValidateSocketID(socket); err != nil {
		return "", err
	}
	u, err := s.authenticate(ctx, AuthRequest{
		Token: rc.ConnectOptions.Token,
		Type:  rc.ClientInformation.Type,
		Host:  rc.ClientInformation.Host,
	})
	if err != nil {
		return "", fmt.Errorf("authenticate: %w", err)
	}
	if u.Session == "" {
		u.Session = u.ID
	}
	if err := ValidateID(u.ID); err != nil {
		return "", err
	}
	if err := ValidateID(u.Session); err != nil {
		return "", err
	}
	types := u.ConnectionTypes
	if len(types) == 0 {
		types = []string{ConnectionWebSocket}
	}
	allowed := false
	for _, t := range types {
		if t == connectionType(rc.ClientInformation.Type) {
			allowed = true
		}
	}
	if !allowed {
		return "", fmt.Errorf("connection type %q not allowed", rc.ClientInformation.Type)
	}

	now := time.Now()
	exp := now.Add(s.opts.MaxConnectionTTL)
	if !u.ExpiresAt.IsZero() && u.ExpiresAt.Before(exp) {
		exp = u.ExpiresAt
	}
	exp = exp.Truncate(time.Second)
	if !exp.After(now.Add(time.Second)) {
		return "", errors.New("credentials expired")
	}

	var grants []string
	if s.grants != nil {
		if grants, err = s.grants(ctx, u); err != nil {
			return "", fmt.Errorf("grants: %w", err)
		}
		for _, p := range grants {
			if err := ValidatePattern(p); err != nil {
				return "", err
			}
		}
	}
	var info json.RawMessage
	if u.Info != nil {
		if info, err = json.Marshal(u.Info); err != nil {
			return "", fmt.Errorf("encode user info: %w", err)
		}
	}

	rec := &connRecord{
		User: u.ID, Session: u.Session, Info: info, Grants: grants,
		ExpiresAt: exp.UnixMilli(), Server: rc.Server.ID, CID: rc.ClientInformation.ID,
	}
	if err := s.reg.create(ctx, socket, rec); err != nil {
		return "", err
	}
	// Disconnect writes the revocation mark before revoking records; a mark
	// written since this request started means a revocation raced it.
	since := time.Unix(rc.IssuedAt, 0).Add(-revocationSkew)
	revoked, err := s.reg.revokedSince(ctx, u.ID, u.Session, since)
	if err == nil && revoked {
		err = errors.New("revoked during authentication")
	}
	if err != nil {
		if _, _, rerr := s.reg.revoke(ctx, socket); rerr != nil {
			s.log.Warn("jetcast: revoke rejected socket", "error", rerr)
		}
		return "", err
	}

	uc := jwt.NewUserClaims(rc.UserNkey)
	uc.Name = u.ID + "|" + socket
	uc.Audience = s.opts.Account
	uc.Expires = exp.Unix()
	uc.AllowedConnectionTypes.Add(types...)
	for _, sub := range s.userSubscribe(socket, grants) {
		uc.Sub.Allow.Add(sub)
	}
	uc.Sub.Deny.Add("> >")
	uc.Pub.Allow.Add(s.sub.clientRequests(socket))
	return uc.Encode(s.opts.CalloutSigner)
}

// userSubscribe returns the subscribe permissions of a client connection.
func (s *Server) userSubscribe(socket string, grants []string) []string {
	out := []string{s.sub.evPattern(KindPublic, ">"), s.sub.conn(socket) + ".>"}
	for _, p := range grants {
		out = append(out, s.sub.evPattern(KindPrivate, p))
	}
	return out
}
