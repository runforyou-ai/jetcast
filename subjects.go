package jetcast

// subjects builds every subject jetcast uses from the prefix.
type subjects struct{ p string }

// in is the stream ingress subject of a channel.
func (s subjects) in(c Channel) string { return s.p + ".in." + c.String() }

// inAll matches every ingress subject.
func (s subjects) inAll() string { return s.p + ".in.>" }

// ev is the event subject of a channel.
func (s subjects) ev(c Channel) string { return s.p + ".ev." + c.String() }

// evAll matches every event subject.
func (s subjects) evAll() string { return s.p + ".ev.>" }

// evPattern is the event subject of a channel pattern.
func (s subjects) evPattern(kind Kind, pattern string) string {
	return s.p + ".ev." + string(kind) + "." + pattern
}

// conn is the namespace of a connection.
func (s subjects) conn(socket string) string { return s.p + ".c." + socket }

// connEvents receives relayed events.
func (s subjects) connEvents(socket string) string { return s.conn(socket) + ".ev" }

// connControl receives control messages.
func (s subjects) connControl(socket string) string { return s.conn(socket) + ".ctl" }

// connReplies is the prefix of reply subjects of a connection.
func (s subjects) connReplies(socket string) string { return s.conn(socket) + ".r" }

// requests matches the requests any node handles: <p>.rq.<socket>.<op>.
func (s subjects) requests() string { return s.p + ".rq.*.*" }

// nodeRequests matches requests to one node: <p>.rq.<socket>.n.<node>.<op>.
func (s subjects) nodeRequests(node string) string { return s.p + ".rq.*.n." + node + ".*" }

// clientRequests is the publish permission of a connection.
func (s subjects) clientRequests(socket string) string { return s.p + ".rq." + socket + ".>" }

// sys is a node-to-node control subject.
func (s subjects) sys(op string) string { return s.p + ".sys." + op }

// sysAll matches every node-to-node control subject.
func (s subjects) sysAll() string { return s.p + ".sys.>" }
