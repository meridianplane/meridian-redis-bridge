// Package proxy: front-end termination of connection and session commands.
//
// AUTH, HELLO, QUIT, RESET, PING, ECHO and SELECT are always answered by the
// front-end and never forwarded to the backend. This keeps client credentials
// at the front-end — they are checked against the configured Authenticator and
// never travel to the backend ACL nor across a region boundary — and it stops
// connection-scoped commands (SELECT, CLIENT, …) from mutating or reading the
// state of a backend connection that is shared across clients via the pool.
//
// When no Authenticator is configured the front-end requires no credential: it
// still answers AUTH/HELLO, but AUTH reports that no password is set, exactly
// as a Redis with no requirepass would.
package proxy

import (
	"strings"

	"github.com/meridianplane/meridian-redis-bridge/internal/auth"
	"github.com/meridianplane/meridian-redis-bridge/internal/resp"
)

// Session is the per-connection state the dispatcher keeps. The server creates
// one per client connection and threads it through every Dispatch call on that
// connection. A connection is served by a single goroutine, so a Session is
// never accessed concurrently.
type Session struct {
	authed bool
	user   string
}

// authRequired reports whether the connection must authenticate before running
// data commands.
func (d *Dispatcher) authRequired() bool {
	return d.Auth != nil && d.Auth.Required()
}

// handleAuth terminates the AUTH command at the front-end. Forms:
//
//	AUTH <password>             -> the default user
//	AUTH <username> <password>
//
// The credentials are checked against the authenticator and never forwarded to
// the backend. Replies mirror Redis: +OK on success, -WRONGPASS on a bad pair.
func (d *Dispatcher) handleAuth(c *resp.Command, w *resp.Writer, sess *Session) error {
	var user, pass string
	switch len(c.Args) {
	case 2:
		user, pass = auth.DefaultUser, c.Arg(1)
	case 3:
		user, pass = c.Arg(1), c.Arg(2)
	default:
		return w.WriteError("ERR wrong number of arguments for 'auth' command")
	}
	if !d.authRequired() {
		return w.WriteError("ERR Client sent AUTH, but no password is set. Did you mean AUTH <username> <password>?")
	}
	if err := d.Auth.Verify(user, pass); err != nil {
		return w.WriteError(err.Error())
	}
	sess.authed = true
	sess.user = user
	return w.WriteSimpleString("OK")
}

// handleHello terminates HELLO at the front-end so the optional AUTH section's
// credentials never reach the backend.
func (d *Dispatcher) handleHello(c *resp.Command, w *resp.Writer, sess *Session) error {
	var user, pass string
	haveAuth := false
	i := 1
	if i < len(c.Args) {
		switch c.Arg(i) {
		case "2":
		case "3":
			return w.WriteError("NOPROTO unsupported protocol version")
		default:
			return w.WriteError("NOPROTO unsupported protocol version")
		}
		i++
		for i < len(c.Args) {
			switch strings.ToUpper(c.Arg(i)) {
			case "AUTH":
				if i+2 >= len(c.Args) {
					return w.WriteError("ERR syntax error in HELLO")
				}
				user, pass = c.Arg(i+1), c.Arg(i+2)
				haveAuth = true
				i += 3
			case "SETNAME":
				if i+1 >= len(c.Args) {
					return w.WriteError("ERR syntax error in HELLO")
				}
				i += 2
			default:
				return w.WriteError("ERR syntax error in HELLO")
			}
		}
	}

	if haveAuth {
		if !d.authRequired() {
			return w.WriteError("ERR Client sent AUTH, but no password is set. Did you mean AUTH <username> <password>?")
		}
		if err := d.Auth.Verify(user, pass); err != nil {
			return w.WriteError(err.Error())
		}
		sess.authed = true
		sess.user = user
	}
	if d.authRequired() && !sess.authed {
		return w.WriteError("NOAUTH HELLO must be called with the client already authenticated, otherwise the HELLO <proto> AUTH <user> <pass> option can be used to authenticate the client and select the RESP protocol version at the same time")
	}
	return writeHelloReply(w)
}

func writeHelloReply(w *resp.Writer) error {
	bulk := func(s string) error { return w.WriteBulkString([]byte(s)) }
	if err := w.WriteArrayHeader(14); err != nil {
		return err
	}
	for _, kv := range []struct{ k, v string }{
		{"server", "redis"},
		{"version", "7.4.0"},
	} {
		if err := bulk(kv.k); err != nil {
			return err
		}
		if err := bulk(kv.v); err != nil {
			return err
		}
	}
	if err := bulk("proto"); err != nil {
		return err
	}
	if err := w.WriteInt(2); err != nil {
		return err
	}
	if err := bulk("id"); err != nil {
		return err
	}
	if err := w.WriteInt(0); err != nil {
		return err
	}
	for _, kv := range []struct{ k, v string }{
		{"mode", "standalone"},
		{"role", "master"},
	} {
		if err := bulk(kv.k); err != nil {
			return err
		}
		if err := bulk(kv.v); err != nil {
			return err
		}
	}
	if err := bulk("modules"); err != nil {
		return err
	}
	return w.WriteArrayHeader(0)
}

// handleQuit answers QUIT at the front-end.
func (d *Dispatcher) handleQuit(w *resp.Writer) error {
	if err := w.WriteSimpleString("OK"); err != nil {
		return err
	}
	return ErrClientQuit
}

// handleReset clears the connection's authentication state and replies +RESET.
func (d *Dispatcher) handleReset(w *resp.Writer, sess *Session) error {
	sess.authed = false
	sess.user = ""
	return w.WriteSimpleString("RESET")
}

// handlePing answers PING locally.
func (d *Dispatcher) handlePing(c *resp.Command, w *resp.Writer) error {
	switch len(c.Args) {
	case 1:
		return w.WriteSimpleString("PONG")
	case 2:
		return w.WriteBulkString(c.ArgBytes(1))
	default:
		return w.WriteError("ERR wrong number of arguments for 'ping' command")
	}
}

// handleEcho echoes its single argument back as a bulk string.
func (d *Dispatcher) handleEcho(c *resp.Command, w *resp.Writer) error {
	if len(c.Args) != 2 {
		return w.WriteError("ERR wrong number of arguments for 'echo' command")
	}
	return w.WriteBulkString(c.ArgBytes(1))
}

// handleSelect answers SELECT locally. meridian presents a single logical
// database, so only index 0 is accepted.
func (d *Dispatcher) handleSelect(c *resp.Command, w *resp.Writer) error {
	if len(c.Args) != 2 {
		return w.WriteError("ERR wrong number of arguments for 'select' command")
	}
	if c.Arg(1) != "0" {
		return w.WriteError("ERR DB index is out of range (singleowner presents a single logical database)")
	}
	return w.WriteSimpleString("OK")
}
