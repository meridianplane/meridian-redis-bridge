// Package server is the RESP front-end: it accepts client TCP connections,
// reads one command at a time, and hands each to the dispatcher, which decides
// whether to execute it locally, forward it to a peer owner, or reject it.
//
// One goroutine per connection. RESP is a synchronous request/response protocol
// (outside of pub/sub, which meridian does not model), so a connection reads a
// command, writes its reply, flushes, and loops. Per-connection state is just
// the buffered reader/writer; nothing is shared between connections except the
// dispatcher, which is safe for concurrent use.
package server

import (
	"context"
	"errors"
	"io"
	"net"
	"sync"

	
	"github.com/meridianplane/meridian-redis-bridge/internal/proxy"
	"github.com/meridianplane/meridian-redis-bridge/internal/metrics"
	"github.com/meridianplane/meridian-redis-bridge/internal/resp"
)

// Logger is the minimal logging surface the server needs; *slog.Logger fits.
type Logger interface {
	Info(msg string, args ...any)
	Warn(msg string, args ...any)
	Error(msg string, args ...any)
}

// Server accepts RESP connections and dispatches their commands.
type Server struct {
	Metrics *metrics.Metrics
	ln  net.Listener
	d   *proxy.Dispatcher
	log Logger

	wg sync.WaitGroup
}

// New builds a Server bound to ln.
func New(ln net.Listener, d *proxy.Dispatcher, log Logger) *Server {
	return &Server{ln: ln, d: d, log: log}
}

// Serve runs the accept loop until ctx is cancelled or the listener fails. On
// ctx cancellation it stops accepting, waits for in-flight connections to drain,
// and returns nil.
func (s *Server) Serve(ctx context.Context) error {
	// Closing the listener unblocks Accept; do it when ctx is cancelled.
	go func() {
		<-ctx.Done()
		_ = s.ln.Close()
	}()

	for {
		conn, err := s.ln.Accept()
		if err != nil {
			if ctx.Err() != nil {
				// Cancellation: drain and exit cleanly.
				s.wg.Wait()
				return nil
			}
			return err
		}
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			s.handle(ctx, conn)
		}()
	}
}

// handle serves one client connection until it closes or errors.
func (s *Server) handle(ctx context.Context, conn net.Conn) {
	if s.Metrics != nil { s.Metrics.RecordRespOpen() }
	defer func() { if s.Metrics != nil { s.Metrics.RecordRespClose() } }()
	defer conn.Close()
	// Stop a blocked read when the server is shutting down.
	go func() {
		<-ctx.Done()
		_ = conn.Close()
	}()

	rd := resp.NewReader(conn)
	wr := resp.NewWriter(conn)
	// Per-connection state (e.g. authentication) lives here; the dispatcher
	// itself is shared and stateless across connections.
	sess := &proxy.Session{}
	for {
		c, err := rd.ReadCommand()
		if err != nil {
			// A clean client disconnect (EOF) or a shutdown-induced close is
			// not worth logging; a protocol error is.
			if !errors.Is(err, io.EOF) && ctx.Err() == nil && !isClosedConn(err) {
				s.log.Warn("read command failed", "remote", conn.RemoteAddr().String(), "err", err)
			}
			return
		}
		if len(c.Args) == 0 {
			continue
		}
		if err := s.d.Dispatch(ctx, c, wr, sess); err != nil {
			// A client QUIT is answered by the dispatcher and then asks us to
			// close: flush the +OK and return without logging.
			if errors.Is(err, proxy.ErrClientQuit) {
				_ = wr.Flush()
				return
			}
			if ctx.Err() == nil && !isClosedConn(err) {
				s.log.Warn("dispatch failed", "remote", conn.RemoteAddr().String(), "err", err)
			}
			return
		}
		if err := wr.Flush(); err != nil {
			return
		}
	}
}

// isClosedConn reports whether err is the "use of closed network connection"
// that Accept/Read return after we deliberately close on shutdown.
func isClosedConn(err error) bool {
	return errors.Is(err, net.ErrClosed)
}
