// Package metrics exposes health checks and debug endpoints via an HTTP server.
// No external dependencies: expvar + pprof from stdlib, Prometheus scrapeable
// via /debug/vars.
package metrics

import (
	"expvar"
	"net/http"
	"net/http/pprof"
	"sync/atomic"
)

var (
	ConnectedFollowers atomic.Int64
	WALSegments        atomic.Int64
	WALSeq             atomic.Int64
	BackendHealthy     atomic.Int64
)

func init() {
	expvar.Publish("followers", expvar.Func(func() any { return ConnectedFollowers.Load() }))
	expvar.Publish("wal_segments", expvar.Func(func() any { return WALSegments.Load() }))
	expvar.Publish("wal_seq", expvar.Func(func() any { return WALSeq.Load() }))
	expvar.Publish("backend_healthy", expvar.Func(func() any { return BackendHealthy.Load() }))
}

// Serve starts an HTTP server on addr that serves /healthz, /readyz,
// /debug/vars (expvar), and /debug/pprof.
func Serve(addr string) error {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", healthz)
	mux.HandleFunc("/readyz", readyz)
	mux.HandleFunc("/debug/vars", expvarHandler)
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
	return http.ListenAndServe(addr, mux)
}

func healthz(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("ok\n"))
}

func readyz(w http.ResponseWriter, _ *http.Request) {
	if BackendHealthy.Load() == 0 {
		w.WriteHeader(http.StatusServiceUnavailable)
		w.Write([]byte("backend not reachable\n"))
		return
	}
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("ready\n"))
}

func expvarHandler(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	expvar.Handler().ServeHTTP(w, nil)
}
