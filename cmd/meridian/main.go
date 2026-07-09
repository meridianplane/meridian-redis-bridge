package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"time"
	"syscall"

	"google.golang.org/grpc"

	"github.com/meridianplane/meridian-redis-bridge/internal/auth"
	"github.com/meridianplane/meridian-redis-bridge/internal/config"
	"github.com/meridianplane/meridian-redis-bridge/internal/metrics"
	"github.com/meridianplane/meridian-redis-bridge/internal/proxy"
	"github.com/meridianplane/meridian-redis-bridge/internal/server"
	"github.com/meridianplane/meridian-redis-bridge/internal/sync"
	"github.com/meridianplane/meridian-redis-bridge/internal/wal"
	pb "github.com/meridianplane/meridian-redis-bridge/proto"
)

var version = "dev"

func main() {
	configPath := flag.String("config", "", "path to config.json")
	showVersion := flag.Bool("version", false, "print version and exit")
	flag.Parse()

	if *showVersion {
		fmt.Println("meridian-redis-bridge", version)
		return
	}
	if *configPath == "" {
		if flag.NArg() > 0 {
			*configPath = flag.Arg(0)
		} else {
			fmt.Fprintf(os.Stderr, "usage: %s -config <config.json>\n", os.Args[0])
			os.Exit(2)
		}
	}

	cfg, err := config.Load(*configPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "config: %v\n", err)
		os.Exit(1)
	}
	log := slog.New(slog.NewTextHandler(os.Stderr, nil))
	if err := run(cfg, log); err != nil {
		log.Error("run failed", "err", err)
		os.Exit(1)
	}
}

func run(cfg *config.Config, log *slog.Logger) error {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	m := metrics.New(cfg.Cluster, cfg.Name)

	isPrimary := cfg.IsPrimary()
	role := "follower"
	if isPrimary { role = "primary" }
	if cfg.IsLB() { role = "lb" }
	if cfg.Relay { role = "relay" }
	log = log.With("role", role)
	m.Info.WithLabelValues(cfg.Cluster, cfg.Name, role).Set(1)
	upstream := cfg.UpstreamAddrs()

	// Metrics / health HTTP server.
	if cfg.MetricsListen != "" {
		go func() {
			if err := http.ListenAndServe(cfg.MetricsListen, metrics.HTTPHandler()); err != nil {
				log.Error("metrics server stopped", "err", err)
			}
		}()
	}

	// LB mode: pure proxy, no WAL, no backend, no RESP frontend.
	if cfg.IsLB() {
		if len(upstream) == 0 {
			return fmt.Errorf("lb mode requires upstream addresses")
		}
		grpcLn, err := net.Listen("tcp", cfg.GRPCListen)
		if err != nil { return err }
		gs := grpc.NewServer()
		syncSrv := sync.NewServer(nil, nil, 0, m)
		syncSrv.SetUpstream(upstream)
		pb.RegisterReplicationServer(gs, syncSrv)
		go func() { <-ctx.Done(); gs.GracefulStop() }()
		log.Info("lb started", "grpc", cfg.GRPCListen)
		return gs.Serve(grpcLn)
	}

	// Local backend — optional (relay-only nodes skip it).
	var be *proxy.Backend
	if cfg.Backend.Addr != "" || len(cfg.Backend.Addrs) > 0 {
		be = proxy.NewBackend(proxy.BackendConfig{
			Addr:     cfg.Backend.Addr,
			Addrs:    cfg.Backend.Addrs,
			Username: cfg.Backend.Username,
			Password: cfg.Backend.Password,
			DB:       cfg.Backend.DB,
			PoolSize: cfg.Backend.PoolSize,
		})
		defer be.Close()
		if err := be.Ping(ctx); err != nil {
			return err
		}
		m.BackendHealthy.Set(1)
	}

	// Write-ahead log.
	w, err := wal.Open(wal.Options{
		Dir:           cfg.WALDir(),
		Flush:         walFlushMode(cfg.WALFlush),
		FlushInterval: cfg.WALFlushInterval,
	})
	if err != nil {
		return err
	}

	// Write forwarder. Nil on the primary.
	var fwd proxy.Forwarder
	if !isPrimary && len(upstream) > 0 {
		fwd = proxy.NewGRPCForwarder(upstream[0], m)
		defer fwd.(*proxy.GRPCForwarder).Close()
	}

	// Island forwarders: key prefix → gRPC forwarder to another primary.
	routes := make([]proxy.RouteEntry, 0, len(cfg.Routes))
	for _, r := range cfg.Routes {
		addrs, ok := cfg.Primaries[r.Target]
		if !ok || len(addrs) == 0 {
			return fmt.Errorf("route %q references unknown primary %q", r.Prefix, r.Target)
		}
		routes = append(routes, proxy.RouteEntry{
			Prefix: r.Prefix,
			Fwd:    proxy.NewGRPCForwarder(addrs[0], m),
		})
	}

	// Dispatcher.
	d := proxy.NewDispatcher(be, proxy.NewRouter(), w, isPrimary, fwd, cfg.ForwardWritesEnabled(), cfg.Relay)
	d.Routes = routes
	d.Metrics = m
	if cfg.AuthEnabled() {
		a, err := auth.NewFromFile(cfg.Auth.PasswdFile)
		if err != nil {
			return fmt.Errorf("auth: %w", err)
		}
		d.Auth = a
	}

	// Periodic WAL metrics.
	go func() {
		t := time.NewTicker(15 * time.Second)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				m.WALSegmentCount.Set(float64(w.SegmentCount()))
				m.WALCurrentSeq.Set(float64(w.NextSeq()))
			}
		}
	}()

	// gRPC replication server.
	grpcLn, err := net.Listen("tcp", cfg.GRPCListen)
	if err != nil { return err }
	gs := grpc.NewServer()
	syncSrv := sync.NewServer(w, d, 0, m)
	syncSrv.SetUpstream(upstream)
	pb.RegisterReplicationServer(gs, syncSrv)
	go func() { <-ctx.Done(); gs.GracefulStop() }()
	go func() {
		if serveErr := gs.Serve(grpcLn); serveErr != nil {
			log.Error("gRPC serve stopped", "err", serveErr)
		}
	}()

	// RESP front-end.
	ln, err := net.Listen("tcp", cfg.Listen)
	if err != nil { return err }
	srv := server.New(ln, d, log)
	srv.Metrics = m

	// Follower: subscribe to upstream WAL.
	if !isPrimary && len(upstream) > 0 {
		wmPath := filepath.Join(cfg.StateDir(), "watermark")
		wm, err := sync.OpenWatermark(wmPath)
		if err != nil {
			return fmt.Errorf("watermark open %s: %w", wmPath, err)
		}
		flw := sync.NewFollower(sync.FollowerConfig{
			OwnerAddrs:     upstream,
			FollowerRegion: cfg.GRPCListen,
			FollowerID:     cfg.GRPCListen,
			Applier:        d,
			Watermark:      wm,
			Logger:         log,
		})
		go func() {
			if err := flw.Run(ctx); err != nil && ctx.Err() == nil {
				log.Error("follower stream exited", "err", err)
			}
		}()
	}

	log.Info("meridian started", "listen", cfg.Listen, "grpc", cfg.GRPCListen)
	return srv.Serve(ctx)
}
func walFlushMode(s string) wal.FlushMode {
	switch s {
	case "sync":
		return wal.FlushSync
	case "periodic":
		return wal.FlushPeriodic
	default:
		return wal.FlushNone
	}
}
