package e2e

import (
	"context"
	"fmt"
	"net"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	
	
	"github.com/meridianplane/meridian-redis-bridge/internal/proxy"
	"github.com/meridianplane/meridian-redis-bridge/internal/resp"
	"github.com/meridianplane/meridian-redis-bridge/internal/server"
	rsync "github.com/meridianplane/meridian-redis-bridge/internal/sync"
	"github.com/meridianplane/meridian-redis-bridge/internal/wal"
	pb "github.com/meridianplane/meridian-redis-bridge/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// cluster is a primary + one follower, each with its own miniredis backend,
// wired for WAL replication over a real loopback gRPC connection.
type cluster struct {
	primary *node
	follower *node
	wal     *wal.WAL   // primary WAL
	gRPCAddr string    // primary gRPC listen address
	cleanup func()
}

type node struct {
	be *proxy.Backend
	d  *proxy.Dispatcher
	mr *miniredis.Miniredis
}

// newCluster wires a primary and follower with independent miniredis backends.
func newCluster(t *testing.T, followerForward bool) *cluster {
	t.Helper()

	// Primary miniredis + 
	pmr := miniredis.RunT(t)
	pbe := proxy.NewBackend(proxy.BackendConfig{Addr: pmr.Addr()})

	// Primary WAL + gRPC replication server.
	pwal, err := wal.Open(wal.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("primary wal open: %v", err)
	}
	grpcLn, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("gRPC listen: %v", err)
	}
	gsrv := grpc.NewServer()

	// Primary dispatcher (no forwarding — it is the owner).
	pd := proxy.NewDispatcher(pbe, proxy.NewRouter(), pwal, true, nil, false, false)

	pb.RegisterReplicationServer(gsrv, rsync.NewServer(pwal, pd, 0, nil))
	go func() { _ = gsrv.Serve(grpcLn) }()

	// Primary RESP frontend.
	pln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("primary listen: %v", err)
	}
	psrv := server.New(pln, pd, discardLogger())
	ctx, cancel := context.WithCancel(context.Background())
	go func() { _ = psrv.Serve(ctx) }()

	// Follower miniredis + 
	fmr := miniredis.RunT(t)
	fbe := proxy.NewBackend(proxy.BackendConfig{Addr: fmr.Addr()})

	// Follower WAL.
	fwal, err := wal.Open(wal.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("follower wal open: %v", err)
	}

	// Follower forwarder to primary over gRPC.
	fwd := proxy.NewGRPCForwarder(grpcLn.Addr().String(), nil)

	// Follower dispatcher.
	fd := proxy.NewDispatcher(fbe, proxy.NewRouter(), fwal, false, fwd, followerForward, false)

	// Follower watermark + gRPC follower.
	wm, err := rsync.OpenWatermark(t.TempDir() + "/wm")
	if err != nil {
		t.Fatalf("watermark: %v", err)
	}
	flw := rsync.NewFollower(rsync.FollowerConfig{
		OwnerAddrs:     []string{grpcLn.Addr().String()},
		FollowerRegion: "eu",
		FollowerID:     "eu",
		Applier:        fd,
		Watermark:      wm,
	})
	go func() { _ = flw.Run(ctx) }()

	// Follower RESP frontend.
	fln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("follower listen: %v", err)
	}
	fsrv := server.New(fln, fd, discardLogger())
	go func() { _ = fsrv.Serve(ctx) }()

	c := &cluster{
		primary:  &node{be: pbe, d: pd, mr: pmr},
		follower: &node{be: fbe, d: fd, mr: fmr},
		wal:      pwal,
		gRPCAddr: grpcLn.Addr().String(),
		cleanup: func() {
			cancel()
			gsrv.Stop()
			_ = pwal.Close()
			_ = fwal.Close()
			_ = pbe.Close()
			_ = fbe.Close()
		},
	}
	t.Cleanup(c.cleanup)
	return c
}

func (c *cluster) doPrimary(args ...any) (string, error) {
	// Convert to [][]byte for WAL, then execute on  This mirrors
	// dispatchWrite's WAL → Do path exactly.
	walArgs := make([][]byte, len(args))
	for i, a := range args {
		walArgs[i] = []byte(fmt.Sprint(a))
	}
	if _, err := c.wal.Append(&pb.WalEntry{
		Args: walArgs,
	}); err != nil {
		return "", err
	}
	v, e := c.primary.be.Do(context.Background(), args...)
	if e != nil {
		return "", e
	}
	return fmt.Sprint(v), nil
}

func (c *cluster) doFollower(args ...any) (string, error) {
	v, e := c.follower.be.Do(context.Background(), args...)
	if e != nil {
		return "", e
	}
	return fmt.Sprint(v), nil
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

// ── String ──

func TestStringReplication(t *testing.T) {
	c := newCluster(t, false)
	c.doPrimary("SET", "k1", "hello")
	waitFor(t, "SET replicated", func() bool {
		v, _ := c.doFollower("GET", "k1")
		return v == "hello"
	})

	c.doPrimary("EXPIRE", "k1", "10")
	waitFor(t, "EXPIRE replicated", func() bool {
		v, _ := c.doFollower("TTL", "k1")
		return v != "-1" && v != "-2"
	})

	c.doPrimary("DEL", "k1")
	waitFor(t, "DEL replicated", func() bool {
		v, _ := c.doFollower("EXISTS", "k1")
		return v == "0"
	})
}

// ── Hash ──

func TestHashReplication(t *testing.T) {
	c := newCluster(t, false)
	c.doPrimary("HSET", "h1", "f1", "v1", "f2", "v2")
	waitFor(t, "HSET replicated", func() bool {
		v, _ := c.doFollower("HGET", "h1", "f1")
		return v == "v1"
	})

	c.doPrimary("HDEL", "h1", "f2")
	waitFor(t, "HDEL replicated", func() bool {
		v, _ := c.doFollower("HEXISTS", "h1", "f2")
		return v == "0"
	})
}

// ── List ──

func TestListReplication(t *testing.T) {
	c := newCluster(t, false)
	c.doPrimary("LPUSH", "l1", "c", "b", "a")
	waitFor(t, "LPUSH replicated", func() bool {
		v, _ := c.doFollower("LLEN", "l1")
		return v == "3"
	})

	c.doPrimary("RPOP", "l1")
	waitFor(t, "RPOP replicated", func() bool {
		v, _ := c.doFollower("LLEN", "l1")
		return v == "2"
	})

	// Verify value order.
	v, _ := c.doFollower("LRANGE", "l1", "0", "-1")
	if v != "[a b]" {
		t.Fatalf("LRANGE = %q, want [a b]", v)
	}
}

// ── Set ──

func TestSetReplication(t *testing.T) {
	c := newCluster(t, false)
	c.doPrimary("SADD", "s1", "x", "y", "z")
	waitFor(t, "SADD replicated", func() bool {
		v, _ := c.doFollower("SCARD", "s1")
		return v == "3"
	})

	c.doPrimary("SREM", "s1", "y")
	waitFor(t, "SREM replicated", func() bool {
		v, _ := c.doFollower("SISMEMBER", "s1", "y")
		return v == "0"
	})
	// Verify remaining members.
	members, _ := c.doFollower("SMEMBERS", "s1")
	if members != "[x z]" && members != "[z x]" {
		t.Fatalf("SMEMBERS = %q, want [x z]", members)
	}
}

// ── Sorted Set ──

func TestSortedSetReplication(t *testing.T) {
	c := newCluster(t, false)
	c.doPrimary("ZADD", "z1", "1", "a", "2", "b", "3", "c")
	waitFor(t, "ZADD replicated", func() bool {
		v, _ := c.doFollower("ZCARD", "z1")
		return v == "3"
	})

	c.doPrimary("ZREM", "z1", "b")
	waitFor(t, "ZREM replicated", func() bool {
		v, _ := c.doFollower("ZCARD", "z1")
		return v == "2"
	})
	// Verify scores and members.
	score, _ := c.doFollower("ZSCORE", "z1", "a")
	if score != "1" {
		t.Fatalf("ZSCORE a = %q, want 1", score)
	}
	score, _ = c.doFollower("ZSCORE", "z1", "c")
	if score != "3" {
		t.Fatalf("ZSCORE c = %q, want 3", score)
	}
	// b should be gone.
	exists, _ := c.doFollower("ZSCORE", "z1", "b")
	if exists != "" {
		t.Fatalf("ZSCORE b = %q, want empty (removed)", exists)
	}
}

// ── Conditional / read-modify-write ──

func TestConditionalCommands(t *testing.T) {
	c := newCluster(t, false)

	c.doPrimary("INCR", "ctr")
	waitFor(t, "INCR", func() bool { v, _ := c.doFollower("GET", "ctr"); return v == "1" })

	c.doPrimary("INCRBY", "ctr", "5")
	waitFor(t, "INCRBY", func() bool { v, _ := c.doFollower("GET", "ctr"); return v == "6" })

	c.doPrimary("HSET", "hc", "f", "10")
	waitFor(t, "HINCRBY base", func() bool { v, _ := c.doFollower("HGET", "hc", "f"); return v == "10" })
	c.doPrimary("HINCRBY", "hc", "f", "5")
	waitFor(t, "HINCRBY", func() bool { v, _ := c.doFollower("HGET", "hc", "f"); return v == "15" })

	c.doPrimary("ZADD", "zi", "1", "a")
	c.doPrimary("ZINCRBY", "zi", "4", "a")
	waitFor(t, "ZINCRBY", func() bool {
		v, _ := c.doFollower("ZSCORE", "zi", "a")
		return v == "5"
	})

	c.doPrimary("SET", "ap", "hel")
	c.doPrimary("APPEND", "ap", "lo")
	waitFor(t, "APPEND", func() bool { v, _ := c.doFollower("GET", "ap"); return v == "hello" })

	c.doPrimary("SET", "sn", "v1")
	c.doPrimary("SETNX", "sn", "v2")
	waitFor(t, "SETNX no-op", func() bool { v, _ := c.doFollower("GET", "sn"); return v == "v1" })

	c.doPrimary("SET", "pe", "x", "EX", "100")
	c.doPrimary("PERSIST", "pe")
	waitFor(t, "PERSIST", func() bool {
		v, _ := c.doFollower("TTL", "pe")
		return v == "-1"
	})
}

// ── Baseline: many keys ──

func TestManyKeys(t *testing.T) {
	c := newCluster(t, false)
	const n = 50
	for i := 0; i < n; i++ {
		c.doPrimary("SET", fmt.Sprintf("k%d", i), fmt.Sprintf("v%d", i))
	}
	waitFor(t, "all keys replicated", func() bool {
		for i := 0; i < n; i++ {
			v, _ := c.doFollower("GET", fmt.Sprintf("k%d", i))
			if v != fmt.Sprintf("v%d", i) {
				return false
			}
		}
		return true
	})
}

// ── gRPC stream ──

func TestGRPCReplicationStream(t *testing.T) {
	walDir := t.TempDir()
	w, err := wal.Open(wal.Options{Dir: walDir})
	if err != nil {
		t.Fatalf("wal.Open: %v", err)
	}
	defer w.Close()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	gs := grpc.NewServer()
	pb.RegisterReplicationServer(gs, rsync.NewServer(w, nil, 0, nil))
	go func() { _ = gs.Serve(ln) }()
	defer gs.Stop()

	for i := 0; i < 3; i++ {
		w.Append(&pb.WalEntry{
			Args: [][]byte{[]byte("SET"), []byte("k"), []byte("v")},
		})
	}

	conn, err := grpc.NewClient(ln.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("gRPC dial: %v", err)
	}
	defer conn.Close()
	cli := pb.NewReplicationClient(conn)
	stream, err := cli.Subscribe(context.Background(), &pb.SubscribeRequest{FromSeq: 1})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	var mu sync.Mutex
	var seqs []uint64
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		for {
			batch, err := stream.Recv()
			if err != nil {
				return
			}
			mu.Lock()
			for _, e := range batch.Entries {
				seqs = append(seqs, e.SeqId)
			}
			mu.Unlock()
			if len(seqs) >= 5 {
				cancel()
				return
			}
		}
	}()

	time.Sleep(200 * time.Millisecond)
	for i := 0; i < 2; i++ {
		w.Append(&pb.WalEntry{
			Args: [][]byte{[]byte("SET"), []byte("k"), []byte("v")},
		})
	}

	<-ctx.Done()
	mu.Lock()
	defer mu.Unlock()
	if len(seqs) != 5 {
		t.Fatalf("received %d entries, want 5", len(seqs))
	}
	for i, s := range seqs {
		if s != uint64(i+1) {
			t.Fatalf("entry %d has seq %d, want %d", i, s, i+1)
		}
	}
}
func TestRelay_PassThrough(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Primary with Redis.
	pmr := miniredis.RunT(t)
	pbe := proxy.NewBackend(proxy.BackendConfig{Addr: pmr.Addr()})
	pwal, _ := wal.Open(wal.Options{Dir: t.TempDir()})
	grpcLn, _ := net.Listen("tcp", "127.0.0.1:0")
	pd := proxy.NewDispatcher(pbe, proxy.NewRouter(), pwal, true, nil, false, false)
	gsrv := grpc.NewServer()
	pb.RegisterReplicationServer(gsrv, rsync.NewServer(pwal, pd, 0, nil))
	go gsrv.Serve(grpcLn)
	defer gsrv.Stop()

	// Relay — no backend, WAL pass-through.
	rwal, _ := wal.Open(wal.Options{Dir: t.TempDir()})
	defer rwal.Close()
	rpd := proxy.NewDispatcher(nil, proxy.NewRouter(), rwal, false,
		proxy.NewGRPCForwarder(grpcLn.Addr().String(), nil), true, true)
	relayLn, _ := net.Listen("tcp", "127.0.0.1:0")
	rgsrv := grpc.NewServer()
	pb.RegisterReplicationServer(rgsrv, rsync.NewServer(rwal, rpd, 0, nil))
	go rgsrv.Serve(relayLn)
	defer rgsrv.Stop()

	// Relay subscribes to primary.
	relayWm, _ := rsync.OpenWatermark(t.TempDir() + "/rwm")
	go rsync.NewFollower(rsync.FollowerConfig{
		OwnerAddrs:     []string{grpcLn.Addr().String()},
		FollowerRegion: "relay", FollowerID: "relay",
		Applier: rpd, Watermark: relayWm, AckInterval: 100 * time.Millisecond,
	}).Run(ctx)

	// Follower with Redis, connected through relay.
	fmr := miniredis.RunT(t)
	fbe := proxy.NewBackend(proxy.BackendConfig{Addr: fmr.Addr()})
	fwal, _ := wal.Open(wal.Options{Dir: t.TempDir()})
	defer fwal.Close()
	fpd := proxy.NewDispatcher(fbe, proxy.NewRouter(), fwal, false, nil, false, false)
	followerWm, _ := rsync.OpenWatermark(t.TempDir() + "/fwm")
	go rsync.NewFollower(rsync.FollowerConfig{
		OwnerAddrs:     []string{relayLn.Addr().String()},
		FollowerRegion: "follower", FollowerID: "follower",
		Applier: fpd, Watermark: followerWm, AckInterval: 100 * time.Millisecond,
	}).Run(ctx)

	// Write through dispatcher so WAL is populated.
	pd.DispatchWrite(ctx, [][]byte{[]byte("SET"), []byte("x"), []byte("hello-from-relay")})
	t.Logf("primary WAL seq=%d, relay WAL seq=%d, follower WAL seq=%d",
		pwal.NextSeq(), rwal.NextSeq(), fwal.NextSeq())
	waitFor(t, "relay pass-through", func() bool {
		v, _ := fmr.Get("x")
		return v == "hello-from-relay"
	})
	_ = pbe.Close()
	_ = fbe.Close()
}
func TestLB_Passthrough(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Primary with Redis.
	pmr := miniredis.RunT(t)
	pbe := proxy.NewBackend(proxy.BackendConfig{Addr: pmr.Addr()})
	pwal, _ := wal.Open(wal.Options{Dir: t.TempDir()})
	defer pwal.Close()
	grpcLn, _ := net.Listen("tcp", "127.0.0.1:0")
	pd := proxy.NewDispatcher(pbe, proxy.NewRouter(), pwal, true, nil, false, false)
	gsrv := grpc.NewServer()
	pb.RegisterReplicationServer(gsrv, rsync.NewServer(pwal, pd, 0, nil))
	go gsrv.Serve(grpcLn)
	defer gsrv.Stop()

	// LB — pure proxy, no WAL, no backend.
	lbLn, _ := net.Listen("tcp", "127.0.0.1:0")
	lbsrv := grpc.NewServer()
	lbSync := rsync.NewServer(nil, nil, 0, nil)
	lbSync.SetUpstream([]string{grpcLn.Addr().String()})
	pb.RegisterReplicationServer(lbsrv, lbSync)
	go lbsrv.Serve(lbLn)
	defer lbsrv.Stop()

	// Follower connects through LB.
	fmr := miniredis.RunT(t)
	fbe := proxy.NewBackend(proxy.BackendConfig{Addr: fmr.Addr()})
	defer fbe.Close()
	fwal, _ := wal.Open(wal.Options{Dir: t.TempDir()})
	defer fwal.Close()
	fpd := proxy.NewDispatcher(fbe, proxy.NewRouter(), fwal, false, nil, false, false)
	followerWm, _ := rsync.OpenWatermark(t.TempDir() + "/fwm")
	go rsync.NewFollower(rsync.FollowerConfig{
		OwnerAddrs:     []string{lbLn.Addr().String()},
		FollowerRegion: "follower", FollowerID: "follower",
		Applier: fpd, Watermark: followerWm, AckInterval: 100 * time.Millisecond,
	}).Run(ctx)

	// Write via primary, read through LB-proxied stream.
	pd.DispatchWrite(ctx, [][]byte{[]byte("SET"), []byte("lb"), []byte("works")})
	waitFor(t, "LB passthrough", func() bool {
		v, _ := fmr.Get("lb")
		return v == "works"
	})
	_ = pbe.Close()
}

func TestRoutes_CrossPrefixForwarding(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Island US: primary-us.
	usMR := miniredis.RunT(t)
	usBe := proxy.NewBackend(proxy.BackendConfig{Addr: usMR.Addr()})
	usWal, _ := wal.Open(wal.Options{Dir: t.TempDir()})
	usLn, _ := net.Listen("tcp", "127.0.0.1:0")
	usD := proxy.NewDispatcher(usBe, proxy.NewRouter(), usWal, true, nil, false, false)
	usGsrv := grpc.NewServer()
	pb.RegisterReplicationServer(usGsrv, rsync.NewServer(usWal, usD, 0, nil))
	go usGsrv.Serve(usLn)
	defer usGsrv.Stop()

	// Island EU: primary-eu.
	euMR := miniredis.RunT(t)
	euBe := proxy.NewBackend(proxy.BackendConfig{Addr: euMR.Addr()})
	euWal, _ := wal.Open(wal.Options{Dir: t.TempDir()})
	euLn, _ := net.Listen("tcp", "127.0.0.1:0")
	euD := proxy.NewDispatcher(euBe, proxy.NewRouter(), euWal, true, nil, false, false)
	euGsrv := grpc.NewServer()
	pb.RegisterReplicationServer(euGsrv, rsync.NewServer(euWal, euD, 0, nil))
	go euGsrv.Serve(euLn)
	defer euGsrv.Stop()

	// Primary-US has routes: eu:* → forward to primary-eu.
	usD.Routes = []proxy.RouteEntry{
		{Prefix: "eu:", Fwd: proxy.NewGRPCForwarder(euLn.Addr().String(), nil)},
	}
	euD.Routes = []proxy.RouteEntry{
		{Prefix: "us:", Fwd: proxy.NewGRPCForwarder(usLn.Addr().String(), nil)},
	}

	// Write via primary-US RESP path: local key stays, eu goes to EU.
	dispatch := func(d *proxy.Dispatcher, args ...string) {
		cmd := &resp.Command{Args: make([][]byte, len(args))}
		for i, a := range args { cmd.Args[i] = []byte(a) }
		w := resp.NewWriter(os.Stdout)
		d.Dispatch(ctx, cmd, w, nil)
	}
	dispatch(usD, "SET", "us:nyc", "us-value")
	dispatch(usD, "SET", "eu:ber", "eu-value")

	// Verify routing.
	if v, _ := usMR.Get("us:nyc"); v != "us-value" {
		t.Fatalf("us:nyc should be on US island, got %q", v)
	}
	if exists := usMR.Exists("eu:ber"); exists {
		t.Fatal("eu:ber should NOT be on US island")
	}
	if v, _ := euMR.Get("eu:ber"); v != "eu-value" {
		t.Fatalf("eu:ber should be on EU island, got %q", v)
	}
	_ = usBe.Close()
	_ = euBe.Close()
	_ = usWal.Close()
	_ = euWal.Close()
}
