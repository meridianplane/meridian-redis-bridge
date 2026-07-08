// Package proxy: the dispatcher executes one client command.
//
// Single-owner, command-based WAL model: the primary records every write as the
// raw command that produced it, executes it on the local backend, and fans it
// out via the WAL. Followers replay each command via Backend.Do in strict WAL
// order, which is deterministic because every follower applies the same
// commands in the same sequence — there is no per-key ownership to resolve,
// no claim to fan out, and no canonical op to reduce to.
//
// Writes on a follower are forwarded to the primary (or rejected when
// ForwardWrites is disabled). Reads always run against the local copy.
package proxy

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/meridianplane/meridian-redis-bridge/internal/auth"
	"github.com/meridianplane/meridian-redis-bridge/internal/metrics"
	"github.com/meridianplane/meridian-redis-bridge/internal/resp"
	"github.com/redis/go-redis/v9"
	"github.com/meridianplane/meridian-redis-bridge/internal/wal"
	pb "github.com/meridianplane/meridian-redis-bridge/proto"
)

// ── Backend ──

type BackendConfig struct {
	Addr       string   // single node
	Addrs      []string // cluster seed nodes
	MasterName string   // sentinel master
	Username   string   // Redis 6+ ACL username
	Password   string   // requirepass or ACL password
	DB         int
	PoolSize   int
}

type Backend struct {
	cli redis.UniversalClient
}

func NewBackend(c BackendConfig) *Backend {
	if c.PoolSize == 0 { c.PoolSize = 32 }
	addrs := c.Addrs
	if len(addrs) == 0 && c.Addr != "" { addrs = []string{c.Addr} }
	cli := redis.NewUniversalClient(&redis.UniversalOptions{
		Addrs: addrs, MasterName: c.MasterName, Username: c.Username, Password: c.Password, DB: c.DB, PoolSize: c.PoolSize,
	})
	return &Backend{cli: cli}
}

func (b *Backend) Close() error                  { return b.cli.Close() }
func (b *Backend) Ping(ctx context.Context) error { return b.cli.Ping(ctx).Err() }
func (b *Backend) Do(ctx context.Context, args ...any) (any, error) {
	return b.cli.Do(ctx, args...).Result()
}
func IsNil(err error) bool { return errors.Is(err, redis.Nil) }

// shards returns each master shard for cluster backends, or nil for standalone.
func (b *Backend) shards() []*Backend {
	if b == nil {
		return nil
	}
	type shardWalker interface {
		ForEachMaster(ctx context.Context, fn func(context.Context, *redis.Client) error) error
	}
	if sw, ok := b.cli.(shardWalker); ok {
		var out []*Backend
		_ = sw.ForEachMaster(context.Background(), func(ctx context.Context, cli *redis.Client) error {
			out = append(out, &Backend{cli: cli})
			return nil
		})
		return out
	}
	return nil
}

// ── Forwarder ──

// Forwarder relays a write to an upstream node and returns its reply verbatim.
type Forwarder interface {
	Forward(ctx context.Context, c *resp.Command) (any, error)
}

// Dispatcher executes parsed commands against the local backend and the WAL.
type Dispatcher struct {
	Backend   *Backend
	Router    *Router
	WAL       *wal.WAL
	NowMillis func() int64

	IsPrimary     bool
	Forward       Forwarder
	ForwardWrites bool
	Relay         bool // when true, Apply writes entries to local WAL for downstream followers
	Auth          auth.Authenticator
	Metrics       *metrics.Metrics

	writeMu sync.Mutex // serialises WAL append + backend execute
}

var ErrClientQuit = errors.New("client quit")

func NewDispatcher(b *Backend, r *Router, w *wal.WAL, isPrimary bool, fwd Forwarder, forwardWrites bool, relay bool) *Dispatcher {
	return &Dispatcher{
		Backend:       b,
		Router:        r,
		WAL:           w,
		NowMillis:     func() int64 { return time.Now().UnixMilli() },
		IsPrimary:     isPrimary,
		Forward:       fwd,
		ForwardWrites: forwardWrites,
		Relay:         relay,
	}
}

// Dispatch handles a single client command end to end.
func (d *Dispatcher) Dispatch(ctx context.Context, c *resp.Command, w *resp.Writer, sess *Session) error {
	if sess == nil {
		sess = &Session{}
	}
	// Pre-auth connection commands — always allowed.
	switch c.Name() {
	case "AUTH":
		return d.handleAuth(c, w, sess)
	case "HELLO":
		return d.handleHello(c, w, sess)
	case "QUIT":
		return d.handleQuit(w)
	case "RESET":
		return d.handleReset(w, sess)
	}

	// Auth gate: everything beyond this point requires authentication.
	if d.authRequired() && !sess.authed {
		return w.WriteError("NOAUTH Authentication required.")
	}

	// Connection-local commands that never reach the 
	switch c.Name() {
	case "PING":
		return d.handlePing(c, w)
	case "ECHO":
		return d.handleEcho(c, w)
	case "SELECT":
		return d.handleSelect(c, w)
	}

	route := d.Router.Decide(c.Name())
	switch route {
	case RouteDeny:
		d.Metrics.RecordDispatch("deny", 0)
		return w.WriteError(fmt.Sprintf(
			"ERR command %q is not supported by singleowner", c.Name()))
	case RouteRead:
		t0 := time.Now()
		reply, err := d.do(ctx, c)
		d.Metrics.RecordDispatch("read", time.Since(t0))
		return writeReply(w, reply, err)
	case RouteWrite:
		t0 := time.Now()
		if !d.IsPrimary {
			return d.forwardWrite(ctx, c, w)
		}
		d.writeMu.Lock()
		reply, derr := d.dispatchWrite(ctx, c)
		d.writeMu.Unlock()
		d.Metrics.RecordDispatch("write", time.Since(t0))
		return writeReply(w, reply, derr)
	}
	return w.WriteError("ERR internal: unknown route")
}

// StreamSnapshot streams the backend's full state as a snapshot. barrier is
// the WAL seq pinned during the scan. fn is called for each batch of
// commands; after StreamSnapshot returns, the scan is complete and WAL
// streaming can begin from barrier.
func (d *Dispatcher) StreamSnapshot(ctx context.Context, barrier uint64, fn func([][][]byte) error) error {
	const batchSize = 500
	batch := make([][][]byte, 0, batchSize)

	// Discover shards: for cluster backends, iterate each master.
	shards := d.Backend.shards()
	if len(shards) == 0 {
		shards = append(shards, d.Backend)
	}
	for _, shard := range shards {
		cursor := uint64(0)
		for {
			v, err := shard.Do(ctx, "SCAN", fmt.Sprint(cursor), "COUNT", "1000")
			if err != nil {
				return err
			}
			arr, ok := v.([]any)
			if !ok || len(arr) != 2 {
				return fmt.Errorf("bad SCAN reply")
			}
			cursor, _ = strconv.ParseUint(fmt.Sprint(arr[0]), 10, 64)
			keys, ok := arr[1].([]any)
			if !ok {
				return fmt.Errorf("bad SCAN keys")
			}
			for _, k := range keys {
				key := fmt.Sprint(k)
				cmds := scanKey(ctx, shard, key)
				batch = append(batch, cmds...)
				if len(batch) >= batchSize {
					if err := fn(batch); err != nil {
						return err
					}
					batch = batch[:0]
				}
			}
			if cursor == 0 {
				break
			}
		}
	}
	if len(batch) > 0 {
		return fn(batch)
	}
	return nil
}

func scanKey(ctx context.Context, be *Backend, key string) [][][]byte {
	t, _ := be.Do(ctx, "TYPE", key)
	typ := fmt.Sprint(t)
	var cmds [][][]byte
	switch typ {
	case "string":
		v, _ := be.Do(ctx, "GET", key)
		if v != nil {
			cmds = append(cmds, bargs("SET", key, fmt.Sprint(v)))
			ttl, _ := be.Do(ctx, "PTTL", key)
			if ttlMs, ok := ttl.(int64); ok && ttlMs > 0 {
				cmds = append(cmds, bargs("PEXPIREAT", key, fmt.Sprint(time.Now().UnixMilli()+ttlMs)))
			}
		}
	case "hash":
		fields, _ := be.Do(ctx, "HGETALL", key)
		if arr2, ok := fields.([]any); ok && len(arr2) > 0 {
			parts := []string{"HSET", key}
			for _, f := range arr2 {
				parts = append(parts, fmt.Sprint(f))
			}
			cmds = append(cmds, bargs(parts...))
		}
	case "list":
		elems, _ := be.Do(ctx, "LRANGE", key, "0", "-1")
		if arr2, ok := elems.([]any); ok && len(arr2) > 0 {
			for i := len(arr2) - 1; i >= 0; i-- {
				cmds = append(cmds, bargs("RPUSH", key, fmt.Sprint(arr2[i])))
			}
		}
	case "set":
		members, _ := be.Do(ctx, "SMEMBERS", key)
		if arr2, ok := members.([]any); ok && len(arr2) > 0 {
			parts := []string{"SADD", key}
			for _, m := range arr2 {
				parts = append(parts, fmt.Sprint(m))
			}
			cmds = append(cmds, bargs(parts...))
		}
	case "zset":
		zs, _ := be.Do(ctx, "ZRANGE", key, "0", "-1", "WITHSCORES")
		if arr2, ok := zs.([]any); ok && len(arr2) > 0 {
			parts := []string{"ZADD", key}
			for i := 0; i < len(arr2); i += 2 {
				parts = append(parts, fmt.Sprint(arr2[i+1]), fmt.Sprint(arr2[i]))
			}
			cmds = append(cmds, bargs(parts...))
		}
	}
	return cmds
}

// DispatchWrite records a raw command in the WAL and executes it. It is the
// entry point for forwarded writes arriving over gRPC.
func (d *Dispatcher) DispatchWrite(ctx context.Context, args [][]byte) (any, error) {
	d.writeMu.Lock()
	defer d.writeMu.Unlock()
	if d.WAL != nil {
		if _, err := d.WAL.Append(&pb.WalEntry{
			Args:        args,
			TimestampMs: d.NowMillis(),
		}); err != nil {
			return nil, fmt.Errorf("WAL append: %w", err)
		}
	}
	if d.Backend == nil {
		return "OK", nil // relay-only: forwarded writes acknowledged
	}
	doArgs := make([]any, len(args))
	for i := range args {
		doArgs[i] = args[i]
	}
	return d.Backend.Do(ctx, doArgs...)
}

// dispatchWrite records the command in the WAL, then executes it on the
//  WAL-first means a crash between Append and backend execution leaves
// a record that re-applies on restart; a crash before Append loses the write
// entirely (the client retries). Under the single-writer model the reply is a
// deterministic function of backend state at this point, so followers replaying
// the same command in the same sequence produce the same reply.
func (d *Dispatcher) dispatchWrite(ctx context.Context, c *resp.Command) (any, error) {
	// Normalize relative expiry into absolute PEXPIREAT before WAL append
	// so the WAL is time-independent. The backend still runs the original
	// command (which handles relative time correctly at execution time).
	if d.WAL != nil {
		now := d.NowMillis()
		entries := normalizeExpiry(c.Args, now)
		for _, args := range entries {
			if _, err := d.WAL.Append(&pb.WalEntry{Args: args, TimestampMs: now}); err != nil {
				return nil, fmt.Errorf("WAL append: %w", err)
			}
		}
	}
	return d.do(ctx, c)
}

// normalizeExpiry converts commands with relative expiry (EX/PX on SET,
// EXPIRE/PEXPIRE, SETEX/PSETEX) into time-independent absolute form. A SET
// with EX becomes [SET k v] + [PEXPIREAT k abs]; an EXPIRE becomes
// [PEXPIREAT k abs]. Commands without expiry return as-is.
func normalizeExpiry(args [][]byte, nowMs int64) [][][]byte {
	if len(args) < 2 {
		return [][][]byte{args}
	}
	cmd := strings.ToUpper(string(args[0]))
	switch cmd {
	case "SET":
		return normalizeSet(args, nowMs)
	case "SETEX", "PSETEX":
		return normalizeSetEx(args, nowMs)
	case "EXPIRE", "PEXPIRE":
		return normalizeExpireCmd(args, nowMs, false)
	case "EXPIREAT", "PEXPIREAT":
		return normalizeExpireCmd(args, nowMs, true)
	case "PERSIST":
		return [][][]byte{args} // PERSIST is already absolute
	}
	return [][][]byte{args}
}

func normalizeSet(args [][]byte, nowMs int64) [][][]byte {
	var expireAtMs int64
	// Build the cleaned SET command: SET key value [NX|XX] [KEEPTTL|GET]
	writeArgs := [][]byte{args[0], args[1], args[2]}
	for i := 3; i < len(args); i++ {
		opt := strings.ToUpper(string(args[i]))
		switch opt {
		case "EX", "PX", "EXAT", "PXAT":
			if i+1 < len(args) {
				n, _ := strconv.ParseInt(string(args[i+1]), 10, 64)
				switch opt {
				case "EX":
					expireAtMs = nowMs + n*1000
				case "PX":
					expireAtMs = nowMs + n
				case "EXAT":
					expireAtMs = n * 1000
				case "PXAT":
					expireAtMs = n
				}
				i++
			}
		case "NX", "XX", "KEEPTTL", "GET":
			writeArgs = append(writeArgs, args[i])
		}
	}
	if expireAtMs > 0 {
		return [][][]byte{writeArgs, bargs("PEXPIREAT", string(args[1]), fmt.Sprint(expireAtMs))}
	}
	return [][][]byte{args}
}

func normalizeSetEx(args [][]byte, nowMs int64) [][][]byte {
	// SETEX k 60 v → SET k v + PEXPIREAT k abs
	if len(args) < 4 {
		return [][][]byte{args}
	}
	ms := "1" // PSETEX uses milliseconds
	secs, err := strconv.ParseInt(string(args[2]), 10, 64)
	if err != nil {
		return [][][]byte{args}
	}
	var expireAtMs int64
	if strings.ToUpper(string(args[0])) == "PSETEX" {
		expireAtMs = nowMs + secs
		ms = "1"
	} else {
		expireAtMs = nowMs + secs*1000
	}
	_ = ms
	return [][][]byte{
		bargs("SET", string(args[1]), string(args[3])),
		bargs("PEXPIREAT", string(args[1]), fmt.Sprint(expireAtMs)),
	}
}

func normalizeExpireCmd(args [][]byte, nowMs int64, isAt bool) [][][]byte {
	if len(args) < 3 {
		return [][][]byte{args}
	}
	cmd := strings.ToUpper(string(args[0]))
	secs, err := strconv.ParseInt(string(args[2]), 10, 64)
	if err != nil {
		return [][][]byte{args}
	}
	var expireAtMs int64
	if isAt {
		if cmd == "PEXPIREAT" {
			expireAtMs = secs // already ms
		} else {
			expireAtMs = secs * 1000 // EXPIREAT: seconds → ms
		}
	} else {
		if cmd == "PEXPIRE" {
			expireAtMs = nowMs + secs
		} else {
			expireAtMs = nowMs + secs*1000
		}
	}
	// Preserve NX/XX/GT/LT options if present.
	pexArgs := [][]byte{[]byte("PEXPIREAT"), args[1], []byte(fmt.Sprint(expireAtMs))}
	for i := 3; i < len(args); i++ {
		pexArgs = append(pexArgs, args[i])
	}
	return [][][]byte{pexArgs}
}

// forwardWrite relays a write upstream and renders the reply.
func (d *Dispatcher) forwardWrite(ctx context.Context, c *resp.Command, w *resp.Writer) error {
	if !d.ForwardWrites {
		return w.WriteError("ERR this node is not the primary and write forwarding is disabled")
	}
	if d.Forward == nil {
		return w.WriteError("ERR this node is not the primary and no upstream is configured")
	}
	reply, err := d.Forward.Forward(ctx, c)
	return writeReply(w, reply, err)
}

// Apply replays one WAL entry onto the local backend, optionally relayed.
// A relay-only node (no backend) just writes to its own WAL.
func (d *Dispatcher) Apply(ctx context.Context, e *pb.WalEntry) error {
	if d.Relay {
		if _, err := d.WAL.Append(e); err != nil {
			return err
		}
	}
	if d.Backend == nil {
		return nil
	}
	return d.exec(ctx, e.Args)
}

func (d *Dispatcher) exec(ctx context.Context, args [][]byte) error {
	a := make([]any, len(args))
	for i := range args {
		a[i] = args[i]
	}
	_, err := d.Backend.Do(ctx, a...)
	return err
}

func (d *Dispatcher) do(ctx context.Context, c *resp.Command) (any, error) {
	if d.Backend == nil {
		return nil, fmt.Errorf("ERR this node has no backend configured")
	}
	args := make([]any, len(c.Args))
	for i := range c.Args {
		args[i] = c.Args[i]
	}
	t0 := time.Now()
	reply, err := d.Backend.Do(ctx, args...)
	d.Metrics.RecordBackend(time.Since(t0), err != nil)
	return reply, err
}

func writeReply(w *resp.Writer, v any, err error) error {
	if err != nil {
		if IsNil(err) {
			return w.WriteNullBulk()
		}
		msg := err.Error()
		if !looksLikeRespError(msg) {
			msg = "ERR " + msg
		}
		return w.WriteError(msg)
	}
	switch t := v.(type) {
	case nil:
		return w.WriteNullBulk()
	case string:
		if t == "OK" || t == "PONG" || t == "QUEUED" {
			return w.WriteSimpleString(t)
		}
		return w.WriteBulkString([]byte(t))
	case []byte:
		return w.WriteBulkString(t)
	case int64:
		return w.WriteInt(t)
	case int:
		return w.WriteInt(int64(t))
	case bool:
		if t {
			return w.WriteInt(1)
		}
		return w.WriteInt(0)
	case []any:
		if err := w.WriteArrayHeader(len(t)); err != nil {
			return err
		}
		for _, item := range t {
			if err := writeReply(w, item, nil); err != nil {
				return err
			}
		}
		return nil
	}
	return w.WriteError(fmt.Sprintf("ERR unsupported reply type %T", v))
}

func looksLikeRespError(msg string) bool {
	word := msg
	if i := strings.IndexByte(msg, ' '); i > 0 {
		word = msg[:i]
	}
	if word == "" {
		return false
	}
	for _, r := range word {
		if r < 'A' || r > 'Z' {
			return false
		}
	}
	return true
}


func bargs(ss ...string) [][]byte {
	out := make([][]byte, len(ss))
	for i, s := range ss {
		out[i] = []byte(s)
	}
	return out
}
