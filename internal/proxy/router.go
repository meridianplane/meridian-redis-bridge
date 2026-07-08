// Package proxy turns a parsed RESP command into one of three outcomes:
// a replicated write, a local read, or a rejection.
//
// Routing is a strict whitelist with default-deny:
//
//   - RouteWrite — a command we have an explicit handler for. It runs on the
//                  primary AND is normalised into the WAL so it replicates to
//                  followers.
//   - RouteRead  — a read-only command over the keyspace. Every node holds the
//                  full replicated copy, so it is answered locally.
//   - RouteDeny  — everything else, INCLUDING any command we have not
//                  implemented.
//
// There is deliberately no "forward this verbatim to the backend" route.
// Connection and session commands (AUTH, HELLO, PING, ECHO, SELECT, QUIT,
// RESET) are terminated at the front-end before routing is consulted.
package proxy

import "strings"

type Route int

const (
	RouteDeny Route = iota
	RouteRead
	RouteWrite
)

// Router holds the routed command tables. It is read-only after construction,
// so concurrent Decide calls need no locking.
type Router struct {
	write map[string]struct{}
	read  map[string]struct{}
	deny  map[string]struct{}
}

// NewRouter returns the default router.
func NewRouter() *Router {
	r := &Router{
		write: make(map[string]struct{}),
		read:  make(map[string]struct{}),
		deny:  make(map[string]struct{}),
	}
	for _, c := range defaultWriteCommands {
		r.write[c] = struct{}{}
	}
	for _, c := range defaultReadCommands {
		r.read[c] = struct{}{}
	}
	for _, c := range defaultDenyCommands {
		r.deny[c] = struct{}{}
	}
	return r
}

// Decide returns the route for a command name (case-insensitive). The deny
// table wins over everything; an unknown command falls through to RouteDeny.
func (r *Router) Decide(name string) Route {
	name = strings.ToUpper(name)
	if _, ok := r.deny[name]; ok {
		return RouteDeny
	}
	if _, ok := r.write[name]; ok {
		return RouteWrite
	}
	if _, ok := r.read[name]; ok {
		return RouteRead
	}
	return RouteDeny
}

var defaultWriteCommands = []string{
	// String
	"SET", "SETNX", "SETEX", "PSETEX", "GETSET", "GETDEL", "MSET", "APPEND",
	"DEL", "UNLINK",
	"INCR", "DECR", "INCRBY", "DECRBY", "INCRBYFLOAT",
	// Hash
	"HSET", "HMSET", "HSETNX", "HDEL", "HINCRBY", "HINCRBYFLOAT",
	// List
	"LPUSH", "RPUSH", "LPUSHX", "RPUSHX", "LPOP", "RPOP",
	"LSET", "LINSERT", "LTRIM", "LREM", "LMOVE", "RPOPLPUSH",
	// Set
	"SADD", "SREM", "SMOVE", "SINTERSTORE", "SUNIONSTORE", "SDIFFSTORE",
	// Sorted Set
	"ZADD", "ZREM", "ZINCRBY", "ZPOPMIN", "ZPOPMAX",
	"ZREMRANGEBYLEX", "ZREMRANGEBYRANK", "ZREMRANGEBYSCORE",
	"ZUNIONSTORE", "ZINTERSTORE",
	// Expiry
	"EXPIRE", "PEXPIRE", "EXPIREAT", "PEXPIREAT", "PERSIST",
}

var defaultReadCommands = []string{
	// String
	"GET", "MGET", "GETRANGE", "STRLEN", "GETBIT",
	// Hash
	"HGET", "HGETALL", "HMGET", "HKEYS", "HVALS", "HLEN", "HEXISTS", "HRANDFIELD",
	// List
	"LLEN", "LINDEX", "LRANGE", "LPOS",
	// Set
	"SCARD", "SISMEMBER", "SMEMBERS", "SRANDMEMBER",
	"SINTER", "SUNION", "SDIFF", "SSCAN",
	// Sorted Set
	"ZCARD", "ZCOUNT", "ZLEXCOUNT",
	"ZRANGE", "ZREVRANGE", "ZRANGEBYLEX", "ZREVRANGEBYLEX",
	"ZRANGEBYSCORE", "ZREVRANGEBYSCORE", "ZRANK", "ZREVRANK",
	"ZSCORE", "ZMSCORE", "ZSCAN",
	// Key metadata
	"EXISTS", "TYPE", "TTL", "PTTL", "EXPIRETIME", "PEXPIRETIME", "OBJECT", "RANDOMKEY",
}

var defaultDenyCommands = []string{
	"FLUSHALL", "FLUSHDB", "DEBUG", "CONFIG", "REPLICAOF", "SLAVEOF", "SHUTDOWN",
	"SCRIPT", "EVAL", "EVALSHA", "FUNCTION",
	"KEYS", "SCAN",
	"MIGRATE", "SWAPDB", "FAILOVER", "MONITOR", "CLUSTER",
	"INFO", "COMMAND", "TIME", "CLIENT", "LATENCY", "MEMORY", "DBSIZE",
	// Non-deterministic: random result that cannot be replayed identically
	"SPOP",
	// Blocking: connection-scoped state incompatible with stateless proxy
	"BLPOP", "BRPOP", "BRPOPLPUSH", "BLMOVE", "BZPOPMIN", "BZPOPMAX",
	// Stream: consumer group semantics incompatible with cross-region replay
	"XADD", "XREAD", "XREADGROUP", "XACK", "XCLAIM", "XDEL", "XGROUP",
	"XINFO", "XLEN", "XPENDING", "XRANGE", "XREVRANGE", "XTRIM",
}
