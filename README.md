# meridian-redis-bridge

单主写入、多点读取的 Redis 跨区域同步代理。Primary 持有写权，写操作通过
WAL 复制到 Follower，Follower 接受本地读。Relay 纯过路缓存 WAL，
LB 透明代理。各节点以 upstream 指针形成树形拓扑。

## 快速开始

```bash
cd examples
docker compose up --build

# 写 primary，读 follower
redis-cli -p 6381 SET hello world
redis-cli -p 6382 GET hello   # → "world"
```

## 设计

```
写入方向:   client → follower → relay → primary → WAL → backend
复制方向:   primary → WAL → gRPC → relay → gRPC → follower → backend
```

- **Primary**（`upstream` 为空）：写请求本地执行、写入 WAL、复制给下游。
- **Follower**（有 `upstream`，有 `backend`）：读请求本地执行；写请求转发给上游。
- **Relay**（有 `upstream`，`relay: true`，无 `backend`）：缓存 WAL，扇出给下游。无 backend 时不执行读写，纯过路。
- **LB**（有 `upstream`，无 `relay`，无 `backend`）：纯 gRPC 代理，订阅和写转发透明穿透。零落盘。

## 配置

```json
{
  "cluster": "prod-us",
  "name": "primary-east",
  "upstream": ["10.0.0.1:7070", "10.0.0.2:7070"],
  "relay": false,
  "listen": ":6380",
  "grpc_listen": ":7070",
  "metrics_listen": ":8080",
  "forward_writes": true,
  "backend": {"addr": "127.0.0.1:6379"},
  "data_dir": "/var/lib/meridian"
}
```

| 字段 | 说明 |
|------|------|
| `cluster` | 集群名（监控用 label） |
| `name` | 节点名（监控用 label） |
| `upstream` | 上游 gRPC 地址列表，follower 随机选一连接。空 = primary |
| `relay` | 是否缓写 WAL 供下游订阅。默认 `false` |
| `listen` | RESP 前端地址（LB 模式可省略） |
| `grpc_listen` | gRPC 复制/转发地址 |
| `metrics_listen` | Prometheus metrics + health/pprof HTTP 地址 |
| `forward_writes` | follower 是否转发写。默认 `true` |
| `backend` | 本地 Redis/Kvrocks 地址（LB/relay 可省略）。Cluster 模式用 `addrs` 数组 |
| `data_dir` | WAL 和 watermark 存储目录（LB 可省略） |
| `auth` | 客户端认证，`passwd_file` 为 htpasswd 格式 |

### 节点角色速查

| 角色 | upstream | relay | backend | data_dir |
|------|----------|-------|---------|----------|
| Primary | 空 | - | 必须 | 必须 |
| Follower | 有 | false | 必须 | 必须 |
| Relay | 有 | true | 空 | 必须 |
| LB | 有 | false | 空 | 空 |

## 支持的命令

### 写命令（RouteWrite）

String：`SET` `SETNX` `SETEX` `PSETEX` `GETSET` `GETDEL` `MSET` `APPEND` `DEL` `UNLINK`
`INCR` `DECR` `INCRBY` `DECRBY` `INCRBYFLOAT`

Hash：`HSET` `HMSET` `HSETNX` `HDEL` `HINCRBY` `HINCRBYFLOAT`

List：`LPUSH` `RPUSH` `LPUSHX` `RPUSHX` `LPOP` `RPOP` `LSET` `LINSERT` `LTRIM` `LREM` `LMOVE` `RPOPLPUSH`

Set：`SADD` `SREM` `SMOVE` `SINTERSTORE` `SUNIONSTORE` `SDIFFSTORE`

Sorted Set：`ZADD` `ZREM` `ZINCRBY` `ZPOPMIN` `ZPOPMAX` `ZREMRANGEBYLEX` `ZREMRANGEBYRANK` `ZREMRANGEBYSCORE` `ZUNIONSTORE` `ZINTERSTORE`

Expiry：`EXPIRE` `PEXPIRE` `EXPIREAT` `PEXPIREAT` `PERSIST`

### 读命令（RouteRead）

String：`GET` `MGET` `GETRANGE` `STRLEN` `GETBIT`

Hash：`HGET` `HGETALL` `HMGET` `HKEYS` `HVALS` `HLEN` `HEXISTS` `HRANDFIELD`

List：`LLEN` `LINDEX` `LRANGE` `LPOS`

Set：`SCARD` `SISMEMBER` `SMEMBERS` `SRANDMEMBER` `SINTER` `SUNION` `SDIFF` `SSCAN`

Sorted Set：`ZCARD` `ZCOUNT` `ZLEXCOUNT` `ZRANGE` `ZREVRANGE` `ZRANGEBYLEX` `ZREVRANGEBYLEX` `ZRANGEBYSCORE` `ZREVRANGEBYSCORE` `ZRANK` `ZREVRANK` `ZSCORE` `ZMSCORE` `ZSCAN`

Key：`EXISTS` `TYPE` `TTL` `PTTL` `EXPIRETIME` `PEXPIRETIME` `OBJECT` `RANDOMKEY`

### 会话命令

端终结：`AUTH` `HELLO` `QUIT` `RESET` `PING` `ECHO` `SELECT`

### 拒绝命令

- **非确定性**：`SPOP`
- **阻塞命令**：`BLPOP` `BRPOP` `BRPOPLPUSH` `BLMOVE` `BZPOPMIN` `BZPOPMAX`
- **Stream**：`XADD` `XREAD` 等
- **危险/管理类**：`FLUSHALL` `FLUSHDB` `CONFIG` `SHUTDOWN` `DEBUG` `SCRIPT` `EVAL` `EVALSHA`
- **非代理类**：`KEYS` `SCAN` `INFO` `MONITOR` `CLUSTER` `DBSIZE`

## 认证

```json
{ "auth": { "passwd_file": "/etc/meridian/passwd" } }
```

文件格式：每行 `username:password` 或 `username:$2b$...`（bcrypt），`#` 注释。
不配置 `passwd_file` 则不认证。

## 构建

```bash
go build ./cmd/meridian
```

要求 Go ≥ 1.24。

### WAL 刷盘性能（Apple M3）

| 模式 | 单线程 | 8 并发 | 延迟 |
|------|--------|--------|------|
| `none` | 282k ops/s | 208k ops/s | 4.7µs |
| `periodic` | 50k ops/s | 25k ops/s | 33µs |
| `sync` | 556 ops/s | 438 ops/s | 2.9ms |

配置：`"wal_flush": "periodic", "wal_flush_interval": 100`（periodic 毫秒间隔）

### Docker

```bash
docker build -t meridian-redis-bridge .
```

## 为什么是单主

### 没有 per-key ownership

Per-key ownership 的收益前提是 key 天然有区域亲和性（GDPR 属地等）。
多数生产系统跨区 Redis 的写几乎全部集中于主区域，读才是全局的。claim
机制为极低频的"多区域并发首次写入同一 key"买单，而这在真实负载中接近不出现。

### 没有 CRDT

CRDT 需要每数据结构的专用 delta 编码、向量时钟、墓碑 GC，且与
非确定性操作不兼容。AWS MemoryDB 在存储引擎层面做了 LWW + 元素级合并，
代价是放弃 List 类型。Redis 是单线程确定性执行的存储；跨区同步是缓存和
session 问题，不是 CRDT 的协作编辑问题。

**单主写入 + 命令级 WAL + relay 链覆盖了 Redis 跨区同步的真实需求。**

### WAL 模型

命令级 WAL：每条写命令以原始参数写入 WAL，follower 原样重放。在单主模型
下重放是确定性的——follower 按 primary 的 WAL 顺序执行完全相同命令。

全量同步：新 follower 的 `Subscribe(from_seq=0)` 触发 primary 的 `SCAN`
流式快照，之后切到 WAL。expiry 自动转 `PEXPIREAT` 绝对值。

## 架构

```
cmd/meridian
├── RESP 前端 (internal/server)
├── 命令分发 (internal/proxy/dispatch)
├── 复制 (internal/sync) — gRPC Subscribe + Forward
├── WAL (internal/wal) — 分段、CRC 校验的追加日志
├── 后端 (internal/proxy/backend) — go-redis 封装
├── 转发 (internal/proxy/forward) — gRPC Forward RPC
├── 认证 (internal/auth) — htpasswd + bcrypt
└── 配置 (internal/config)
```

## 许可

MIT
