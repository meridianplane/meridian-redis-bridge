# docker compose demo

Primary → relay → follower, with a Redis backend on each end. The relay
passes WAL through without a Redis backend of its own.

## Run

```bash
docker compose up --build
```

## Verify

```bash
# write via primary
redis-cli -p 6381 SET hello world

# read via follower
redis-cli -p 6382 GET hello    # → "world"

# all data types
redis-cli -p 6381 HSET user:1 name alice
redis-cli -p 6382 HGETALL user:1

redis-cli -p 6381 LPUSH list a b c
redis-cli -p 6382 LRANGE list 0 -1

redis-cli -p 6381 SADD tags redis proxy
redis-cli -p 6382 SMEMBERS tags
```

## Topology

```
redis-primary ← meridian-primary
              ↗     │ (gRPC)
     meridian-lb   meridian-relay  ←  no Redis, WAL pass-through
         │                │ (gRPC)
         └──── follower via LB? ──┘
redis-follower ← meridian-follower
```

| Port | Service           |
|------|-------------------|
| 6381 | primary proxy     |
| 6382 | follower proxy    |
| 7070 | primary gRPC      |
| 7072 | LB gRPC           |
| 7073 | relay gRPC        |
| 7071 | follower gRPC     |
