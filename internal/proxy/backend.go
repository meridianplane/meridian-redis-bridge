package proxy

import (
	"context"
	"errors"

	"github.com/redis/go-redis/v9"
)

type BackendConfig struct {
	Addrs      []string
	Addr       string
	MasterName string
	Password   string
	DB         int
	PoolSize   int
}

type Backend struct {
	cli redis.UniversalClient
}

func NewBackend(c BackendConfig) *Backend {
	if c.PoolSize == 0 {
		c.PoolSize = 32
	}
	addrs := c.Addrs
	if len(addrs) == 0 && c.Addr != "" {
		addrs = []string{c.Addr}
	}
	cli := redis.NewUniversalClient(&redis.UniversalOptions{
		Addrs:      addrs,
		MasterName: c.MasterName,
		Password:   c.Password,
		DB:         c.DB,
		PoolSize:   c.PoolSize,
	})
	return &Backend{cli: cli}
}

func (b *Backend) Close() error { return b.cli.Close() }

func (b *Backend) Ping(ctx context.Context) error { return b.cli.Ping(ctx).Err() }

func (b *Backend) Do(ctx context.Context, args ...any) (any, error) {
	return b.cli.Do(ctx, args...).Result()
}

func IsNil(err error) bool { return errors.Is(err, redis.Nil) }
