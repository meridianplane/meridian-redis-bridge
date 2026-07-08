// Package sync: the follower-side replication consumer.
package sync

import (
	"context"
	"io"
	"log/slog"
	"math/rand"
	"time"

	pb "github.com/meridianplane/meridian-redis-bridge/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// Applier replays one replicated entry onto local state. *proxy.Dispatcher
// satisfies it via its Apply method.
type Applier interface {
	Apply(ctx context.Context, e *pb.WalEntry) error
}

// reconnect backoff bounds for the follower's outer loop.
const (
	minBackoff = 200 * time.Millisecond
	maxBackoff = 5 * time.Second
)

// FollowerConfig configures a replication consumer.
type FollowerConfig struct {
	OwnerAddrs     []string   // owner gRPC addresses, tried in order
	FollowerRegion string     // this follower's region label
	FollowerID     string     // stable identity for observability
	Applier        Applier    // applies each entry to local state
	Watermark      *Watermark // durable last-applied position
	AckInterval    time.Duration // how often to report applied position (0 = 30s)
	Logger         *slog.Logger
}

// Follower pulls its owner's WAL stream and applies it locally.
type Follower struct {
	cfg FollowerConfig
	log *slog.Logger
}

func NewFollower(cfg FollowerConfig) *Follower {
	log := cfg.Logger
	if log == nil {
		log = slog.Default()
	}
	if cfg.AckInterval == 0 {
		cfg.AckInterval = 30 * time.Second
	}
	return &Follower{cfg: cfg, log: log}
}

// Run drives the consume loop until ctx is cancelled. A dropped or failed
// stream is retried with capped exponential backoff; every reconnect resumes
// from the persisted watermark, so at-least-once redelivery only ever re-sends
// entries the idempotent apply path can safely replay.
func (f *Follower) Run(ctx context.Context) error {
	backoff := minBackoff
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		connected, err := f.runOnce(ctx)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if err != nil {
			f.log.Warn("replication stream dropped; retrying",
				"addrs", f.cfg.OwnerAddrs, "from_seq", f.cfg.Watermark.NextSeq(),
				"backoff", backoff, "err", err)
		}
		// A stream that actually opened resets the backoff, so a long-lived
		// healthy connection that later drops retries promptly rather than
		// inheriting a large delay from earlier failures.
		if connected {
			backoff = minBackoff
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
		}
		if !connected {
			if backoff *= 2; backoff > maxBackoff {
				backoff = maxBackoff
			}
		}
	}
}

// runOnce picks a random address, connects, and streams until the stream
// drops. On failure the outer loop retries with a fresh random pick.
func (f *Follower) runOnce(ctx context.Context) (bool, error) {
	from := f.cfg.Watermark.NextSeq()
	addrs := f.cfg.OwnerAddrs
	if len(addrs) == 0 {
		return false, nil
	}
	i := rand.Intn(len(addrs))
	addr := addrs[i]

	conn, err := grpc.NewClient(addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return false, err
	}
	client := pb.NewReplicationClient(conn)
	stream, err := client.Subscribe(ctx, &pb.SubscribeRequest{
		FollowerRegion: f.cfg.FollowerRegion,
		FollowerId:     f.cfg.FollowerID,
		FromSeq:        from,
	})
	if err != nil {
		conn.Close()
		return false, err
	}
	f.log.Info("replication stream open", "owner", addr, "from_seq", from)

	// Background ack: tell the primary how far we've applied, so it can
	// compact segments all followers have passed.
	ackCtx, ackCancel := context.WithCancel(ctx)
	defer ackCancel()
	go f.ackLoop(ackCtx, client)

	for {
		batch, err := stream.Recv()
		if err == io.EOF {
			return true, nil
		}
		if err != nil {
			return true, err
		}
		if err := f.applyBatch(ctx, batch); err != nil {
			return true, err
		}
	}
}

func (f *Follower) ackLoop(ctx context.Context, cli pb.ReplicationClient) {
	t := time.NewTicker(f.cfg.AckInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if _, err := cli.Ack(ctx, &pb.AckRequest{
				FollowerId: f.cfg.FollowerID,
				AppliedSeq: f.cfg.Watermark.Applied(),
			}); err != nil && ctx.Err() == nil {
				f.log.Warn("ack failed", "err", err)
			}
		}
	}
}

// applyBatch replays a batch in order and advances the watermark once, after
// the whole batch lands. Persisting per batch (not per entry) bounds fsyncs;
// crash recovery re-applies from the last persisted seq, which is safe because
// every op is idempotent.
func (f *Follower) applyBatch(ctx context.Context, batch *pb.SyncBatch) error {
	want := f.cfg.Watermark.NextSeq()
	for _, e := range batch.Entries {
		if e.SeqId < want {
			continue // already applied (redelivery after reconnect)
		}
		if err := f.cfg.Applier.Apply(ctx, e); err != nil {
			return err
		}
		want = e.SeqId + 1
	}
	if batch.UpToSeq > 0 {
		return f.cfg.Watermark.Set(batch.UpToSeq)
	}
	return nil
}
