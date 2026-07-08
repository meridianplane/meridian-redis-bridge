// Package proxy: the follower apply path.
//
// A follower replays every WAL entry — a raw Redis command — via Backend.Do
// in strict seq order. Replay is deterministic under single-writer because
// the follower applies the same commands in the same sequence the primary
// wrote them, so backend state at each step is identical.
//
// Relay mode: when the follower has a local WAL (wal_dir configured), it
// writes each entry to its own WAL before applying to the backend. This turns
// the follower into a replication relay — downstream followers subscribe to
// the relay's WAL, forming a chain: primary → relay → follower. This is
// useful when compliance boundaries prevent two regions from connecting
// directly (e.g. an intermediate region acts as a data diode).
package proxy

import (
	"context"

	pb "github.com/meridianplane/meridian-redis-bridge/proto"
)

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
