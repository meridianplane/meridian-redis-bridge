package proxy

import (
	"context"
	"errors"
	"sync"

	"github.com/meridianplane/meridian-redis-bridge/internal/resp"
	pb "github.com/meridianplane/meridian-redis-bridge/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// GRPCForwarder relays writes upstream over gRPC, the same channel used
// for WAL replication.
type GRPCForwarder struct {
	addr string
	mu   sync.Mutex
	conn *grpc.ClientConn
	cli  pb.ReplicationClient
}

func NewGRPCForwarder(addr string) *GRPCForwarder {
	return &GRPCForwarder{addr: addr}
}

func (g *GRPCForwarder) client() (pb.ReplicationClient, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.cli != nil {
		return g.cli, nil
	}
	conn, err := grpc.NewClient(g.addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	g.conn = conn
	g.cli = pb.NewReplicationClient(conn)
	return g.cli, nil
}

func (g *GRPCForwarder) Forward(ctx context.Context, c *resp.Command) (any, error) {
	cli, err := g.client()
	if err != nil {
		return nil, err
	}
	reply, err := cli.Forward(ctx, &pb.ForwardRequest{Args: c.Args})
	if err != nil {
		return nil, err
	}
	if reply.IsNil {
		return nil, redisNil{}
	}
	if reply.Value != "" && reply.Value[0] == '-' {
		return nil, errors.New(reply.Value[1:])
	}
	return reply.Value, nil
}

func (g *GRPCForwarder) Close() error {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.conn != nil {
		return g.conn.Close()
	}
	return nil
}

type redisNil struct{}

func (redisNil) Error() string { return "redis: nil" }
