package sync

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/meridianplane/meridian-redis-bridge/internal/wal"
	pb "github.com/meridianplane/meridian-redis-bridge/proto"
)

const defaultMaxBatch = 256

type WriteHandler interface {
	DispatchWrite(ctx context.Context, args [][]byte) (any, error)
}

type StreamSnapshotter interface {
	StreamSnapshot(ctx context.Context, barrier uint64, fn func([][][]byte) error) error
}

type Server struct {
	pb.UnimplementedReplicationServer

	wal      *wal.WAL
	writer   WriteHandler
	maxBatch int

	upstreamAddrs []string // for relay: proxy full sync to upstream

	mu         sync.Mutex
	watermarks map[string]uint64
}

func NewServer(w *wal.WAL, writer WriteHandler, maxBatch int) *Server {
	if maxBatch <= 0 {
		maxBatch = defaultMaxBatch
	}
	return &Server{
		wal:        w,
		writer:     writer,
		maxBatch:   maxBatch,
		watermarks: map[string]uint64{},
	}
}

func (s *Server) SetUpstream(addrs []string) { s.upstreamAddrs = addrs }

func (s *Server) Ack(_ context.Context, req *pb.AckRequest) (*pb.AckReply, error) {
	s.mu.Lock()
	if req.AppliedSeq > s.watermarks[req.FollowerId] {
		s.watermarks[req.FollowerId] = req.AppliedSeq
	}
	s.mu.Unlock()
	return &pb.AckReply{}, nil
}

func (s *Server) Subscribe(req *pb.SubscribeRequest, stream pb.Replication_SubscribeServer) error {
	from := req.GetFromSeq()
	ctx := stream.Context()

	// LB mode: no local WAL, always proxy.
	if s.wal == nil {
		return s.proxyFullSync(stream, ctx)
	}

	notify := s.wal.Notify()

	if from == 0 || (s.wal.MinSeq() > 0 && from < s.wal.MinSeq()) {
		if err := s.fullSync(stream, ctx); err != nil {
			return err
		}
		from = s.wal.MinSeq()
	}
	if from == 0 {
		from = 1
	}

	for {
		for {
			entries, err := s.wal.ReadFrom(from, s.maxBatch)
			if err != nil {
				return err
			}
			if len(entries) == 0 {
				break
			}
			upTo := entries[len(entries)-1].SeqId
			if err := stream.Send(&pb.SyncBatch{Entries: entries, UpToSeq: upTo}); err != nil {
				return err
			}
			from = upTo + 1
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-notify:
		}
	}
}

func (s *Server) fullSync(stream pb.Replication_SubscribeServer, ctx context.Context) error {
	snapper, ok := s.writer.(StreamSnapshotter)
	if !ok {
		// Relay mode: proxy full sync to upstream.
		return s.proxyFullSync(stream, ctx)
	}
	barrier := s.wal.StartSnapshot()
	seq := uint64(1)
	if err := snapper.StreamSnapshot(ctx, barrier, func(cmds [][][]byte) error {
		entries := make([]*pb.WalEntry, len(cmds))
		for i, cmd := range cmds {
			entries[i] = &pb.WalEntry{SeqId: seq, Args: cmd}
			seq++
		}
		return stream.Send(&pb.SyncBatch{Entries: entries, UpToSeq: seq - 1})
	}); err != nil {
		s.wal.AbortSnapshot()
		return err
	}
	return nil
}

func (s *Server) proxyFullSync(stream pb.Replication_SubscribeServer, ctx context.Context) error {
	if len(s.upstreamAddrs) == 0 {
		return fmt.Errorf("full sync not available — no upstream configured")
	}
	// Connect to upstream and proxy the stream.
	conn, err := grpc.NewClient(s.upstreamAddrs[0], grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}
	defer conn.Close()
	cli := pb.NewReplicationClient(conn)
	upstream, err := cli.Subscribe(ctx, &pb.SubscribeRequest{FromSeq: 0})
	if err != nil {
		return err
	}
	for {
		batch, err := upstream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		if err := stream.Send(batch); err != nil {
			return err
		}
	}
}

func (s *Server) Forward(ctx context.Context, req *pb.ForwardRequest) (*pb.ForwardReply, error) {
	reply, derr := s.writer.DispatchWrite(ctx, req.Args)
	if derr != nil {
		if errors.Is(derr, redis.Nil) {
			return &pb.ForwardReply{IsNil: true}, nil
		}
		return &pb.ForwardReply{Value: fmt.Sprintf("-%s", derr.Error())}, nil
	}
	return &pb.ForwardReply{Value: fmt.Sprint(reply)}, nil
}
