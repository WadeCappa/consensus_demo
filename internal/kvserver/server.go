package kvserver

import (
	"context"
	"fmt"
	"time"

	"github.com/WadeCappa/consensus/internal/db"
	"github.com/WadeCappa/consensus/pkg/go/kvstore/v1"
	"google.golang.org/grpc"
)

type kvserver struct {
	kvstorepb.KvstoreServer

	data *db.Database
}

func NewKvServer(db *db.Database) kvstorepb.KvstoreServer {
	return &kvserver{
		data: db,
	}
}

func (s *kvserver) Put(
	ctx context.Context,
	request *kvstorepb.PutRequest,
) (*kvstorepb.PutResponse, error) {
	if err := s.data.Put(request.GetKey(), request.GetUpdate(), time.Now()); err != nil {
		return nil, fmt.Errorf("putting record: %w", err)
	}

	return &kvstorepb.PutResponse{}, nil
}

func (s *kvserver) Get(
	request *kvstorepb.GetRequest,
	stream grpc.ServerStreamingServer[kvstorepb.GetResponse],
) error {
	data, exists := s.data.Get(request.Key)
	if !exists {
		return fmt.Errorf("failed to find data for key %s", request.Key)
	}

	for _, c := range data.Chunks {
		c.Visit(func(writeTime time.Time, nodeId, version uint64, data []byte) {
			stream.Send(&kvstorepb.GetResponse{
				Data:                data,
				WriteTimeUnixMillis: uint64(writeTime.UnixMilli()),
				Version:             version,
				NodeId:              nodeId,
			})
		})
	}

	return nil
}
