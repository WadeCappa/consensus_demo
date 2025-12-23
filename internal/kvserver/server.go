package kvserver

import (
	"context"
	"fmt"

	"github.com/WadeCappa/consensus/internal/db"
	"github.com/WadeCappa/consensus/pkg/go/kvstore/v1"
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

func (s *kvserver) Get(ctx context.Context, request *kvstorepb.GetRequest) (*kvstorepb.GetResponse, error) {
	data, exists := s.data.Get(request.Key)
	if !exists {
		return nil, fmt.Errorf("failed to find data for key %s", request.Key)
	}

	return &kvstorepb.GetResponse{
		Version: data.Version,
		Data:    data.Data,
	}, nil
}

func (s *kvserver) Put(ctx context.Context, request *kvstorepb.PutRequest) (*kvstorepb.PutResponse, error) {
	newRecord := db.NewDataVersion(request.Version, request.Data)
	if err := s.data.Put(request.Key, newRecord); err != nil {
		return nil, fmt.Errorf("putting record: %w", err)
	}

	return &kvstorepb.PutResponse{}, nil
}
