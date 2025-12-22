package server

import (
	"context"
	"fmt"

	"github.com/WadeCappa/consensus/internal/db"
	consensuspb "github.com/WadeCappa/consensus/pkg/go/consensus/v1"
)

type server struct {
	consensuspb.ConsensusServer

	data *db.Database
}

func NewServer() consensuspb.ConsensusServer {
	return &server{
		data: db.NewDatabase(),
	}
}

func (s *server) Get(ctx context.Context, request *consensuspb.GetRequest) (*consensuspb.GetResponse, error) {
	data, exists := s.data.Get(request.Key)
	if !exists {
		return nil, fmt.Errorf("failed to find data for key %s", request.Key)
	}

	return &consensuspb.GetResponse{
		Version: data.Version,
		Data:    data.Data,
	}, nil
}

func (s *server) Put(ctx context.Context, request *consensuspb.PutRequest) (*consensuspb.PutResponse, error) {
	newRecord := db.NewRecord(request.Version, request.Data)
	if err := s.data.Put(request.Key, newRecord); err != nil {
		return nil, fmt.Errorf("putting record: %w", err)
	}

	return &consensuspb.PutResponse{}, nil
}
