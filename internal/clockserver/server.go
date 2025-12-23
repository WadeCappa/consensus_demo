package clockserver

import (
	"fmt"
	"io"

	"github.com/WadeCappa/consensus/gen/go/clocks/v1"
	"github.com/WadeCappa/consensus/internal/db"
	"google.golang.org/grpc"
)

type clockServer struct {
	clockspb.ClocksServer

	data *db.Database
}

func NewClockServer(db *db.Database) clockspb.ClocksServer {
	return &clockServer{
		data: db,
	}
}

func (s *clockServer) Publish(
	stream grpc.ClientStreamingServer[clockspb.PublishRequest, clockspb.PublishResponse],
) error {
	for {
		select {
		case <-stream.Context().Done():
		default:
		}
		request, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("receiving next publish request: %w", err)
		}
		if err := s.data.Merge(request.Key, request.Data, db.FromWireType(request.Clock)); err != nil {
			return fmt.Errorf("merging clocks: %w", err)
		}
	}
	if err := stream.SendAndClose(&clockspb.PublishResponse{}); err != nil {
		return fmt.Errorf("closing stream: %w", err)
	}
	return nil
}

func (s *clockServer) Ack(
	request *clockspb.AckRequest,
	stream grpc.ServerStreamingServer[clockspb.AckResponse],
) error {
	return s.data.Range(func(key string, record *db.Record) error {
		if err := stream.Send(&clockspb.AckResponse{
			Key:   key,
			Clock: record.Clock.ToWireType(),
		}); err != nil {
			return fmt.Errorf("sending ack: %w", err)
		}
		return nil
	})
}
