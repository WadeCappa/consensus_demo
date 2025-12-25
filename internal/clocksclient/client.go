package clocksclient

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"time"

	"github.com/WadeCappa/consensus/gen/go/clocks/v1"
	"github.com/WadeCappa/consensus/internal/db"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/encoding/protojson"
)

type ClockClient struct {
	data   *db.Database
	secure bool
}

func NewClocksClient(db *db.Database, secure bool) *ClockClient {
	return &ClockClient{
		data:   db,
		secure: secure,
	}
}

func (s *ClockClient) SendDataWithRetry(ctx context.Context, hostname string) {
	backoffTicker := time.NewTicker(time.Second)
	defer backoffTicker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-backoffTicker.C:
		}
		fmt.Printf("starting send stream for server %s\n", hostname)
		if err := withConnection(hostname, s.secure, func(client clockspb.ClocksClient) error {
			return s.sendData(ctx, client)
		}); err != nil {
			fmt.Printf("Failed to stream data: %s\n", err.Error())
		}
	}
}

func (s *ClockClient) RunAcksWithRetry(ctx context.Context, hostname string) {
	backoffTicker := time.NewTicker(time.Second)
	defer backoffTicker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-backoffTicker.C:
		}
		fmt.Printf("starting ack stream for server %s\n", hostname)
		if err := withConnection(hostname, s.secure, func(client clockspb.ClocksClient) error {
			return s.sendAcks(ctx, client)
		}); err != nil {
			fmt.Printf("Failed to stream acks: %s\n", err.Error())
		}
	}
}

func (s *ClockClient) sendAcks(ctx context.Context, client clockspb.ClocksClient) error {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("acks context closed: %w", ctx.Err())
		case <-ticker.C:
		}

		stream, err := client.Ack(ctx, &clockspb.AckRequest{})
		if err != nil {
			return fmt.Errorf("opening acks stream: %w", err)
		}

		for {
			ack, err := stream.Recv()
			if err == io.EOF {
				break
			}
			if err != nil {
				return fmt.Errorf("receiving ack: %w", err)
			}
			// Just print for now, no logic to process these just yet
			fmt.Println(protojson.Format(ack))
		}
	}
}

func (s *ClockClient) sendData(ctx context.Context, client clockspb.ClocksClient) error {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("send data context closed: %w", ctx.Err())
		case <-ticker.C:
		}
		stream, err := client.Publish(ctx)
		if err != nil {
			return fmt.Errorf("opening publish stream: %w", err)
		}

		if err := s.data.Range(func(key string, record *db.Record) error {
			if err := stream.Send(&clockspb.PublishRequest{
				Key:    key,
				Chunks: db.ChunksToWireType(record.Chunks),
				Clock:  record.Clock.ToWireType(),
			}); err != nil {
				return fmt.Errorf("publishing next clock: %w", err)
			}
			return nil
		}); err != nil {
			return fmt.Errorf("iterating over data: %w", err)
		}
		response, err := stream.CloseAndRecv()
		if err != nil {
			return fmt.Errorf("receiving closing message: %w", err)
		}
		fmt.Println(protojson.Format(response))
	}
}

func withConnection(hostname string, secure bool, consumer func(clockspb.ClocksClient) error) error {
	var creds credentials.TransportCredentials
	if secure {
		creds = credentials.NewTLS(&tls.Config{})
	} else {
		creds = insecure.NewCredentials()
	}
	conn, err := grpc.NewClient(hostname, grpc.WithTransportCredentials(creds))
	if err != nil {
		return fmt.Errorf("connecting to grpc server: %w", err)
	}
	defer conn.Close()
	if err := consumer(clockspb.NewClocksClient(conn)); err != nil {
		return fmt.Errorf("consuming client: %w", err)
	}
	return nil
}
