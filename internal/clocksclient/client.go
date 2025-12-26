package clocksclient

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/WadeCappa/consensus/gen/go/clocks/v1"
	"github.com/WadeCappa/consensus/internal/db"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

type ClockClient struct {
	data         *db.Database
	secure       bool
	remoteClocks *db.RemoteClocks
	delay        time.Duration
}

func NewClocksClient(data *db.Database, secure bool, delay time.Duration) *ClockClient {
	return &ClockClient{
		data:         data,
		secure:       secure,
		remoteClocks: db.NewRemoteClocks(),
		delay:        delay,
	}
}

func (s *ClockClient) SendDataWithRetry(ctx context.Context, hostname string) {
	remoteSystemId := getRemoteSystemId(hostname)
	backoffTicker := time.NewTicker(s.delay)
	defer backoffTicker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-backoffTicker.C:
		}
		fmt.Printf("starting send stream for server %s\n", hostname)
		if err := withConnection(hostname, s.secure, func(client clockspb.ClocksClient) error {
			return s.sendData(ctx, client, remoteSystemId)
		}); err != nil {
			fmt.Printf("Failed to stream data: %s\n", err.Error())
		}
	}
}

func (s *ClockClient) RunAcksWithRetry(ctx context.Context, hostname string) {
	remoteSystemId := getRemoteSystemId(hostname)
	backoffTicker := time.NewTicker(s.delay)
	defer backoffTicker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-backoffTicker.C:
		}
		fmt.Printf("starting ack stream for server %s\n", hostname)
		if err := withConnection(hostname, s.secure, func(client clockspb.ClocksClient) error {
			return s.getAcks(ctx, client, remoteSystemId)
		}); err != nil {
			fmt.Printf("Failed to stream acks: %s\n", err.Error())
		}
	}
}

func (s *ClockClient) getAcks(
	ctx context.Context,
	client clockspb.ClocksClient,
	remoteSystemId uint64,
) error {
	ticker := time.NewTicker(s.delay)
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
			s.remoteClocks.Accept(remoteSystemId, ack.GetKey(), db.FromWireType(ack.GetClock()))
		}
	}
}

func (s *ClockClient) sendData(
	ctx context.Context,
	client clockspb.ClocksClient,
	remoteSystemId uint64,
) error {
	ticker := time.NewTicker(s.delay)
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
			remoteClock := s.remoteClocks.Get(remoteSystemId, key)
			var chunksSince []*db.Chunk
			if remoteClock == nil {
				chunksSince = record.Chunks
			} else {
				chunksSince = record.GetChunksSince(remoteClock)
			}
			if len(chunksSince) == 0 {
				return nil
			}
			msg := &clockspb.PublishRequest{
				Key:    key,
				Chunks: db.ChunksToWireType(chunksSince),
				Clock:  record.Clock.ToWireType(),
			}
			if err := stream.Send(msg); err != nil {
				return fmt.Errorf("publishing next clock: %w", err)
			}
			return nil
		}); err != nil {
			return fmt.Errorf("iterating over data: %w", err)
		}
		_, err = stream.CloseAndRecv()
		if err != nil {
			return fmt.Errorf("receiving closing message: %w", err)
		}
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

func getRemoteSystemId(hostname string) uint64 {
	hostnameParts := strings.Split(hostname, ":")
	id, err := strconv.Atoi(hostnameParts[1])
	if err != nil {
		panic(err.Error())
	}
	remoteSystemId := uint64(id)
	return remoteSystemId
}
