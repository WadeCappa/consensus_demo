package main

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"time"

	"github.com/WadeCappa/consensus/pkg/go/kvstore/v1"
	"github.com/alecthomas/kong"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/encoding/protojson"
)

type result struct {
	Version   uint64    `json:"version"`
	Data      string    `json:"data"`
	NodeId    uint64    `json:"nodeId"`
	WriteTime time.Time `json:"writeTime"`
}

type Conn struct {
	Hostname string `help:"specify the server's hostname" default:"localhost:3100"`
	Secure   bool   `help:"toggle if this connection routes through TLS"`
}

type Get struct {
	Conn
	Key string `arg:"" name:"key" help:"Key to retreive" type:"string"`
}

type Put struct {
	Conn
	Key     string `arg:"" name:"key" help:"Key to retreive" type:"string"`
	Data    string `arg:"" name:"data" help:"The data to put into the key-value store"`
}

var cli struct {
	Get Get `cmd:"" help:"Get by key"`
	Put Put `cmd:"" help:"Put key if versions match"`
}

func main() {
	ctx := kong.Parse(&cli)
	err := ctx.Run()
	ctx.FatalIfErrorf(err)
}

func (cmd *Get) Run() error {
	ctx := context.Background()
	return withKvClient(cmd.Conn.Hostname, cmd.Conn.Secure, func(client kvstorepb.KvstoreClient) error {
		response, err := client.Get(ctx, &kvstorepb.GetRequest{
			Key: cmd.Key,
		})
		if err != nil {
			return err
		}
		for {
			response, err := response.Recv()
			if err == io.EOF {
				break
			}
			if err != nil {
				return fmt.Errorf("getting next response: %w", err)
			}
			result := &result{
				Data:      string(response.Data),
				WriteTime: time.UnixMilli(int64(response.WriteTimeUnixMillis)),
				Version:   response.Version,
				NodeId:    response.NodeId,
			}
			stringResults, err := json.Marshal(result)
			if err != nil {
				return fmt.Errorf("marshaling response json: %w", err)
			}
			fmt.Println(string(stringResults))
		}
		return nil
	})
}

func (cmd *Put) Run() error {
	ctx := context.Background()
	return withKvClient(cmd.Conn.Hostname, cmd.Conn.Secure, func(client kvstorepb.KvstoreClient) error {
		response, err := client.Put(ctx, &kvstorepb.PutRequest{
			Key:    cmd.Key,
			Update: []byte(cmd.Data),
		})
		if err != nil {
			return err
		}
		fmt.Println(protojson.Format(response))
		return nil
	})
}

func getGrpcClient(hostname string, secure bool) (*grpc.ClientConn, error) {
	var creds credentials.TransportCredentials
	if secure {
		creds = credentials.NewTLS(&tls.Config{})
	} else {
		creds = insecure.NewCredentials()
	}
	return grpc.NewClient(hostname, grpc.WithTransportCredentials(creds))
}

func withKvClient(
	hostname string,
	secure bool,
	consumer func(client kvstorepb.KvstoreClient) error,
) error {
	conn, err := getGrpcClient(hostname, secure)
	if err != nil {
		return fmt.Errorf("connecting to grpc server: %w", err)
	}
	defer conn.Close()
	return consumer(kvstorepb.NewKvstoreClient(conn))
}
