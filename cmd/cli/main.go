package main

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"

	consensuspb "github.com/WadeCappa/consensus/pkg/go/consensus/v1"
	"github.com/alecthomas/kong"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/encoding/protojson"
)

type result struct {
	Version uint64 `json:"version"`
	Data    string `json:"data"`
}

type Conn struct {
	Hostname string `help:"specify the server's hostname" default:"localhost:3100"`
	Secure   bool   `help:"toggle if this connection routes through TLS"`
}

type Get struct {
	Key string `arg:"" name:"key" help:"Key to retreive" type:"string"`
	Conn
}

type Put struct {
	Key     string `arg:"" name:"key" help:"Key to retreive" type:"string"`
	Version uint64 `arg:"" name:"version" help:"Version of element to put" type:"uint64"`
	Data    string `arg:"" name:"data" help:"The data to put into the key-value store"`
	Conn
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
	return withClient(cmd.Conn.Hostname, cmd.Conn.Secure, func(client consensuspb.ConsensusClient) error {
		response, err := client.Get(ctx, &consensuspb.GetRequest{
			Key: cmd.Key,
		})
		if err != nil {
			return err
		}
		result := &result{
			Version: response.Version,
			Data:    string(response.Data),
		}
		stringResults, err := json.Marshal(result)
		if err != nil {
			return fmt.Errorf("marshaling result string: %w", err)
		}
		fmt.Println(string(stringResults))
		return nil
	})
}

func (cmd *Put) Run() error {
	ctx := context.Background()
	return withClient(cmd.Conn.Hostname, cmd.Conn.Secure, func(client consensuspb.ConsensusClient) error {
		response, err := client.Put(ctx, &consensuspb.PutRequest{
			Key:     cmd.Key,
			Version: cmd.Version,
			Data:    []byte(cmd.Data),
		})
		if err != nil {
			return err
		}
		fmt.Println(protojson.Format(response))
		return nil
	})
}

func withClient(
	hostname string,
	secure bool,
	consumer func(client consensuspb.ConsensusClient) error,
) error {
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
	return consumer(consensuspb.NewConsensusClient(conn))
}
