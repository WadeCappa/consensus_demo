package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"net"
	"strings"

	"github.com/WadeCappa/consensus/gen/go/clocks/v1"
	"github.com/WadeCappa/consensus/internal/clocksclient"
	"github.com/WadeCappa/consensus/internal/clockserver"
	"github.com/WadeCappa/consensus/internal/db"
	"github.com/WadeCappa/consensus/internal/kvserver"
	"github.com/WadeCappa/consensus/pkg/go/kvstore/v1"
	"google.golang.org/grpc"
)

var (
	port   = flag.Int("port", 3100, "The server port")
	secure = flag.Bool("secure", false, "Set this flag if we connect to remote servers with TLS")
	servers  []string
)

func main() {
	flag.Func("servers", "the hostnames for servers that this node should drive consensus with. Deliminate this list with ','", func(s string) error {
		servers = strings.Split(s, ",")
		return nil
	})
	flag.Parse()
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", *port))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	s := grpc.NewServer()

	fmt.Printf("listening to %s\n", servers)

	serverId := rand.Uint64()
	db := db.NewDatabase(serverId)

	kvServer := kvserver.NewKvServer(db)
	kvstorepb.RegisterKvstoreServer(s, kvServer)

	clockServer := clockserver.NewClockServer(db)
	clockspb.RegisterClocksServer(s, clockServer)

	client := clocksclient.NewClocksClient(db, *secure)
	for _, server := range servers {
		go client.RunAcksWithRetry(context.Background(), server)
		go client.SendDataWithRetry(context.Background(), server)
	}

	log.Printf("server listening at %v", lis.Addr())
	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
