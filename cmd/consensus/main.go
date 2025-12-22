package main

import (
	"flag"
	"fmt"
	"log"
	"net"

	"github.com/WadeCappa/consensus/internal/server"
	"github.com/WadeCappa/consensus/pkg/go/consensus/v1"
	"google.golang.org/grpc"
)

var (
	port = flag.Int("port", 3100, "The server port")
)

func main() {
	flag.Parse()
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", *port))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	s := grpc.NewServer()
	server := server.NewServer()
	consensuspb.RegisterConsensusServer(s, server)
	log.Printf("server listening at %v", lis.Addr())
	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
