package main

import (
	"fmt"
	"log"
	"net"

	pb "github.com/dylan-p-wong/kvstore/api"
	"github.com/dylan-p-wong/kvstore/server/config"
	"github.com/dylan-p-wong/kvstore/server/service"
	"google.golang.org/grpc"
)

func main() {
	cfg, err := config.LoadConfig()
	if err != nil {
		log.Fatalf("failed to load config: %v", err)
	}

	listen, err := net.Listen("tcp", fmt.Sprintf("%s", cfg.URL))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	
	server := service.NewServer(cfg)
	s := grpc.NewServer()
	pb.RegisterKVServer(s, &server)

	log.Printf("server listening at %v", listen.Addr())
	if err := s.Serve(listen); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
