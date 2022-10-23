package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"

	pb "github.com/dylan-p-wong/kvstore/api"
	"google.golang.org/grpc"
)

var (
	port = flag.Int("port", 50051, "The server port")
)

// server is used to implement api.KVServer.
type server struct {
	pb.UnimplementedKVServer
}

func (s *server) Put(ctx context.Context, in *pb.KeyValue) (*pb.KeyValue, error) {
	log.Printf("Received KV Pair: %v %v", in.GetKey(), in.GetValue())
	return &pb.KeyValue{Key: []byte(in.GetKey()), Value: []byte(in.GetValue())}, nil
}

func (s *server) Get(ctx context.Context, in *pb.Key) (*pb.KeyValue, error) {
	log.Printf("Received Key: %v", in.GetKey())
	return &pb.KeyValue{Key: []byte(in.GetKey()), Value: []byte("?")}, nil
}

func main() {
	flag.Parse()
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", *port))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	s := grpc.NewServer()
	pb.RegisterKVServer(s, &server{})
	log.Printf("server listening at %v", lis.Addr())
	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
