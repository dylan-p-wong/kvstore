package service

import (
	"context"
	"log"

	pb "github.com/dylan-p-wong/kvstore/api"
)

type Server struct {
	pb.UnimplementedKVServer
}

func (s *Server) Put(ctx context.Context, in *pb.PutRequest) (*pb.PutResponse, error) {
	log.Printf("Received KV Pair: %v %v", in.GetKey(), in.GetValue())
	return &pb.PutResponse{Success: true}, nil
}

func (s *Server) Get(ctx context.Context, in *pb.GetRequest) (*pb.GetResponse, error) {
	log.Printf("Received Key: %v", in.GetKey())
	return &pb.GetResponse{Success: true, Key: []byte(in.GetKey()), Value: []byte("?")}, nil
}

func (s *Server) AppendEntries(ctx context.Context, in *pb.AppendEntriesRequest) (*pb.AppendEntriesResponse, error) {

	return &pb.AppendEntriesResponse{
		Term:    0,
		Success: true,
	}, nil
}

func (s *Server) RequestVote(ctx context.Context, in *pb.RequestVoteRequest) (*pb.RequestVoteResponse, error) {

	return &pb.RequestVoteResponse{
		Term:        0,
		VoteGranted: true,
	}, nil
}
