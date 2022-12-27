package service

import (
	"context"

	pb "github.com/dylan-p-wong/kvstore/api"
)

func (s *server) AppendEntries(ctx context.Context, in *pb.AppendEntriesRequest) (*pb.AppendEntriesResponse, error) {
	s.sugar.Infow("received append entries request", "request", in)
	defer s.sugar.Infow("responsed to append entries request", "request", in)

	response, err := s.send(in)

	if err != nil {
		return nil, err
	}

	return response.(*pb.AppendEntriesResponse), nil
}

func (s *server) RequestVote(ctx context.Context, in *pb.RequestVoteRequest) (*pb.RequestVoteResponse, error) {
	s.sugar.Infow("received request vote request", "request", in)
	defer s.sugar.Infow("responsed to request vote request", "request", in)

	response, err := s.send(in)

	if err != nil {
		return nil, err
	}

	return response.(*pb.RequestVoteResponse), nil
}
