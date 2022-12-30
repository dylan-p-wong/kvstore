package service

import (
	"context"
	"encoding/json"
	"errors"

	pb "github.com/dylan-p-wong/kvstore/api"
)

func (s *server) Put(ctx context.Context, in *pb.PutRequest) (*pb.PutResponse, error) {
	s.sugar.Infow("received PUT request", "request", in)

	// put requests must be sent to the leader. we help the client by return the current leader
	if s.raftState.state != LEADER {
		return &pb.PutResponse{Success: false, Leader: int64(s.raftState.leader)}, errors.New("not leader")
	}

	_, err := s.send(in)

	if err != nil {
		return &pb.PutResponse{Success: false}, err
	}

	return &pb.PutResponse{Success: true}, nil
}

func (s *server) Delete(ctx context.Context, in *pb.DeleteRequest) (*pb.DeleteResponse, error) {
	s.sugar.Infow("received DELETE request", "request", in)

	// delete requests must be sent to the leader. we help the client by return the current leader
	if s.raftState.state != LEADER {
		return &pb.DeleteResponse{Success: false, Leader: int64(s.raftState.leader)}, errors.New("not leader")
	}

	// we basiclly put with an empty key
	_, err := s.send(&pb.PutRequest{Key: in.GetKey(), Value: []byte("")})

	if err != nil {
		return &pb.DeleteResponse{Success: false}, err
	}

	return &pb.DeleteResponse{Success: true}, nil
}

func (s *server) Get(ctx context.Context, in *pb.GetRequest) (*pb.GetResponse, error) {

	// get requests must be sent to the leader. we help the client by return the current leader
	if s.raftState.state != LEADER {
		return &pb.GetResponse{Success: false, Leader: int64(s.raftState.leader)}, errors.New("not leader")
	}

	value, err := s.logStorage.Get(string(in.GetKey()))

	// there was an error getting from persistence
	if err != nil {
		return &pb.GetResponse{Success: false, Key: []byte(in.GetKey())}, err
	}

	// if there is no entry with this key in the persister
	if value == nil {
		return &pb.GetResponse{Success: true, Key: []byte(in.GetKey())}, errors.New("not found")
	}

	var encoded *EncodedEntry
	err = json.Unmarshal([]byte(value.Value), &encoded)

	if err != nil {
		return &pb.GetResponse{Success: false, Key: []byte(in.GetKey())}, err
	}

	if encoded.Value == "" {
		return &pb.GetResponse{Success: true, Key: []byte(in.GetKey())}, errors.New("not found")
	}

	return &pb.GetResponse{Success: true, Key: []byte(in.GetKey()), Value: []byte(encoded.Value)}, nil
}
