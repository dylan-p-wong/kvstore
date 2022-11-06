package service

import (
	"context"
	"fmt"
	"log"

	pb "github.com/dylan-p-wong/kvstore/api"
	"github.com/dylan-p-wong/kvstore/server/config"
)

type RPCResponse struct {
	Response interface{}
	Error    error
}

type RPCRequest struct {
	Command         interface{}
	ResponseChannel chan<- RPCResponse
}

type Server struct {
	cfg config.ServerConfig
	pb.UnimplementedKVServer
	t *Transport
}

func NewServer(cfg config.ServerConfig) Server {
	fmt.Println(cfg)

	t := NewTransport()
	go t.RPCWorker()

	s := Server{
		cfg: cfg,
		t:   &t,
	}

	return s
}

func (s *Server) handleRPC(command interface{}) (interface{}, error) {
	channel := make(chan RPCResponse, 1)

	rpc := RPCRequest{
		Command:         command,
		ResponseChannel: channel,
	}

	s.t.rpcChannel <- rpc

	response := <-channel

	if response.Error != nil {
		return nil, response.Error
	}

	return response.Response, nil
}

func (s *Server) Put(ctx context.Context, in *pb.PutRequest) (*pb.PutResponse, error) {
	log.Printf("Received KV Pair: %v %v", in.GetKey(), in.GetValue())
	defer log.Printf("Finished Put");

	for k, v := range s.cfg.Peers {
		c, err := s.t.GetPeer(k, v)

		if err != nil {
			return nil, err
		}

		_, err = c.AppendEntries(ctx, &pb.AppendEntriesRequest{
			Term:         1,
			LeaderId:     2,
			PrevLogIndex: 3,
			PrevLogTerm:  4,
			Entries:      make([][]byte, 0),
			LeaderCommit: 6,
		})

		if err != nil {
			return nil, err
		}
	}

	return &pb.PutResponse{Success: true}, nil
}

func (s *Server) Get(ctx context.Context, in *pb.GetRequest) (*pb.GetResponse, error) {
	log.Printf("Received Key: %v", in.GetKey())
	defer log.Printf("Finished Get");

	return &pb.GetResponse{Success: true, Key: []byte(in.GetKey()), Value: []byte("?")}, nil
}

func (s *Server) AppendEntries(ctx context.Context, in *pb.AppendEntriesRequest) (*pb.AppendEntriesResponse, error) {
	log.Printf("AppendEntriesStart")
	defer log.Printf("AppendEntriesEnd")

	response, err := s.handleRPC(in)

	if err != nil {
		fmt.Println(err.Error())
		return nil, err
	}


	return response.(*pb.AppendEntriesResponse), nil
}

func (s *Server) RequestVote(ctx context.Context, in *pb.RequestVoteRequest) (*pb.RequestVoteResponse, error) {
	log.Printf("RequestVote")
	defer log.Printf("RequestEnd")

	return &pb.RequestVoteResponse{
		Term:        0,
		VoteGranted: true,
	}, nil
}
