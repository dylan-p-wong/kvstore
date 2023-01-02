package service

import (
	"time"

	"github.com/dylan-p-wong/kvstore/server/storage/kv"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/test/bufconn"

	pb "github.com/dylan-p-wong/kvstore/api"
)

const (
	DefaultTestHeartbeatInterval         = 10 * time.Millisecond
	DefaultTestElectionTimeoutLowerBound = 40 * time.Millisecond
	DefaultTestElectionTimeoutUpperBound = 50 * time.Millisecond
)

const bufSize = 1024 * 1024

func NewTestServer(id int) *server {
	url := ""

	state := raftState{
		state:       INITIALIZED,
		leader:      -1,
		currentTerm: 0,
		votedFor:    -1,
		log:         make([]*LogEntry, 0),
		commitIndex: 0,
		lastApplied: 0,
		nextIndex:   make(map[int]int),
		matchIndex:  make(map[int]int),
	}

	raftStorage := kv.NewMemory()
	logStorage := kv.NewMemory()

	s := &server{
		events: make(chan EventRequest),

		raftState:                 state,
		peers:                     make(map[int]*peer),
		id:                        id,
		url:                       url,
		heartbeatInterval:         DefaultTestHeartbeatInterval,
		electionTimeoutLowerBound: DefaultTestElectionTimeoutLowerBound,
		electionTimeoutUpperBound: DefaultTestElectionTimeoutUpperBound,

		sugar: zap.NewNop().Sugar(),

		raftStorage: raftStorage,
		logStorage:  logStorage,
	}

	return s
}

func (s *server) StartTestServer() error {
	s.StartEventLoop()

	listen := bufconn.Listen(bufSize)

	grpcServer := grpc.NewServer()
	pb.RegisterKVServer(grpcServer, s)

	// attach grpcServer to our server
	s.grpcServer = grpcServer

	err := grpcServer.Serve(listen)
	if err != nil {
		return err
	}

	return nil
}
