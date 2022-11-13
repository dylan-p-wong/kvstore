package service

import (
	"context"
	"errors"
	"log"
	"sync"
	"time"

	pb "github.com/dylan-p-wong/kvstore/api"
	"go.uber.org/zap"
)

type State int

const (
	FOLLOWER State = 1 + iota
	CANDIDATE
	LEADER
	STOPPED
	INITIALIZED
)

const (
	DefaultHeartbeatInterval = 1000 * time.Millisecond
	DefaultElectionTimeout   = 150 * time.Millisecond
)

type RaftState struct {
	// State
	state State

	// Persistent state
	currentTerm int
	votedFor    int
	log         []*LogEntry
	// Volatile state
	commitIndex int
	lastApplied int
	// Volatile state (LEADERS)
	nextIndex  []int
	matchIndex []int
}

type Server struct {
	raftState RaftState
	pb.UnimplementedKVServer

	events       chan RPCRequest
	stopped      chan bool
	routineGroup sync.WaitGroup

	leader            int
	peers             map[int]*peer
	id                int
	url               string
	heartbeatInterval time.Duration
	electionTimeout   time.Duration

	sugar *zap.SugaredLogger
}

func NewServer(id int, url string, sugar *zap.SugaredLogger) *Server {

	state := RaftState{
		currentTerm: 0,
		votedFor:    -1,
		log:         make([]*LogEntry, 0),
		commitIndex: 0,
		lastApplied: 0,
		nextIndex:   make([]int, 0),
		matchIndex:  make([]int, 0),
	}

	s := &Server{
		raftState:         state,
		peers:             make(map[int]*peer),
		id:                id,
		url:               url,
		heartbeatInterval: DefaultHeartbeatInterval,
		electionTimeout:   DefaultElectionTimeout,

		sugar: sugar,
	}

	return s
}

func (s *Server) Init() error {
	s.sugar.Infow("server initialized")
	s.raftState.state = INITIALIZED
	return nil
}

func (s *Server) Start() error {
	s.sugar.Infow("server starting")
	err := s.Init()

	if err != nil {
		return err
	}

	s.stopped = make(chan bool)

	s.raftState.state = FOLLOWER

	s.routineGroup.Add(1)
	go func() {
		defer s.routineGroup.Done()
		s.loop()
	}()

	s.sugar.Infow("server started")
	return nil
}

func (s *Server) Put(ctx context.Context, in *pb.PutRequest) (*pb.PutResponse, error) {
	log.Printf("Received KV Pair: %s %s", in.GetKey(), in.GetValue())
	defer log.Printf("Finished Put")

	response, err := s.send(in)

	if err != nil {
		return nil, err
	}

	return response.(*pb.PutResponse), nil
}

func (s *Server) Get(ctx context.Context, in *pb.GetRequest) (*pb.GetResponse, error) {
	log.Printf("Received Key: %v", in.GetKey())
	defer log.Printf("Finished Get")

	return &pb.GetResponse{Success: true, Key: []byte(in.GetKey()), Value: []byte("?")}, nil
}

func (s *Server) AppendEntries(ctx context.Context, in *pb.AppendEntriesRequest) (*pb.AppendEntriesResponse, error) {
	log.Printf("AppendEntriesStart")
	defer log.Printf("AppendEntriesEnd")

	response, err := s.send(in)

	if err != nil {
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

func (s *Server) AddPeer(id int, url string) error {
	if s.peers[id] != nil {
		return errors.New("cannot add peer with the same id")
	}

	if s.id == id {
		return errors.New("cannot add peer with the same id")
	}

	peer, err := NewPeer(id, url, s.heartbeatInterval, s)

	if err != nil {
		return err
	}

	if s.raftState.state == LEADER {
		peer.StartHeartbeat()
	}

	s.peers[id] = peer

	return nil
}

func (s *Server) RemovePeer(id int) error {
	if id != s.id {
		return errors.New("cannot remove this server")
	}

	peer := s.peers[id]
	if peer == nil {
		return errors.New("cannot find this peer")
	}

	if s.raftState.state == LEADER {
		for _, p := range s.peers {
			p.StopHeartbeat(true)
		}
	}

	delete(s.peers, id)

	return nil
}
