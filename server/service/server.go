package service

import (
	"context"
	"encoding/json"
	"errors"
	"math/rand"
	"path/filepath"
	"sync"
	"time"

	pb "github.com/dylan-p-wong/kvstore/api"
	"github.com/dylan-p-wong/kvstore/server/storage/kv"
	"go.uber.org/zap"
)

const (
	dataDirectory = "data"
)

type stateType int

const (
	FOLLOWER stateType = 1 + iota
	CANDIDATE
	LEADER
	STOPPED
	INITIALIZED
)

const (
	DefaultHeartbeatInterval         = 1000 * time.Millisecond
	DefaultElectionTimeoutLowerBound = 4000 * time.Millisecond
	DefaultElectionTimeoutUpperBound = 5000 * time.Millisecond
)

type raftState struct {
	// State (LEADER, FOLLOWER, CANDIDATE, etc)
	state  stateType
	leader int

	// Persistent state
	currentTerm int
	votedFor    int
	log         []*LogEntry
	// Volatile state
	// https://stackoverflow.com/questions/46376293/what-is-lastapplied-and-matchindex-in-raft-protocol-for-volatile-state-in-server
	// commitIndex is set then we apply to state machine then we update lastApplied
	commitIndex int
	lastApplied int
	// Volatile state (LEADERS)
	// https://stackoverflow.com/questions/46376293/what-is-lastapplied-and-matchindex-in-raft-protocol-for-volatile-state-in-server
	nextIndex  map[int]int
	matchIndex map[int]int
}

type RaftStorage interface {
	Get(key string) (*kv.StorageEntry, error)
	Set(key string, value string) error
}

type LogStorage interface {
	Get(key string) (*kv.StorageEntry, error)
	Set(key string, value string) error
	GetAll() ([]kv.StorageEntry, error)
}

type server struct {
	raftState raftState
	pb.UnimplementedKVServer

	events       chan EventRequest
	stopped      chan bool
	routineGroup sync.WaitGroup

	peers                     map[int]*peer
	id                        int
	url                       string
	heartbeatInterval         time.Duration
	electionTimeoutLowerBound time.Duration
	electionTimeoutUpperBound time.Duration

	sugar *zap.SugaredLogger

	raftStorage RaftStorage
	logStorage  LogStorage
}

func NewServer(id int, url string, dir string, sugar *zap.SugaredLogger) (*server, error) {

	state := raftState{
		currentTerm: 0,
		votedFor:    -1,
		log:         make([]*LogEntry, 0),
		commitIndex: 0,
		lastApplied: 0,
		nextIndex:   make(map[int]int),
		matchIndex:  make(map[int]int),
	}

	// initialize persistent storage
	raftStorage, err := kv.New(filepath.Join(dir, "raft"))
	if err != nil {
		return nil, err
	}

	logStorage, err := kv.New(filepath.Join(dir, "logs"))
	if err != nil {
		return nil, err
	}

	s := &server{
		events: make(chan EventRequest),

		raftState:                 state,
		peers:                     make(map[int]*peer),
		id:                        id,
		url:                       url,
		heartbeatInterval:         DefaultHeartbeatInterval,
		electionTimeoutLowerBound: DefaultElectionTimeoutLowerBound,
		electionTimeoutUpperBound: DefaultElectionTimeoutUpperBound,

		sugar: sugar,

		raftStorage: raftStorage,
		logStorage:  logStorage,
	}

	return s, nil
}

func (s *server) Init() error {
	defer s.sugar.Infow("server initialized")

	// restore from persistence storage
	err := s.restoreFromStorage()

	if err != nil {
		return err
	}

	// set state to initialized
	s.raftState.state = INITIALIZED

	return nil
}

func (s *server) Start() error {
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

	go s.monitorState()

	return nil
}

func (s *server) getElectionTimeout() time.Duration {
	r := rand.Intn(int(s.electionTimeoutUpperBound) - int(s.electionTimeoutLowerBound))
	return s.electionTimeoutLowerBound + time.Duration(r)
}

func (s *server) Put(ctx context.Context, in *pb.PutRequest) (*pb.PutResponse, error) {
	s.sugar.Infow("received PUT request", "request", in)

	// put requests must be sent to the leader. we help the client by return the current leader
	if s.raftState.state != LEADER {
		return &pb.PutResponse{Success: false, Leader: uint64(s.raftState.leader)}, errors.New("not leader")
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
		return &pb.DeleteResponse{Success: false, Leader: uint64(s.raftState.leader)}, errors.New("not leader")
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
		return &pb.GetResponse{Success: false, Leader: uint64(s.raftState.leader)}, errors.New("not leader")
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

func (s *server) AddPeer(id int, url string) error {
	if s.peers[id] != nil {
		return errors.New("cannot add peer with the same id")
	}

	if s.id == id {
		return errors.New("cannot add peer with the same id")
	}

	peer, err := newPeer(id, url, s.heartbeatInterval, s)

	if err != nil {
		return err
	}

	if s.raftState.state == LEADER {
		peer.startHeartbeat()
	}

	s.peers[id] = peer

	s.sugar.Infow("added peer", "id", id, "url", url)

	return nil
}

func (s *server) RemovePeer(id int) error {
	if id != s.id {
		return errors.New("cannot remove this server")
	}

	peer := s.peers[id]
	if peer == nil {
		return errors.New("cannot find this peer")
	}

	if s.raftState.state == LEADER {
		for _, p := range s.peers {
			p.stopHeartbeat(true)
		}
	}

	delete(s.peers, id)

	return nil
}

// monitoring state
func (s *server) monitorState() {
	ticker := time.NewTicker(2000 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			s.logState()
		}
	}
}

// logging state helper
func (s *server) logState() {
	s.sugar.Infow("server state", "commitIndex", s.raftState.commitIndex, "lastApplied", s.raftState.lastApplied, "nextIndex", s.raftState.nextIndex, "matchIndex", s.raftState.matchIndex, "log", s.raftState.log)
}
