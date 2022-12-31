package service

import (
	"errors"
	"fmt"
	"math/rand"
	"net"
	"path/filepath"
	"sync"
	"time"

	pb "github.com/dylan-p-wong/kvstore/api"
	"github.com/dylan-p-wong/kvstore/server/storage/kv"
	"go.uber.org/zap"
	"google.golang.org/grpc"
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

	grpcServer *grpc.Server
}

func NewServer(id int, url string, dir string, sugar *zap.SugaredLogger) (*server, error) {

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

		raftState: state,

		peers: make(map[int]*peer),
		id:    id,
		url:   url,

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

// set state to follower and starts the event loop
func (s *server) StartEventLoop() error {
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

	// go s.monitorState()

	return nil
}

func (s *server) StartServer() error {
	s.StartEventLoop()

	listen, err := net.Listen("tcp", fmt.Sprintf("%s", s.url))
	if err != nil {
		return err
	}
	grpcServer := grpc.NewServer()
	pb.RegisterKVServer(grpcServer, s)

	// attach grpcServer to our server
	s.grpcServer = grpcServer

	err = grpcServer.Serve(listen)
	if err != nil {
		return err
	}

	return nil
}

func (s *server) Stop() {
	s.stopped <- true

	if s.grpcServer != nil {
		s.grpcServer.Stop()
	}
}

func (s *server) getElectionTimeout() time.Duration {
	r := rand.Intn(int(s.electionTimeoutUpperBound) - int(s.electionTimeoutLowerBound))
	return s.electionTimeoutLowerBound + time.Duration(r)
}

func (s *server) getMajority() int {
	return (len(s.peers)+1)/2 + 1
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
// func (s *server) monitorState() {
// 	ticker := time.NewTicker(2000 * time.Millisecond)
// 	defer ticker.Stop()

// 	for {
// 		select {
// 		case <-ticker.C:
// 			s.logState()
// 		}
// 	}
// }

// logging state helper
// func (s *server) logState() {
// 	for _, le := range s.raftState.log {
// 		s.sugar.Infow("server log", "command", le.command, "index", le.index, "term", le.term)
// 	}

// 	s.sugar.Infow("server state", "leader", s.raftState.leader, "state", s.raftState.state, "currentTerm", s.raftState.currentTerm, "votedFor", s.raftState.votedFor, "commitIndex", s.raftState.commitIndex, "lastApplied", s.raftState.lastApplied, "nextIndex", s.raftState.nextIndex, "matchIndex", s.raftState.matchIndex, "log", s.raftState.log)
// }
