package service

import (
	"github.com/dylan-p-wong/kvstore/server/storage/kv"
	"go.uber.org/zap"
)

func NewTestServer(id int) *server {
	url := ""

	state := raftState{
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
		heartbeatInterval:         DefaultHeartbeatInterval,
		electionTimeoutLowerBound: DefaultElectionTimeoutLowerBound,
		electionTimeoutUpperBound: DefaultElectionTimeoutUpperBound,

		sugar: zap.NewNop().Sugar(),

		raftStorage: raftStorage,
		logStorage:  logStorage,
	}

	return s
}
