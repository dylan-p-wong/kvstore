package service

import (
	"testing"

	"github.com/stretchr/testify/assert"

	pb "github.com/dylan-p-wong/kvstore/api"
)

func TestProcessPutRequest(t *testing.T) {
	tests := []struct {
		name  string
		peers map[int]*peer

		currentTerm int
		commitIndex int
		matchIndex  map[int]int
		nextIndex   map[int]int
		log         []*LogEntry

		request         *pb.PutRequest
		responseChannel chan EventResponse

		expectedCommitIndex int
		expectedMatchIndex  map[int]int
		expectedNextIndex   map[int]int
		expectedLog         []*LogEntry
	}{
		{
			name:        "success with peers",
			peers:       map[int]*peer{0: nil, 1: nil, 2: nil},
			currentTerm: 1,
			commitIndex: 0,
			matchIndex:  map[int]int{0: 0, 1: 0, 2: 0},
			nextIndex:   map[int]int{0: 1, 1: 1, 2: 1},
			log:         []*LogEntry{},
			request: &pb.PutRequest{
				Key:   []byte("key"),
				Value: []byte("value"),
			},
			responseChannel:     nil,
			expectedCommitIndex: 0,
			expectedMatchIndex:  map[int]int{0: 1, 1: 0, 2: 0},
			expectedNextIndex:   map[int]int{0: 2, 1: 1, 2: 1},
			expectedLog:         []*LogEntry{{term: 1, index: 1, command: "{\"Key\":\"key\",\"Value\":\"value\"}", responseChannel: nil}},
		},
		{
			name:        "success with no peers",
			peers:       map[int]*peer{},
			currentTerm: 1,
			commitIndex: 0,
			matchIndex:  map[int]int{0: 0},
			nextIndex:   map[int]int{0: 1},
			log:         []*LogEntry{},
			request: &pb.PutRequest{
				Key:   []byte("key"),
				Value: []byte("value"),
			},
			responseChannel:     nil,
			expectedCommitIndex: 1,
			expectedMatchIndex:  map[int]int{0: 1},
			expectedNextIndex:   map[int]int{0: 2},
			expectedLog:         []*LogEntry{{term: 1, index: 1, command: "{\"Key\":\"key\",\"Value\":\"value\"}", responseChannel: nil}},
		},
	}

	for _, tt := range tests {
		s := NewTestServer(0)
		s.peers = tt.peers

		s.raftState.currentTerm = tt.currentTerm
		s.raftState.commitIndex = tt.commitIndex
		s.raftState.matchIndex = tt.matchIndex
		s.raftState.nextIndex = tt.nextIndex
		s.raftState.log = tt.log

		// copy before since nothing else should change
		expectedRaftState := s.raftState

		s.processPutRequest(tt.request, tt.responseChannel)

		// set expected changed fields
		expectedRaftState.commitIndex = tt.expectedCommitIndex
		expectedRaftState.matchIndex = tt.expectedMatchIndex
		expectedRaftState.nextIndex = tt.expectedNextIndex
		expectedRaftState.log = tt.expectedLog

		assert.Equal(t, expectedRaftState, s.raftState)
	}
}

func TestHandleAllServerRequestResponseRules(t *testing.T) {
	tests := []struct {
		currentTerm     int
		currentState    stateType
		currentLeader   int
		currentVotedFor int

		term     int
		serverId int

		expectedTerm     int
		expectedState    stateType
		expectedLeader   int
		expectedVotedFor int
	}{
		{
			currentTerm:     0,
			currentState:    LEADER,
			currentLeader:   0,
			currentVotedFor: 0,

			term:     1,
			serverId: 1,

			expectedTerm:     1,
			expectedState:    FOLLOWER,
			expectedLeader:   1,
			expectedVotedFor: -1,
		},
		{
			currentTerm:     0,
			currentState:    CANDIDATE,
			currentLeader:   0,
			currentVotedFor: 0,

			term:     1,
			serverId: 1,

			expectedTerm:     1,
			expectedState:    FOLLOWER,
			expectedLeader:   1,
			expectedVotedFor: -1,
		},
		{
			currentTerm:     0,
			currentState:    LEADER,
			currentLeader:   0,
			currentVotedFor: 0,

			term:     0,
			serverId: 0,

			expectedTerm:     0,
			expectedState:    LEADER,
			expectedLeader:   0,
			expectedVotedFor: 0,
		},
	}

	for _, tt := range tests {
		s := NewTestServer(0)

		// set server state
		s.raftState.currentTerm = tt.currentTerm
		s.raftState.state = tt.currentState
		s.raftState.leader = tt.currentLeader
		s.raftState.votedFor = tt.currentVotedFor

		s.handleAllServerRequestResponseRules(tt.term, tt.serverId)

		assert.Equal(t, tt.expectedTerm, s.raftState.currentTerm)
		assert.Equal(t, tt.expectedState, s.raftState.state)
		assert.Equal(t, tt.expectedLeader, s.raftState.leader)
		assert.Equal(t, tt.expectedVotedFor, s.raftState.votedFor)
	}
}

func TestCandidateUpToDate(t *testing.T) {
	tests := []struct {
		log                   []*LogEntry
		candidateLastLogTerm  uint64
		candidateLastLogIndex uint64
		expected              bool
	}{
		{
			log:                   make([]*LogEntry, 0),
			candidateLastLogTerm:  0,
			candidateLastLogIndex: 0,
			expected:              true,
		},
		{
			log:                   []*LogEntry{{term: 1, index: 0}},
			candidateLastLogTerm:  0,
			candidateLastLogIndex: 0,
			expected:              false,
		},
		{
			log:                   []*LogEntry{{term: 1, index: 1}},
			candidateLastLogTerm:  1,
			candidateLastLogIndex: 1,
			expected:              true,
		},
		{
			log:                   []*LogEntry{{term: 1, index: 1}, {term: 1, index: 2}},
			candidateLastLogTerm:  1,
			candidateLastLogIndex: 1,
			expected:              false,
		},
	}

	for _, tt := range tests {
		s := NewTestServer(0)
		// set the server log
		s.raftState.log = tt.log

		assert.Equal(t, tt.expected, s.candidateUpToDate(tt.candidateLastLogTerm, tt.candidateLastLogIndex))
	}
}

func TestProcessRequestVoteRequest(t *testing.T) {
	tests := []struct {
		name string

		currentTerm int
		votedFor    int
		log         []*LogEntry

		request *pb.RequestVoteRequest

		expectedVotedFor      int
		expectedEventResponse EventResponse
	}{
		{
			name:        "term < currentTerm",
			currentTerm: 1,
			votedFor:    -1,
			log:         []*LogEntry{},
			request: &pb.RequestVoteRequest{
				Term:         0,
				CandidateId:  1,
				LastLogIndex: 0,
				LastLogTerm:  0,
			},
			expectedVotedFor: -1,
			expectedEventResponse: EventResponse{
				Response: &pb.RequestVoteResponse{
					Term:        1,
					VoteGranted: false,
				},
				Error: nil,
			},
		},
		{
			name:        "voted for is NOT -1 and NOT candidate id",
			currentTerm: 1,
			votedFor:    100,
			log:         []*LogEntry{},
			request: &pb.RequestVoteRequest{
				Term:         1,
				CandidateId:  1,
				LastLogIndex: 0,
				LastLogTerm:  0,
			},
			expectedVotedFor: 100,
			expectedEventResponse: EventResponse{
				Response: &pb.RequestVoteResponse{
					Term:        1,
					VoteGranted: false,
				},
				Error: nil,
			},
		},
		{
			name:        "candidate log is NOT up to date",
			currentTerm: 1,
			votedFor:    100,
			log:         []*LogEntry{{term: 1, index: 1}},
			request: &pb.RequestVoteRequest{
				Term:         1,
				CandidateId:  100,
				LastLogIndex: 0,
				LastLogTerm:  0,
			},
			expectedVotedFor: 100,
			expectedEventResponse: EventResponse{
				Response: &pb.RequestVoteResponse{
					Term:        1,
					VoteGranted: false,
				},
				Error: nil,
			},
		},
		{
			name:        "voted for is -1",
			currentTerm: 1,
			votedFor:    -1,
			log:         []*LogEntry{},
			request: &pb.RequestVoteRequest{
				Term:         1,
				CandidateId:  100,
				LastLogIndex: 0,
				LastLogTerm:  0,
			},
			expectedVotedFor: 100,
			expectedEventResponse: EventResponse{
				Response: &pb.RequestVoteResponse{
					Term:        1,
					VoteGranted: true,
				},
				Error: nil,
			},
		},
		{
			name:        "voted for is candidate id",
			currentTerm: 1,
			votedFor:    100,
			log:         []*LogEntry{{term: 1, index: 1}},
			request: &pb.RequestVoteRequest{
				Term:         1,
				CandidateId:  100,
				LastLogIndex: 2,
				LastLogTerm:  1,
			},
			expectedVotedFor: 100,
			expectedEventResponse: EventResponse{
				Response: &pb.RequestVoteResponse{
					Term:        1,
					VoteGranted: true,
				},
				Error: nil,
			},
		},
	}

	for _, tt := range tests {
		s := NewTestServer(0)
		s.raftState.currentTerm = tt.currentTerm
		s.raftState.votedFor = tt.votedFor
		s.raftState.log = tt.log

		// copy before since nothing else should change
		expectedRaftState := s.raftState

		eventResponse := s.processRequestVoteRequest(tt.request)

		// set expected changed fields
		expectedRaftState.votedFor = tt.expectedVotedFor
		assert.Equal(t, expectedRaftState, s.raftState)

		assert.Equal(t, tt.expectedEventResponse, eventResponse)
	}
}

func TestProcessRequestVoteResponse(t *testing.T) {
	tests := []struct {
		name string

		currentTerm int

		response *pb.RequestVoteResponse

		expected bool
	}{
		{
			name:        "vote not granted",
			currentTerm: 1,
			response: &pb.RequestVoteResponse{
				Term:        1,
				VoteGranted: false,
			},
			expected: false,
		},
		{
			name:        "not the same term",
			currentTerm: 2,
			response: &pb.RequestVoteResponse{
				Term:        1,
				VoteGranted: true,
			},
			expected: false,
		},
		{
			name:        "returns true",
			currentTerm: 2,
			response: &pb.RequestVoteResponse{
				Term:        2,
				VoteGranted: true,
			},
			expected: true,
		},
	}

	for _, tt := range tests {
		s := NewTestServer(0)
		s.raftState.currentTerm = tt.currentTerm

		res := s.processRequestVoteResponse(tt.response)
		assert.Equal(t, tt.expected, res)
	}
}

func TestProcessAppendEntriesRequest(t *testing.T) {
	tests := []struct {
		name string

		currentTerm int
		commitIndex int
		log         []*LogEntry

		request *pb.AppendEntriesRequest

		expectedEventResponse EventResponse

		expectedCurrentTerm int
		expectedCommitIndex int
		expectedLastApplied int
		expectedLog         []*LogEntry
	}{
		{
			name:        "request.Term < currentTerm",
			currentTerm: 2,
			commitIndex: 100,
			log:         []*LogEntry{},
			request: &pb.AppendEntriesRequest{
				Term:         1,
				LeaderId:     100,
				PrevLogIndex: 100,
				PrevLogTerm:  100,
				Entries:      []*pb.LogEntry{},
				LeaderCommit: 100,
			},
			expectedEventResponse: EventResponse{
				Response: &pb.AppendEntriesResponse{
					Term:         2,
					Success:      false,
					ServerId:     0,
					PrevLogIndex: 100,
					Entries:      []*pb.LogEntry{},
				},
				Error: nil,
			},
			expectedCurrentTerm: 2,
			expectedCommitIndex: 100,
			expectedLastApplied: 0,
			expectedLog:         []*LogEntry{},
		},
		{
			name:        "log does not contain an entry at prevLogIndex whose term matches prevLogTerm (log is empty)",
			currentTerm: 2,
			commitIndex: 100,
			log:         []*LogEntry{},
			request: &pb.AppendEntriesRequest{
				Term:         2,
				LeaderId:     100,
				PrevLogIndex: 100,
				PrevLogTerm:  100,
				Entries:      []*pb.LogEntry{},
				LeaderCommit: 100,
			},
			expectedEventResponse: EventResponse{
				Response: &pb.AppendEntriesResponse{
					Term:         2,
					Success:      false,
					ServerId:     0,
					PrevLogIndex: 100,
					Entries:      []*pb.LogEntry{},
				},
				Error: nil,
			},
			expectedCurrentTerm: 2,
			expectedCommitIndex: 100,
			expectedLastApplied: 0,
			expectedLog:         []*LogEntry{},
		},
		{
			name:        "log does not contain an entry at prevLogIndex whose term matches prevLogTerm (log contains entries)",
			currentTerm: 2,
			commitIndex: 100,
			log:         []*LogEntry{{term: 1, index: 1}},
			request: &pb.AppendEntriesRequest{
				Term:         2,
				LeaderId:     100,
				PrevLogIndex: 1,
				PrevLogTerm:  2,
				Entries:      []*pb.LogEntry{},
				LeaderCommit: 100,
			},
			expectedEventResponse: EventResponse{
				Response: &pb.AppendEntriesResponse{
					Term:         2,
					Success:      false,
					ServerId:     0,
					PrevLogIndex: 1,
					Entries:      []*pb.LogEntry{},
				},
				Error: nil,
			},
			expectedCurrentTerm: 2,
			expectedCommitIndex: 100,
			expectedLastApplied: 0,
			expectedLog:         []*LogEntry{{term: 1, index: 1}},
		},
		{
			name:        "entry conflicts",
			currentTerm: 2,
			commitIndex: 0,
			log:         []*LogEntry{{term: 1, index: 1}, {term: 1, index: 2}},
			request: &pb.AppendEntriesRequest{
				Term:         2,
				LeaderId:     100,
				PrevLogIndex: 1,
				PrevLogTerm:  1,
				Entries:      []*pb.LogEntry{{Term: 2, Index: 2}},
				LeaderCommit: 0,
			},
			expectedEventResponse: EventResponse{
				Response: &pb.AppendEntriesResponse{
					Term:         2,
					Success:      true,
					ServerId:     0,
					PrevLogIndex: 1,
					Entries:      []*pb.LogEntry{{Term: 2, Index: 2}},
				},
				Error: nil,
			},
			expectedCurrentTerm: 2,
			expectedCommitIndex: 0,
			expectedLastApplied: 0,
			expectedLog:         []*LogEntry{{term: 1, index: 1}, {term: 2, index: 2}},
		},
		{
			name:        "append any new entries not already in log",
			currentTerm: 2,
			commitIndex: 0,
			log:         []*LogEntry{{term: 1, index: 1}, {term: 1, index: 2}},
			request: &pb.AppendEntriesRequest{
				Term:         2,
				LeaderId:     100,
				PrevLogIndex: 1,
				PrevLogTerm:  1,
				Entries:      []*pb.LogEntry{{Term: 2, Index: 2}, {Term: 2, Index: 3}},
				LeaderCommit: 0,
			},
			expectedEventResponse: EventResponse{
				Response: &pb.AppendEntriesResponse{
					Term:         2,
					Success:      true,
					ServerId:     0,
					PrevLogIndex: 1,
					Entries:      []*pb.LogEntry{{Term: 2, Index: 2}, {Term: 2, Index: 3}},
				},
				Error: nil,
			},
			expectedCurrentTerm: 2,
			expectedCommitIndex: 0,
			expectedLastApplied: 0,
			expectedLog:         []*LogEntry{{term: 1, index: 1}, {term: 2, index: 2}, {term: 2, index: 3}},
		},
		{
			name:        "leaderCommit > commitIndex and lastApplied < commitIndex",
			currentTerm: 2,
			commitIndex: -1,
			log:         []*LogEntry{{term: 1, index: 1}, {term: 1, index: 2}},
			request: &pb.AppendEntriesRequest{
				Term:         2,
				LeaderId:     100,
				PrevLogIndex: 1,
				PrevLogTerm:  1,
				Entries:      []*pb.LogEntry{{Term: 2, Index: 2}, {Term: 2, Index: 3}},
				LeaderCommit: 33,
			},
			expectedEventResponse: EventResponse{
				Response: &pb.AppendEntriesResponse{
					Term:         2,
					Success:      true,
					ServerId:     0,
					PrevLogIndex: 1,
					Entries:      []*pb.LogEntry{{Term: 2, Index: 2}, {Term: 2, Index: 3}},
				},
				Error: nil,
			},
			expectedCurrentTerm: 2,
			expectedCommitIndex: 3, // length of log
			expectedLastApplied: 3,
			expectedLog:         []*LogEntry{{term: 1, index: 1}, {term: 2, index: 2}, {term: 2, index: 3}},
		},
	}

	for _, tt := range tests {
		s := NewTestServer(0)
		s.raftState.currentTerm = tt.currentTerm
		s.raftState.log = tt.log
		s.raftState.commitIndex = tt.commitIndex

		// copy before since nothing else should change
		expectedRaftState := s.raftState

		eventResponse := s.processAppendEntriesRequest(tt.request)

		// set expected changed fields
		expectedRaftState.currentTerm = tt.expectedCurrentTerm
		expectedRaftState.log = tt.expectedLog
		expectedRaftState.commitIndex = tt.expectedCommitIndex
		expectedRaftState.lastApplied = tt.expectedLastApplied
		assert.Equal(t, expectedRaftState, s.raftState)

		assert.Equal(t, tt.expectedEventResponse, eventResponse)
	}
}

func TestProcessAppendEntriesResponse(t *testing.T) {
	tests := []struct {
		name string

		peers map[int]*peer

		state       stateType
		currentTerm int
		commitIndex int
		lastApplied int
		log         []*LogEntry
		matchIndex  map[int]int
		nextIndex   map[int]int

		response *pb.AppendEntriesResponse

		expectedEventResponse EventResponse

		expectedCommitIndex int
		expectedLastApplied int
		expectedMatchIndex  map[int]int
		expectedNextIndex   map[int]int
	}{
		{
			name:        "append entries failed",
			peers:       map[int]*peer{0: nil, 1: nil, 2: nil},
			state:       LEADER,
			currentTerm: 1,
			commitIndex: 10,
			lastApplied: 10,
			log:         []*LogEntry{},
			matchIndex:  map[int]int{0: 15, 1: 0, 2: 0},
			nextIndex:   map[int]int{0: 15, 1: 13, 2: 13},
			response: &pb.AppendEntriesResponse{
				Term:         0,
				Success:      false,
				ServerId:     1,
				PrevLogIndex: 100,
				Entries:      []*pb.LogEntry{},
			},
			expectedEventResponse: EventResponse{
				Error: ErrUnsuccessfulAppendEntries,
			},
			expectedCommitIndex: 10,
			expectedLastApplied: 10,
			expectedMatchIndex:  map[int]int{0: 15, 1: 0, 2: 0},
			expectedNextIndex:   map[int]int{0: 15, 1: 12, 2: 13},
		},
		{
			name:        "append entries success without commitedIndex increase",
			peers:       map[int]*peer{1: nil, 2: nil, 3: nil, 4: nil},
			state:       LEADER,
			currentTerm: 1,
			commitIndex: 0,
			lastApplied: 0,
			log:         []*LogEntry{},
			matchIndex:  map[int]int{0: 15, 1: 0, 2: 0, 3: 0, 4: 0},
			nextIndex:   map[int]int{0: 15, 1: 10, 2: 10, 3: 10, 4: 10},
			response: &pb.AppendEntriesResponse{
				Term:         1,
				Success:      true,
				ServerId:     1,
				PrevLogIndex: 10,
				Entries:      []*pb.LogEntry{{Term: 1, Index: 11}},
			},
			expectedEventResponse: EventResponse{
				Error: nil,
			},
			expectedCommitIndex: 0,
			expectedLastApplied: 0,
			expectedMatchIndex:  map[int]int{0: 15, 1: 11, 2: 0, 3: 0, 4: 0},
			expectedNextIndex:   map[int]int{0: 15, 1: 12, 2: 10, 3: 10, 4: 10},
		},
		{
			name:        "append entries success without commitedIndex increase (entries of size 1)",
			peers:       map[int]*peer{0: nil, 1: nil, 2: nil},
			state:       LEADER,
			currentTerm: 1,
			commitIndex: 0,
			lastApplied: 0,
			log:         []*LogEntry{{term: 1, index: 1, command: "", responseChannel: nil}},
			matchIndex:  map[int]int{0: 1, 1: 0, 2: 0},
			nextIndex:   map[int]int{0: 2, 1: 0, 2: 0},
			response: &pb.AppendEntriesResponse{
				Term:         0,
				Success:      true,
				ServerId:     1,
				PrevLogIndex: 0,
				Entries:      []*pb.LogEntry{{Term: 1, Index: 1}},
			},
			expectedEventResponse: EventResponse{
				Error: nil,
			},
			expectedCommitIndex: 1,
			expectedLastApplied: 1,
			expectedMatchIndex:  map[int]int{0: 1, 1: 1, 2: 0},
			expectedNextIndex:   map[int]int{0: 2, 1: 2, 2: 0},
		},
		{
			name:        "append entries success without commitedIndex increase (entries of size 2)",
			peers:       map[int]*peer{0: nil, 1: nil, 2: nil},
			state:       LEADER,
			currentTerm: 1,
			commitIndex: 0,
			lastApplied: 0,
			log:         []*LogEntry{{term: 1, index: 1, command: "", responseChannel: nil}, {term: 1, index: 2, command: "", responseChannel: nil}},
			matchIndex:  map[int]int{0: 2, 1: 0, 2: 0},
			nextIndex:   map[int]int{0: 3, 1: 0, 2: 0},
			response: &pb.AppendEntriesResponse{
				Term:         0,
				Success:      true,
				ServerId:     1,
				PrevLogIndex: 0,
				Entries:      []*pb.LogEntry{{Term: 1, Index: 1}, {Term: 1, Index: 2}},
			},
			expectedEventResponse: EventResponse{
				Error: nil,
			},
			expectedCommitIndex: 2,
			expectedLastApplied: 2,
			expectedMatchIndex:  map[int]int{0: 2, 1: 2, 2: 0},
			expectedNextIndex:   map[int]int{0: 3, 1: 3, 2: 0},
		},
	}

	for _, tt := range tests {
		s := NewTestServer(0)
		s.peers = tt.peers

		s.raftState.state = tt.state
		s.raftState.currentTerm = tt.currentTerm
		s.raftState.commitIndex = tt.commitIndex
		s.raftState.lastApplied = tt.lastApplied
		s.raftState.log = tt.log
		s.raftState.matchIndex = tt.matchIndex
		s.raftState.nextIndex = tt.nextIndex

		// copy before since nothing else should change
		expectedRaftState := s.raftState

		eventResponse := s.processAppendEntriesResponse(tt.response)

		// set expected changed fields
		expectedRaftState.commitIndex = tt.expectedCommitIndex
		expectedRaftState.lastApplied = tt.expectedLastApplied
		expectedRaftState.matchIndex = tt.expectedMatchIndex
		expectedRaftState.nextIndex = tt.expectedNextIndex

		assert.Equal(t, expectedRaftState, s.raftState)

		assert.Equal(t, tt.expectedEventResponse, eventResponse)
	}
}
