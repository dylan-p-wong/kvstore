package service

import (
	"testing"

	"github.com/stretchr/testify/assert"

	pb "github.com/dylan-p-wong/kvstore/api"
)

// func TestProcessPutRequest(t *testing.T) {

// }

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

// func TestProcessAppendEntriesResponse(t *testing.T) {

// }
