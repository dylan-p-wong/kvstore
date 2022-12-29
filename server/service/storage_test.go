package service

import (
	"os"
	"testing"

	"github.com/dylan-p-wong/kvstore/server/storage/kv"
	"github.com/stretchr/testify/assert"
)

func TestRestoreFromStorage(t *testing.T) {
	tests := []struct {
		setup               func(s *server)
		expectedCurrentTerm int
		expectedVotedFor    int
		expectedLog         []*LogEntry
		expectedMatchIndex  map[int]int
		expectedCommitIndex int
		expectedLastApplied int
	}{
		{
			setup: func(s *server) {
				s.raftState.currentTerm = 9
				s.persistCurrentTerm()
				s.raftState.votedFor = 11
				s.persistVotedFor()
				s.persistLogEntry(&LogEntry{term: 1, index: 1, command: "{\"Key\":\"key\",\"Value\":\"value\"}"})
			},
			expectedCurrentTerm: 9,
			expectedVotedFor:    11,
			expectedLog:         []*LogEntry{{term: 1, index: 1, command: "{\"Key\":\"key\",\"Value\":\"value\"}"}},
			expectedCommitIndex: 1,
			expectedLastApplied: 1,
		},
	}

	for _, tt := range tests {
		func() {
			s := NewTestServer(0)
			directory, err := os.MkdirTemp("", "kvtesting")
			assert.NoError(t, err)
			defer os.RemoveAll(directory)
			logStorage, err := kv.New(directory)
			assert.NoError(t, err)
			s.logStorage = logStorage

			tt.setup(s)

			// copy before since nothing else should change
			expectedRaftState := s.raftState
			expectedRaftState.currentTerm = tt.expectedCurrentTerm
			expectedRaftState.votedFor = tt.expectedVotedFor
			expectedRaftState.log = tt.expectedLog
			expectedRaftState.commitIndex = tt.expectedCommitIndex
			expectedRaftState.lastApplied = tt.expectedLastApplied

			s.restoreFromStorage()

			assert.Equal(t, expectedRaftState, s.raftState)
		}()
	}
}
