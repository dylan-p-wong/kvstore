package service

import (
	"context"
	"testing"
	"time"

	"github.com/dylan-p-wong/kvstore/api"
	"github.com/stretchr/testify/assert"
)

func TestPutRequest(t *testing.T) {
	tests := []struct {
		name             string
		request          *api.PutRequest
		expectedResponse *api.PutResponse
		expectedError    error

		expectedCommitIndex int
		expectedLastApplied int
		expectedLog         []*LogEntry
		expectedMatchIndex  map[int]int
		expectedNextIndex   map[int]int
	}{
		{
			name: "success",
			request: &api.PutRequest{
				Key:   []byte("key"),
				Value: []byte("value"),
			},
			expectedResponse: &api.PutResponse{
				Success: true,
				Leader:  0,
			},
			expectedError: nil,

			expectedCommitIndex: 1,
			expectedLastApplied: 1,
			expectedLog:         []*LogEntry{{term: 1, index: 1, command: "{\"Key\":\"key\",\"Value\":\"value\"}", responseChannel: nil}},
			expectedMatchIndex:  map[int]int{0: 1},
			expectedNextIndex:   map[int]int{0: 2},
		},
	}

	for _, tt := range tests {
		s := NewTestServer(0)
		go func() {
			// stop the server after 1000ms
			time.Sleep(1000 * time.Millisecond)
			s.Stop()
		}()
		go func() {
			// start testing after server has started
			time.Sleep(100 * time.Millisecond)
			// since we have no peers and default timeout of 50ms we should be a leader after 100ms
			assert.Equal(t, LEADER, s.raftState.state)

			// copy before since nothing else should change
			expectedRaftState := s.raftState

			response, err := s.Put(context.TODO(), tt.request)
			assert.Equal(t, tt.expectedResponse, response)
			assert.Equal(t, tt.expectedError, err)

			expectedRaftState.commitIndex = tt.expectedCommitIndex
			expectedRaftState.lastApplied = tt.expectedLastApplied
			expectedRaftState.matchIndex = tt.expectedMatchIndex
			expectedRaftState.nextIndex = tt.expectedNextIndex
			
			expectedRaftState.log = tt.expectedLog
			expectedRaftState.log[0].responseChannel = s.raftState.log[0].responseChannel

			assert.Equal(t, expectedRaftState, s.raftState)
		}()
		err := s.StartTestServer()
		assert.NoError(t, err)
	}
}
