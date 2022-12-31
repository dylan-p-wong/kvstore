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
	}{
		{
			name: "not leader",
			request: &api.PutRequest{
				Key:   []byte("key"),
				Value: []byte("value"),
			},
			expectedResponse: &api.PutResponse{
				Success: false,
				Leader:  -1,
			},
			expectedError: ErrNotLeader,
		},
	}

	for _, tt := range tests {
		s := NewTestServer(0)
		go func() {
			time.Sleep(time.Millisecond * 100)
			s.Stop()
		}()
		err := s.StartTestServer()
		assert.NoError(t, err)

		response, err := s.Put(context.TODO(), tt.request)
		assert.Equal(t, tt.expectedResponse, response)
		assert.Equal(t, tt.expectedError, err)
	}
}
