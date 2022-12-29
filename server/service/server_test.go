package service

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGetMajority(t *testing.T) {
	tests := []struct {
		peers    map[int]*peer
		expected int
	}{
		{
			peers:    map[int]*peer{},
			expected: 1,
		},
		{
			peers:    map[int]*peer{1: nil},
			expected: 2,
		},
		{
			peers:    map[int]*peer{1: nil, 2: nil},
			expected: 2,
		},
		{
			peers:    map[int]*peer{1: nil, 2: nil, 3: nil},
			expected: 3,
		},
		{
			peers:    map[int]*peer{1: nil, 2: nil, 3: nil, 4: nil},
			expected: 3,
		},
	}

	for _, tt := range tests {
		s := NewTestServer(0)
		s.peers = tt.peers

		assert.Equal(t, tt.expected, s.getMajority())
	}
}
