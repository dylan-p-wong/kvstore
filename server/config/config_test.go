package config

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParsePeers(t *testing.T) {
	tests := []struct {
		name          string
		peersString   string
		expected      map[int]string
		expectedError bool
	}{
		{
			name:          "bad string",
			peersString:   "",
			expected:      nil,
			expectedError: true,
		},
		{
			name:          "bad pid",
			peersString:   "asf=127.0.0.1:4444,1=127.0.0.1:14444",
			expected:      nil,
			expectedError: true,
		},
		{
			name:          "correct",
			peersString:   "0=127.0.0.1:4444,1=127.0.0.1:14444",
			expected:      map[int]string{0: "127.0.0.1:4444", 1: "127.0.0.1:14444"},
			expectedError: false,
		},
	}

	for _, tt := range tests {
		m, err := parsePeers(&tt.peersString)
		assert.Equal(t, err != nil, tt.expectedError)
		assert.True(t, reflect.DeepEqual(tt.expected, m))
	}
}
