package service

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestEncodeCommand(t *testing.T) {
	tests := []struct {
		key      string
		value    string
		expected *Command
	}{
		{
			key:      "test_key",
			value:    "test_value",
			expected: &Command{Key: "test_key", Value: "test_value"},
		},
		{
			key:      "",
			value:    "",
			expected: &Command{Key: "", Value: ""},
		},
	}

	for _, tt := range tests {
		expectedCommandBytes, err := json.Marshal(tt.expected)
		assert.NoError(t, err)
		expectedCommandString := string(expectedCommandBytes)
		commandString, err := encodeCommand(tt.key, tt.value)
		assert.Equal(t, expectedCommandString, commandString)
	}
}

func TestDecodeCommand(t *testing.T) {
	tests := []struct {
		expectedKey   string
		expectedValue string
		command       *Command
	}{
		{
			expectedKey:   "test_key",
			expectedValue: "test_value",
			command:       &Command{Key: "test_key", Value: "test_value"},
		},
		{
			expectedKey:   "",
			expectedValue: "",
			command:       &Command{Key: "", Value: ""},
		},
	}

	for _, tt := range tests {
		commandBytes, err := json.Marshal(tt.command)
		assert.NoError(t, err)
		commandString := string(commandBytes)
		key, value, err := decodeCommand(commandString)
		assert.NoError(t, err)
		assert.Equal(t, tt.expectedKey, key)
		assert.Equal(t, tt.expectedValue, value)
	}
}
