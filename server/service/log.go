package service

import (
	"errors"
	"strings"
)

type LogEntry struct {
	term  int
	index int
	command string
	responseChannel chan EventResponse
}

func newLogEntry(term int, index int, command string, responseChannel chan EventResponse) LogEntry {
	return LogEntry{
		term:  term,
		index: index,
		command: command,
		responseChannel: responseChannel,
	}
}

func (s *server) appendLogEntry(entry LogEntry) error {
	s.sugar.Infow("appended log entry to log", "index", entry.index, "term", entry.term, "command", entry.command)

	if len(s.raftState.log) > 0 {
		lastEntry := s.raftState.log[len(s.raftState.log)-1]

		if entry.term < lastEntry.term {
			return errors.New("cannot append entry with earlier term")
		}

		if entry.term == lastEntry.term && entry.index <= lastEntry.index {
			return errors.New("cannot appended entry with eariler index in the same term")
		}
	}

	s.raftState.log = append(s.raftState.log, &entry)

	return nil
}

// log is 1-indexed
func (s *server) GetLastLogIndex() int {
	if len(s.raftState.log) == 0 {
		return 0
	}

	return len(s.raftState.log)
}

func (s *server) GetLastLogTerm() int {
	if len(s.raftState.log) == 0 {
		return 0
	}

	return s.raftState.log[len(s.raftState.log) - 1].term
}

func encodeCommand(key string, value string) (string, error) {
	return key + ":" + value, nil
}

func decodeCommand(command string) (string, string, error) {
	s := strings.Split(command, ":")

	if len(s) != 2 {
		return "", "", errors.New("invalid command")
	}

	return s[0], s[1], nil
}
