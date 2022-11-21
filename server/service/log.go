package service

import "errors"

type LogEntry struct {
	term  int
	index int
	command string
	responseChannel chan RPCResponse
}

func newLogEntry(term int, index int, command string, responseChannel chan RPCResponse) LogEntry {
	return LogEntry{
		term:  term,
		index: index,
		command: command,
		responseChannel: responseChannel,
	}
}

func (s *Server) appendLogEntry(entry LogEntry) error {

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
func (s *Server) GetLastLogIndex() int {
	if len(s.raftState.log) == 0 {
		return 0
	}

	return len(s.raftState.log)
}
