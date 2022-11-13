package service

import "errors"

type LogEntry struct {
	term  int
	key   string
	value string
	index int
}

func newLogEntry(term int, index int, key string, value string) LogEntry {
	return LogEntry{
		term:  term,
		key:   key,
		value: value,
		index: index,
	}
}

func (s *Server) getCurrentLogIndex() int {
	if len(s.raftState.log) == 0 {
		return 0
	}
	return s.raftState.log[len(s.raftState.log)-1].index
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
