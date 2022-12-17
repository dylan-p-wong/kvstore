package service

import (
	"encoding/json"
	"sort"
	"strconv"
)

const (
	_currentTermKey = "current_term"
	_votedForKey    = "voted_for"
)

func (s *server) restoreFromStorage() error {
	entry, err := s.raftStorage.Get(_currentTermKey)
	if err != nil {
		return err
	}

	if entry != nil {
		i, err := strconv.Atoi(entry.Value)
		if err != nil {
			return err
		}
		s.raftState.currentTerm = i
	}

	entry, err = s.raftStorage.Get(_votedForKey)
	if err != nil {
		return err
	}
	if entry != nil {
		i, err := strconv.Atoi(entry.Value)
		if err != nil {
			return err
		}
		s.raftState.votedFor = i
	}

	entries, err := s.logStorage.GetAll()

	if err != nil {
		return err
	}

	for _, storageLogEntry := range entries {
		var encoded *EncodedEntry
		err = json.Unmarshal([]byte(storageLogEntry.Value), &encoded)
		if err != nil {
			return err
		}

		command, err := encodeCommand(storageLogEntry.Key, encoded.Value)
		if err != nil {
			return err
		}
		
		le := newLogEntry(encoded.Term, encoded.Index, command, make(chan EventResponse))
		s.raftState.log = append(s.raftState.log, &le)
	}

	// TODO: not ideal but put in for correctness
	sort.Slice(s.raftState.log, func(i int, j int) bool {
		return s.raftState.log[i].index < s.raftState.log[j].index
	})

	if len(s.raftState.log) > 0 {
		// we know these are the highest index applied
		s.raftState.commitIndex = s.raftState.log[len(s.raftState.log) - 1].index
		s.raftState.lastApplied = s.raftState.log[len(s.raftState.log) - 1].index
	}

	return nil
}

func (s *server) persistCurrentTerm() error {
	err := s.raftStorage.Set(_currentTermKey, strconv.Itoa(s.raftState.votedFor))
	s.sugar.Infow("persisted current term", "error", err)
	return err
}

func (s *server) persistVotedFor() error {
	err := s.raftStorage.Set(_votedForKey, strconv.Itoa(s.raftState.votedFor))
	s.sugar.Infow("persisted voted for", "error", err)
	return err
}

type EncodedEntry struct {
	Value string
	Index int
	Term  int
}

func (s *server) persistLogEntry(le *LogEntry) error {
	key, value, err := decodeCommand(le.command)

	encoded := &EncodedEntry{Value: value, Index: le.index, Term: le.term }

	encodedBytes, err := json.Marshal(encoded)

	if err != nil {
		return err
	}

	err = s.logStorage.Set(key, string(encodedBytes))
	s.sugar.Infow("persisted log entry", "error", err)
	return err
}
