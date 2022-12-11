package service

import "strconv"

const (
	_currentTermKey = "current_term"
	_votedForKey = "voted_for"
)

func (s *server) restoreFromStorage() {
	_, err := s.storage.Get(_currentTermKey)
	if err != nil {

	}
	_, err = s.storage.Get(_votedForKey)
	if err != nil {

	}
	// TODO: load log. snapshots
}

func (s *server) persistCurrentTerm() error {
	err := s.storage.Set(_currentTermKey, strconv.Itoa(s.raftState.votedFor))
	return err
}

func (s *server) persistVotedFor() error {
	err := s.storage.Set(_votedForKey, strconv.Itoa(s.raftState.votedFor))
	return err
}
