package storage

import (
	"errors"
	"sync"
)

type Storage interface {
	Set(key string, value string) error
	Get(key string) (string, error)
}

type storage struct {
	mutex    sync.RWMutex
	memtable map[string]string
}

func New(dir string) *storage {
	return &storage{
		memtable: make(map[string]string),
	}
}

func (s *storage) Set(key string, value string) error {
	s.memtable[key] = value
	return nil
}

func (s *storage) Get(key string) (string, error) {
	value, ok := s.memtable[key]

	if !ok {
		return "", errors.New("not found")
	}

	return value, nil
}
