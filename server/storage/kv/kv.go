package kv

import (
	"time"

	"github.com/dylan-p-wong/kvstore/server/storage/memtable"
	"github.com/dylan-p-wong/kvstore/server/storage/wal"
)

type StorageEntry struct {
	Key       string
	Value     string
	Timestamp time.Time
}

type KV struct {
	wal      *wal.WAL
	memTable *memtable.MemTable
}

func New(dir string) (*KV, error) {
	// load wal from dir
	wal, err := wal.New(dir)

	if err != nil {
		return nil, err
	}

	// load memtable
	memTable, err := wal.LoadMemtable()

	if err != nil {
		return nil, err
	}

	return &KV{wal: wal, memTable: memTable}, nil
}

func (kv *KV) Set(key string, value string) error {
	timestamp := time.Now()

	err := kv.wal.Set(key, value, timestamp)

	if err != nil {
		return err
	}

	kv.memTable.Set(key, value, timestamp)

	return nil
}

func (kv *KV) Delete(key string) error {
	timestamp := time.Now()

	err := kv.wal.Delete(key, timestamp)

	if err != nil {
		return err
	}

	kv.memTable.Delete(key, timestamp)

	return nil
}

func (kv *KV) Get(key string) (*StorageEntry, error) {
	memtableEntry := kv.memTable.Get(key)

	if memtableEntry != nil {
		if memtableEntry.Deleted {
			return nil, nil
		}

		return &StorageEntry{
			Key:       memtableEntry.Key,
			Value:     memtableEntry.Value,
			Timestamp: memtableEntry.Timestamp,
		}, nil
	}

	// TODO: now must search disk if we do not find in memtable
	return nil, nil
}

func (kv *KV) GetAll() ([]StorageEntry, error) {
	storageEntries :=  make([]StorageEntry, 0)

	for _, entry := range kv.memTable.GetEntries() {
		storageEntries = append(storageEntries, StorageEntry{
			Key: entry.Key,
			Value: entry.Value,
			Timestamp: entry.Timestamp,
		})
	}

	return storageEntries, nil
}
