package memtable

import (
	"sort"
	"time"
)

type MemTableEntry struct {
	Key       string
	Value     string
	Timestamp time.Time
	Deleted   bool
}

type MemTable struct {
	entries []MemTableEntry
	size    uint64
}

func New() *MemTable {
	return &MemTable{
		entries: make([]MemTableEntry, 0),
		size:    0,
	}
}

// temporary method for keeping memtable sorted
func (mt *MemTable) sortEntries() {
	sort.Slice(mt.entries, func(i int, j int) bool {
		if mt.entries[i].Key == mt.entries[j].Key {
			return mt.entries[i].Timestamp.Before(mt.entries[j].Timestamp)
		}
		return mt.entries[i].Key < mt.entries[j].Key
	})
}

func (mt *MemTable) Set(key string, value string, timestamp time.Time) {
	// TODO: if new entry makes us over capacity we transform to SSTable and write to disk. We also wipe WAL

	// insert into entries array
	mt.entries = append(mt.entries, MemTableEntry{Key: key, Value: value, Timestamp: timestamp, Deleted: false})
	// TODO: temporary for simplicity
	mt.sortEntries()
}

func (mt *MemTable) Delete(key string, timestamp time.Time) {
	// if new entry makes us over capacity we transform to SSTable and write to disk. We also wipe WAL.

	// insert tombstone entry into entries array
	mt.entries = append(mt.entries, MemTableEntry{Key: key, Timestamp: timestamp, Deleted: true})
	// TODO: temporary for simplicity
	mt.sortEntries()
}

func (mt *MemTable) Get(key string) *MemTableEntry {
	// TODO: temporary for simplicity
	// we search from the back to front since higher timestamps will be towards the back
	for i := len(mt.entries) - 1; i >= 0; i-- {
		if mt.entries[i].Key == key {
			return &mt.entries[i]
		}
	}
	// not found
	return nil
}
