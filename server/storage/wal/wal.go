package wal

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"time"

	"github.com/dylan-p-wong/kvstore/server/storage/memtable"
)

const _walFileName string = "wal.wal"

type WAL struct {
	directory string
	file *os.File
}

type WALEntry struct {
	Key       string
	Value     string
	Timestamp time.Time
	Deleted   bool
}

func New(directory string) (*WAL, error) {
	fname := filepath.Join(directory, _walFileName)

	file, err := os.OpenFile(fname, os.O_APPEND|os.O_RDWR|os.O_CREATE, 0666)

	if err != nil {
		return nil, err
	}

	return &WAL{
		directory: directory,
		file: file,
	}, nil
}

func (wal *WAL) LoadMemtable() *memtable.MemTable {
	mt := memtable.New()

 	walIter := wal.NewIterator()

	for {
		entry, err := walIter.Next()

		if err != nil || entry == nil {
			break;
		}

		mt.Set(entry.Key, entry.Value, entry.Timestamp)
	}

	return mt
}

func (wal *WAL) Set(key string, value string, timestamp time.Time) error {
	keyInBytes := []byte(key)
	valueInBytes := []byte(value)

	toWrite := make([]byte, 0)

	// write length of key
	lenOfKeyInBytes := make([]byte, 8)
    binary.LittleEndian.PutUint64(lenOfKeyInBytes, uint64(len(keyInBytes)))
	toWrite = append(toWrite, lenOfKeyInBytes...)
	
	// write 0
	toWrite = append(toWrite, 0)

	// write length of value
	lenOfValueInBytes := make([]byte, 8)
    binary.LittleEndian.PutUint64(lenOfValueInBytes, uint64(len(valueInBytes)))
	toWrite = append(toWrite, lenOfValueInBytes...)

	// write key
	toWrite = append(toWrite, keyInBytes...)

	// write value
	toWrite = append(toWrite, valueInBytes...)

	// write timestamp
	timestampInBytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(timestampInBytes, uint64(timestamp.UnixMicro()))
	toWrite = append(toWrite, timestampInBytes...)

	_, err := wal.file.Write(toWrite)

	return err
}

func (wal *WAL) Delete(key string, timestamp time.Time) error {
	keyInBytes := []byte(key)

	toWrite := make([]byte, 0)

	// write length of key
	lenOfKeyInBytes := make([]byte, 8)
    binary.LittleEndian.PutUint64(lenOfKeyInBytes, uint64(len(keyInBytes)))
	toWrite = append(toWrite, lenOfKeyInBytes...)

	// write 1 (signals tombstone)
	toWrite = append(toWrite, 1)

	// write key
	toWrite = append(toWrite, keyInBytes...)

	// write timestamp
	timestampInBytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(timestampInBytes, uint64(timestamp.UnixMicro()))
	toWrite = append(toWrite, timestampInBytes...)

	_, err := wal.file.Write(toWrite)

	return err
}
