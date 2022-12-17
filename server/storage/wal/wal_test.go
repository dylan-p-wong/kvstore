package wal

import (
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestWAL(t *testing.T) {
	directory, err := os.MkdirTemp("", "kvstore")
	assert.NoError(t, err)
	defer os.RemoveAll(directory)

	wal, err := New(directory)
	assert.NoError(t, err)

	err = wal.Set("key", "value", time.Now())
	assert.NoError(t, err)

	err = wal.Delete("key", time.Now())
	assert.NoError(t, err)
}

func TestWALIterator1(t *testing.T) {
	directory, err := os.MkdirTemp("", "kvstore")
	assert.NoError(t, err)
	defer os.RemoveAll(directory)

	wal, err := New(directory)
	assert.NoError(t, err)

	timestamp := time.Now()

	err = wal.Set("key1", "value1", timestamp)
	assert.NoError(t, err)

	iterator, err := wal.NewIterator()
	assert.NoError(t, err)

	entry, err := iterator.Next()
	assert.NoError(t, err)
	assert.Equal(t, "key1", entry.Key)
	assert.Equal(t, "value1", entry.Value)
	assert.Equal(t, false, entry.Deleted)
	assert.Equal(t, timestamp.UnixMicro(), entry.Timestamp.UnixMicro())

	entry, err = iterator.Next()
	assert.NoError(t, err)
	assert.Nil(t, entry)
}

func TestWALIterator2(t *testing.T) {
	directory, err := os.MkdirTemp("", "kvstore")
	assert.NoError(t, err)
	defer os.RemoveAll(directory)

	wal, err := New(directory)
	assert.NoError(t, err)

	timestamp := time.Now()

	err = wal.Delete("key1", timestamp)
	assert.NoError(t, err)

	iterator, err := wal.NewIterator()
	assert.NoError(t, err)

	entry, err := iterator.Next()
	assert.NoError(t, err)
	assert.Equal(t, "key1", entry.Key)
	assert.Equal(t, "", entry.Value)
	assert.Equal(t, true, entry.Deleted)
	assert.Equal(t, timestamp.UnixMicro(), entry.Timestamp.UnixMicro())

	entry, err = iterator.Next()
	assert.NoError(t, err)
	assert.Nil(t, entry)
}

func TestWALIterator3(t *testing.T) {
	directory, err := os.MkdirTemp("", "kvstore")
	assert.NoError(t, err)
	defer os.RemoveAll(directory)

	wal, err := New(directory)
	assert.NoError(t, err)

	timestamp := time.Now()

	err = wal.Set("key1", "value1", timestamp)
	assert.NoError(t, err)

	err = wal.Delete("key2", timestamp)
	assert.NoError(t, err)

	err = wal.Delete("key3", timestamp)
	assert.NoError(t, err)

	iterator, err := wal.NewIterator()
	assert.NoError(t, err)

	entry, err := iterator.Next()
	assert.NoError(t, err)
	assert.Equal(t, "key1", entry.Key)
	assert.Equal(t, "value1", entry.Value)
	assert.Equal(t, false, entry.Deleted)
	assert.Equal(t, timestamp.UnixMicro(), entry.Timestamp.UnixMicro())

	entry, err = iterator.Next()
	assert.NoError(t, err)
	assert.Equal(t, "key2", entry.Key)
	assert.Equal(t, "", entry.Value)
	assert.Equal(t, true, entry.Deleted)
	assert.Equal(t, timestamp.UnixMicro(), entry.Timestamp.UnixMicro())

	entry, err = iterator.Next()
	assert.NoError(t, err)
	assert.Equal(t, "key3", entry.Key)
	assert.Equal(t, "", entry.Value)
	assert.Equal(t, true, entry.Deleted)
	assert.Equal(t, timestamp.UnixMicro(), entry.Timestamp.UnixMicro())

	entry, err = iterator.Next()
	assert.NoError(t, err)
	assert.Nil(t, entry)
}
