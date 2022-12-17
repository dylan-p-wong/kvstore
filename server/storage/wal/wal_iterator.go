package wal

import (
	"encoding/binary"
	"errors"
	"io"
	"os"
	"time"
)

var ErrNotEnoughBytesRead = errors.New("not enough bytes read")
var ErrInvalidValueRead = errors.New("invalid value read from file")

type WALIterator struct {
	file *os.File
}

func (wal *WAL) NewIterator() (*WALIterator, error) {
	_, err := wal.file.Seek(0, 0)

	if err != nil {
		return nil, err
	}

	return &WALIterator{
		file: wal.file,
	}, nil
}

func (iter *WALIterator) Next() (*WALEntry, error) {
	lenOfKeyInBytes := make([]byte, 8)
	n, err := iter.file.Read(lenOfKeyInBytes)

	if err != nil {
		if err == io.EOF {
			return nil, nil
		}
		return nil, err
	}
	if n != 8 {
		return nil, ErrNotEnoughBytesRead
	}
	keyLength := binary.LittleEndian.Uint64(lenOfKeyInBytes)

	typeOfEntry := make([]byte, 1)
	n, err = iter.file.Read(typeOfEntry)
	if err != nil {
		return nil, err
	}
	if n != 1 {
		return nil, ErrNotEnoughBytesRead
	}

	if typeOfEntry[0] == 0 {
		lenOfValueInBytes := make([]byte, 8)
		n, err = iter.file.Read(lenOfValueInBytes)
		if err != nil {
			return nil, err
		}
		if n != 8 {
			return nil, ErrNotEnoughBytesRead
		}
		valueLength := binary.LittleEndian.Uint64(lenOfValueInBytes)

		keyInBytes := make([]byte, keyLength)
		n, err = iter.file.Read(keyInBytes)
		if err != nil {
			return nil, err
		}
		if n != int(keyLength) {
			return nil, ErrNotEnoughBytesRead
		}

		valueInBytes := make([]byte, valueLength)
		n, err = iter.file.Read(valueInBytes)
		if err != nil {
			return nil, err
		}
		if n != int(valueLength) {
			return nil, ErrNotEnoughBytesRead
		}

		timestampInBytes := make([]byte, 8)
		n, err = iter.file.Read(timestampInBytes)
		if err != nil {
			return nil, err
		}
		if n != 8 {
			return nil, ErrNotEnoughBytesRead
		}
		timestampUnixMicro := binary.LittleEndian.Uint64(timestampInBytes)

		return &WALEntry{
			Key: string(keyInBytes),
			Value: string(valueInBytes),
			Deleted: false,
			Timestamp: time.UnixMicro(int64(timestampUnixMicro)),
		}, nil
	} else if typeOfEntry[0] == 1 {
		keyInBytes := make([]byte, keyLength)
		n, err = iter.file.Read(keyInBytes)
		if err != nil {
			return nil, err
		}
		if n != int(keyLength) {
			return nil, ErrNotEnoughBytesRead
		}

		timestampInBytes := make([]byte, 8)
		n, err = iter.file.Read(timestampInBytes)
		if err != nil {
			return nil, err
		}
		if n != 8 {
			return nil, ErrNotEnoughBytesRead
		}
		timestampUnixMicro := binary.LittleEndian.Uint64(timestampInBytes)

		return &WALEntry{
			Key: string(keyInBytes),
			Deleted: true,
			Timestamp: time.UnixMicro(int64(timestampUnixMicro)),
		}, nil
	} else {
		return nil, ErrInvalidValueRead
	}
}
