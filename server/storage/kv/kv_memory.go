package kv

type KVMemory struct {
	entires []StorageEntry
}

func NewMemory() *KVMemory {
	return &KVMemory{
		entires: make([]StorageEntry, 0),
	}
}

func (kv *KVMemory) Set(key string, value string) error {
	// TODO
	return nil
}

func (kv *KVMemory) Delete(key string) error {
	// TODO
	return nil
}

func (kv *KVMemory) Get(key string) (*StorageEntry, error) {
	// TODO
	return nil, nil
}

func (kv *KVMemory) GetAll() ([]StorageEntry, error) {
	storageEntries := make([]StorageEntry, 0)
	// TODO
	return storageEntries, nil
}
