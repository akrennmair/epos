package epos

import (
	levigo "github.com/jmhodges/levigo"
)

type LevelDBStorageBackend struct {
	store *levigo.DB
	ro *levigo.ReadOptions
	wo *levigo.WriteOptions
}

func NewLevelDBStorageBackend(db *Database, name string) StorageBackend {
	opts := levigo.NewOptions()
	opts.SetCreateIfMissing(true)

	leveldb := &LevelDBStorageBackend{
		ro: levigo.NewReadOptions(),
		wo: levigo.NewWriteOptions(),
	}

	store, err := levigo.Open(db.path + "/colls/" + name, opts)
	if err != nil {
		panic(err) // TODO: improve this
	}
	leveldb.store = store

	leveldb.ro.SetFillCache(false)

	return leveldb
}

func (s *LevelDBStorageBackend) Read(key string) ([]byte, error) {
	return s.store.Get(s.ro, []byte(key))
}

func (s *LevelDBStorageBackend) Write(key string, value []byte) error {
	return s.store.Put(s.wo, []byte(key), value)
}

func (s *LevelDBStorageBackend) Erase(key string) error {
	return s.store.Delete(s.wo, []byte(key))
}

func (s *LevelDBStorageBackend) Keys() <-chan string {
	ch := make(chan string)

	go func() {
		it := s.store.NewIterator(s.ro)
		defer it.Close()

		for it.SeekToFirst(); it.Valid(); it.Next() {
			ch <- string(it.Key())
		}

		close(ch)
	}()

	return ch
}
