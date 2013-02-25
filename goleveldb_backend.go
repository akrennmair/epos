package epos

import (
	"github.com/syndtr/goleveldb/leveldb"
	desc "github.com/syndtr/goleveldb/leveldb/descriptor"
	"github.com/syndtr/goleveldb/leveldb/opt"
)

type GoLevelDBStorageBackend struct {
	store *leveldb.DB
	ro    *opt.ReadOptions
	wo    *opt.WriteOptions
}

func NewGoLevelDBStorageBackend(path string) StorageBackend {
	desc, err := desc.OpenFile(path)
	if err != nil {
		panic(err)
	}

	db, err := leveldb.Open(desc, &opt.Options{Flag: opt.OFCreateIfMissing})
	if err != nil {
		panic(err)
	}

	ro := &opt.ReadOptions{}
	wo := &opt.WriteOptions{}

	return &GoLevelDBStorageBackend{store: db, ro: ro, wo: wo}
}

func (s *GoLevelDBStorageBackend) Read(key string) ([]byte, error) {
	return s.store.Get([]byte(key), s.ro)
}

func (s *GoLevelDBStorageBackend) Write(key string, value []byte) error {
	return s.store.Put([]byte(key), value, s.wo)
}

func (s *GoLevelDBStorageBackend) Erase(key string) error {
	return s.store.Delete([]byte(key), s.wo)
}

func (s *GoLevelDBStorageBackend) Keys() <-chan string {
	ch := make(chan string)

	go func() {
		iter := s.store.NewIterator(s.ro)
		for iter.Next() {
			ch <- string(iter.Key())
		}

		close(ch)
	}()

	return ch
}
