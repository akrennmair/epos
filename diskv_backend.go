package epos

import (
	"fmt"
	"github.com/peterbourgon/diskv"
)

type DiskvStorageBackend struct {
	store *diskv.Diskv
}

func transformFunc(s string) []string {
	// special case for internal data
	if s == "_next_id" {
		return []string{}
	}

	data := ""
	if len(s) < 4 {
		data = fmt.Sprintf("%04s", s)
	} else {
		data = s[len(s)-4:]
	}

	return []string{data[2:4], data[0:2]}
}

func NewDiskvStorageBackend(path string) StorageBackend {
	diskv := &DiskvStorageBackend{
		store: diskv.New(diskv.Options{
			BasePath:     path,
			Transform:    transformFunc,
			CacheSizeMax: 0, // no cache
		}),
	}

	return diskv
}

func (s *DiskvStorageBackend) Read(key string) ([]byte, error) {
	return s.store.Read(key)
}

func (s *DiskvStorageBackend) Write(key string, value []byte) error {
	return s.store.Write(key, value)
}

func (s *DiskvStorageBackend) Erase(key string) error {
	return s.store.Erase(key)
}

func (s *DiskvStorageBackend) Keys() <-chan string {
	return s.store.Keys()
}
