package epos

import (
	"encoding/binary"
	"io"
	"os"
)

type index struct {
	file *os.File
	field string
	data map[string][]indexEntry
}

type indexEntry struct {
	deleted bool
	value string
	id int64
	fpos int64
}

func newIndex(file *os.File, field string) *index {
	idx := &index{file: file, field: field, data: make(map[string][]indexEntry)}
	return idx
}

func (idx *index) Add(e indexEntry) {
	if entry_list, contains := idx.data[e.value]; contains {
		entry_list = append(entry_list, e)
		idx.data[e.value] = entry_list
	} else {
		idx.data[e.value] = []indexEntry{e}
	}
}

func (e *indexEntry) Deleted() bool {
	return e.deleted
}

func (e *indexEntry) ReadFrom(r io.Reader) (n int64, err error) {
	var deleted byte
	if err = binary.Read(r, binary.BigEndian, &deleted); err != nil {
		return 0, err
	}
	e.deleted = (deleted != 0)

	var value_len uint32
	if err = binary.Read(r, binary.BigEndian, &value_len); err != nil {
		return 0, err
	}

	value := make([]byte, int(value_len))
	for i := 0; i<int(value_len); i++ {
		var b byte
		if err = binary.Read(r, binary.BigEndian, &b); err != nil {
			return 0, err
		}
		value[i] = b
	}
	e.value = string(value)

	var id int64
	if err = binary.Read(r, binary.BigEndian, &id); err != nil {
		return 0, err
	}
	e.id = id

	return int64(binary.Size(deleted) + binary.Size(value) + binary.Size(id)), nil
}

func (e *indexEntry) WriteTo(w io.Writer) (n int64, err error) {
	deleted := byte(0)
	if e.deleted {
		deleted = 1
	}
		
	if err = binary.Write(w, binary.BigEndian, deleted); err != nil {
		return 0, err
	}

	value_len := uint32(len(e.value))
	if err = binary.Write(w, binary.BigEndian, value_len); err != nil {
		return 0, err
	}

	if err = binary.Write(w, binary.BigEndian, []byte(e.value)); err != nil {
		return 0, err
	}

	if err = binary.Write(w, binary.BigEndian, e.id); err != nil {
		return 0, err
	}

	return int64(binary.Size(deleted) + binary.Size([]byte(e.value)) + binary.Size(e.id)), nil
}
