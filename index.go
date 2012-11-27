package epos

import (
	"bytes"
	"encoding/binary"
	"errors"
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
		entry_list := append(entry_list, e)
		idx.data[e.value] = entry_list
	} else {
		idx.data[e.value] = []indexEntry{e}
	}
}

func (e *indexEntry) Deleted() bool {
	return e.deleted
}

func (e *indexEntry) ReadFrom(r io.Reader) (n int64, err error) {
	var size uint32
	if err = binary.Read(r, binary.BigEndian, &size); err != nil {
		return 0, err
	}
	alldata := make([]byte, size)
	m, err := r.Read(alldata)
	if err != nil {
		return 0, err
	}
	if m != int(size) {
		return 0, errors.New("unable read index record")
	}

	e.deleted = (alldata[0] != 0)
	value_len := alldata[1:5]

	var decoded_value_len uint32
	if err = binary.Read(bytes.NewBuffer(value_len), binary.BigEndian, &decoded_value_len); err != nil {
		return 0, err
	}

	e.value = string(alldata[5:5+decoded_value_len])
	id := alldata[5+decoded_value_len:]

	var decoded_id int64
	if err = binary.Read(bytes.NewBuffer(id), binary.BigEndian, &decoded_id); err != nil {
		return 0, err
	}
	e.id = decoded_id

	return int64(int(size) + binary.Size(size)), nil
}

func (e *indexEntry) WriteTo(w io.Writer) (n int64, err error) {
	value_len := uint32(len(e.value))
	size := uint32(binary.Size(e.deleted) + binary.Size(value_len) + int(value_len) + binary.Size(e.id))
	if err = binary.Write(w, binary.BigEndian, size); err != nil {
		return 0, err
	}
	deleted := byte(0)
	if e.deleted {
		deleted = 1
	}
		
	if err = binary.Write(w, binary.BigEndian, deleted); err != nil {
		return 0, err
	}

	if err = binary.Write(w, binary.BigEndian, value_len); err != nil {
		return 0, err
	}

	if err = binary.Write(w, binary.BigEndian, e.value); err != nil {
		return 0, err
	}

	if err = binary.Write(w, binary.BigEndian, e.id); err != nil {
		return 0, err
	}

	return int64(int(size) + binary.Size(size)), nil
}
