package epos

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/peterbourgon/diskv"
	"io"
	"log"
	"os"
	"path/filepath"
	"strconv"
)

type Collection struct {
	store     *diskv.Diskv
	indexpath string
	indexes   map[string]*index
}

type Id int64

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

func (db *Database) openColl(name string) *Collection {
	// create/open collection
	coll := &Collection{store: diskv.New(diskv.Options{
		BasePath:     db.path + "/colls/" + name,
		Transform:    transformFunc,
		CacheSizeMax: 0, // no cache
	}), indexpath: db.path + "/indexes/" + name, indexes: make(map[string]*index)}

	os.Mkdir(coll.indexpath, 0755)

	coll.loadIndexes()

	// if _next_id is unset, then set it to 1.
	if _, err := coll.store.Read("_next_id"); err != nil {
		coll.setNextId(Id(1))
	}
	return coll
}

func (c *Collection) loadIndexes() {
	filepath.Walk(c.indexpath, func(path string, info os.FileInfo, err error) error {
		if (info.Mode() & os.ModeType) == 0 {
			if err := c.loadIndex(path, filepath.Base(path)); err != nil {
				log.Printf("loadIndex %s failed: %v", path, err)
				// TODO: should we maybe remove or rebuild index?
			}
		}
		return nil
	})
}

func (c *Collection) loadIndex(filepath, field string) error {
	file, err := os.OpenFile(filepath, os.O_RDWR, 0644)
	if err != nil {
		return err
	}

	idx := newIndex(file, field)

	for {
		fpos, _ := file.Seek(0, os.SEEK_CUR)
		var entry indexEntry
		_, err = entry.ReadFrom(file)
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		if !entry.Deleted() {
			entry.fpos = fpos
			idx.Add(entry)
		}
	}

	c.indexes[field] = idx
	return nil
}

func (c *Collection) setNextId(next_id Id) {
	next_id_buf := make([]byte, binary.MaxVarintLen64)
	length := binary.PutVarint(next_id_buf, int64(next_id))
	c.store.Write("_next_id", next_id_buf[:length])
}

func (c *Collection) getNextId() Id {
	data, _ := c.store.Read("_next_id")
	next_id, _ := binary.Varint(data)
	c.setNextId(Id(next_id + 1))
	return Id(next_id)
}

// Insert inserts an object into the collection. It returns the object's
// ID and, if the insert fails, a non-nil error describing the problem.
func (c *Collection) Insert(value interface{}) (Id, error) {
	jsondata, err := json.Marshal(value)
	if err != nil {
		return Id(0), err
	}

	id := c.getNextId()
	id_str := fmt.Sprintf("%d", id)
	err = c.store.Write(id_str, jsondata)
	if err != nil {
		c.setNextId(id) // roll back generated ID
		return Id(0), err
	}

	if err = c.addToIndexes(id, jsondata); err != nil {
		c.store.Erase(id_str)
		return Id(0), err
	}
	return id, nil
}

// Update replaces an existing object with a new object. If an error
// occurs during that operation, it returns a non-nil error.
func (c *Collection) Update(id Id, value interface{}) error {
	jsondata, err := json.Marshal(value)
	if err != nil {
		return err
	}

	if err = c.store.Write(fmt.Sprintf("%d", id), jsondata); err != nil {
		return err
	}

	c.removeFromIndexes(id)

	if err = c.addToIndexes(id, jsondata); err != nil {
		return err
	}
	return nil
}

func (c *Collection) addToIndexes(id Id, jsondata []byte) error {
	var value2 map[string]interface{}
	// no error means that we can unmarshal it into a map.
	if err := json.Unmarshal(jsondata, &value2); err == nil {
		for field, idx := range c.indexes {
			if v, contains := value2[field]; contains {
				entry := indexEntry{deleted: false, value: fmt.Sprintf("%v", v), id: int64(id)}
				fpos, _ := idx.file.Seek(0, os.SEEK_END)
				if _, err = entry.WriteTo(idx.file); err != nil {
					return err
				}
				idx.file.Sync()
				entry.fpos = fpos
				idx.Add(entry)
			}
		}
	}
	return nil
}

// AddIndex creates an index for a field. Existing records will be indexed, and 
// future insert and update operations will index that field, as well. If an 
// index for that field already exists, the AddIndex() is a no-op.
//
// A field describes a top-level element of a struct or a particular key of a map.
func (c *Collection) AddIndex(field string) error {
	filepath := c.indexpath + "/" + field

	file, err := os.OpenFile(filepath, os.O_WRONLY | os.O_CREATE | os.O_EXCL, 0644)
	if err != nil {
		// if we couldn't open the file because it already exists, then AddIndex is a no-op.
		if os.IsExist(err) {
			return nil
		}
		return err
	}

	idx := newIndex(file, field)

	for id_str := range c.store.Keys() {
		id, err := strconv.ParseInt(id_str, 10, 64)
		if err != nil {
			log.Printf("AddIndex: skipping key %s", id_str)
			continue
		}

		var entry map[string]interface{}
		data, err := c.store.Read(id_str)
		if err != nil {
			log.Printf("AddIndex: skipping key %s because read from store failed: %v", id_str, err)
			continue
		}

		if err = json.Unmarshal(data, &entry); err != nil {
			log.Printf("AddIndex: skipping key %s because unmarshaling failed: %v", id_str, err)
			continue
		}

		if value, exists := entry[field]; exists {
			entry := indexEntry{deleted: false, value: fmt.Sprintf("%v", value), id: id}
			fpos, _ := file.Seek(0, os.SEEK_END)
			if _, err := entry.WriteTo(file); err != nil {
				log.Printf("AddIndex: writing to index file failed: %v", err)
				c.RemoveIndex(field)
				return err
			}
			file.Sync()
			entry.fpos = fpos
			idx.Add(entry)
		}
	}

	c.indexes[field] = idx

	return nil
}

// RemoveIndex removes an existing index for a field. It returns a non-nil error if
// an error occurs.
func (c *Collection) RemoveIndex(field string) error {
	if idx, exists := c.indexes[field]; exists {
		idx.file.Close()
		delete(c.indexes, field)
		if err := os.Remove(c.indexpath + "/" + field); err != nil {
			return err
		}
	}
	return nil
}

// Reindex deletes and recreates the index for a field.
func (c *Collection) Reindex(field string) error {
	if err := c.RemoveIndex(field); err != nil {
		return err
	}
	return c.AddIndex(field)
}

func (c *Collection) removeFromIndexes(id Id) {
	// remove entries from indexes
	for _, idx := range c.indexes {
		for key, entries := range idx.data {
			new_entries := []indexEntry{}
			for _, e := range entries {
				if e.id != int64(id) {
					new_entries = append(new_entries, e)
				} else {
					idx.file.Seek(e.fpos, os.SEEK_SET)
					e.deleted = true
					e.WriteTo(idx.file)
					idx.file.Sync()
				}
			}
			if len(new_entries) > 0 {
				idx.data[key] = new_entries
			} else {
				delete(idx.data, key)
			}
		}
	}
}

// Delete deletes an object, identified by its ID, from the collection.
func (c *Collection) Delete(id Id) error {
	c.removeFromIndexes(id)
	return c.store.Erase(fmt.Sprintf("%d", id))
}

func (c *Collection) Query(q Condition) (*Result, error) {
	return nil, errors.New("query failed")
}

func (c *Collection) QueryAll() (*Result, error) {
	return c.Query(&True{})
}

// Vacuum expunges old entries that refer to deleted objects from all indexes 
// of a collection.
func (c *Collection) Vacuum() error {
	for field, _ := range c.indexes {
		oldf, err := os.Open(c.indexpath + "/" + field)
		if err != nil {
			return err
		}
		defer oldf.Close()

		newf, err := os.OpenFile(c.indexpath + "/." + field + ".tmp", os.O_WRONLY | os.O_CREATE, 0644)
		if err != nil {
			return err
		}
		defer newf.Close()

		for {
			var entry indexEntry
			n, err := entry.ReadFrom(oldf)
			if err != nil {
				if err == io.EOF {
					break
				}
				return err
			}

			// don't write deleted entries
			if entry.Deleted() {
				continue
			}

			m, err := entry.WriteTo(newf)
			if err != nil {
				return err
			}
			if n != m {
				os.Remove(c.indexpath + "/." + field + ".tmp")
				return fmt.Errorf("short write while writing new index for %s", field)
			}
		}

		os.Rename(c.indexpath + "/." + field + ".tmp", c.indexpath + "/" + field)
		newf.Sync()
	}
	return nil
}
