package epos

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/peterbourgon/diskv"
	"os"
)

type Database struct {
	path  string
	colls map[string]*Collection
}

type Id int64

type Collection struct {
	store *diskv.Diskv
}

type Result struct{}

func OpenDatabase(path string) (*Database, error) {
	db := &Database{path: path, colls: make(map[string]*Collection)}
	for _, p := range []string{path, path+"/colls"} {
		if err := os.Mkdir(p, 0755); err != nil {
			return nil, err
		}
	}
	return db, nil
}

func (db *Database) Close() error {
	db.colls = nil
	return nil
}

func (db *Database) Remove() error {
	// TODO: remove db.path
	return nil
}

func (db *Database) Coll(name string) *Collection {
	// TODO: maybe pre-open collections when opening database so that we can properly report errors?
	coll := db.colls[name]
	if coll == nil {
		coll = db.openColl(name)
		db.colls[name] = coll
	}
	return coll
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

func (db *Database) openColl(name string) *Collection {
	// create/open collection
	coll := &Collection{store: diskv.New(diskv.Options{
		BasePath:  db.path + "/colls/" + name,
		Transform: transformFunc,
		CacheSizeMax: 0, // no cache
	})}

	// if _next_id is unset, then set it to 1.
	if _, err := coll.store.Read("_next_id"); err != nil {
		coll.setNextId(Id(1))
	}
	return coll
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

func (c *Collection) Insert(value interface{}) (Id, error) {
	jsondata, err := json.Marshal(value)
	if err != nil {
		return Id(0), err
	}

	id := c.getNextId()
	err = c.store.Write(fmt.Sprintf("%d", id), jsondata)
	if err != nil {
		c.setNextId(id) // roll back generated ID
		id = Id(0)     // set id to invalid value
	}
	return id, err
}

func (c *Collection) Update(id Id, value interface{}) error {
	jsondata, err := json.Marshal(value)
	if err != nil {
		return err
	}

	return c.store.Write(fmt.Sprintf("%d", id), jsondata)
}

func (c *Collection) AddIndex(field string) error {
	return errors.New("adding index failed")
}

func (c *Collection) RemoveIndex(field string) error {
	return errors.New("removing index failed")
}

func (c *Collection) Reindex(field string) error {
	if err := c.RemoveIndex(field); err != nil {
		return err
	}
	return c.AddIndex(field)
}

func (c *Collection) Delete(id Id) error {
	return c.store.Erase(fmt.Sprintf("%d", id))
}

func (c *Collection) Query(q Condition) (*Result, error) {
	return nil, errors.New("query failed")
}

func (c *Collection) QueryAll() (*Result, error) {
	return c.Query(&True{})
}

func (r *Result) Next(result interface{}) bool {
	return false
}
