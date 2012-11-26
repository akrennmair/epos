package epos

import (
	"errors"
	"fmt"
)

type Database struct{}

type Id int64

type Collection struct{}

type Result struct{}

func OpenDatabase(path string) (*Database, error) {
	return nil, fmt.Errorf("couldn't open database %s", path)
}

func (db *Database) Close() error {
	return nil
}

func (db *Database) Remove() error {
	return nil
}

func (db *Database) Coll(name string) *Collection {
	return nil
}

func (c *Collection) Insert(value interface{}) (Id, error) {
	return 0, errors.New("insert failed")
}

func (c *Collection) Update(id Id, value interface{}) error {
	return errors.New("update failed")
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

func (c *Collection) Delete(key string) error {
	return errors.New("delete failed")
}

func (c *Collection) Query(q Condition) (*Result, error) {
	return nil, errors.New("query failed")
}

func (c *Collection) QueryAll() (*Result, error) {
	return c.Query(nil)
}

func (r *Result) Next(result interface{}) bool {
	return false
}
