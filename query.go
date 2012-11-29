package epos

import (
	"fmt"
	"strconv"
)

// Query takes a query in the form of a (possibly nested) Condition, and returns
// a Result object.
func (c *Collection) Query(q Condition) (*Result, error) {
	fields := getFields(q)

	for _, field := range fields {
		_, ok := c.indexes[field]
		if !ok {
			return nil, fmt.Errorf("no index on field '%s'", field)
		}
	}

	ids := q.match(c.indexes)

	return newResult(c, ids), nil
}

// QueryId returns a Result object that will exactly deliver
// the object with the requested ID.
func (c *Collection) QueryId(id Id) (*Result, error) {
	return c.Query(&id)
}

// QueryAll returns a Result object that will deliver
// all objects in the object store in no particular order.
func (c *Collection) QueryAll() (*Result, error) {
	ids := []Id{}
	it := c.store.NewIterator(c.ro)
	defer it.Close()
	for it.SeekToFirst(); it.Valid(); it.Next() {
		id_str := string(it.Key())
		id, err := strconv.ParseInt(id_str, 10, 64)
		if err == nil {
			ids = append(ids, Id(id))
		}
	}
	return newResult(c, ids), nil
}

func getFields(q Condition) []string {
	raw_fields := q.getFields()

	fields_map := make(map[string]bool)

	for _, field := range raw_fields {
		fields_map[field] = true
	}

	fields := []string{}
	for k, _ := range fields_map {
		fields = append(fields, k)
	}

	return fields
}
