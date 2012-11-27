package epos

import (
	"fmt"
	"strconv"
)

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

func (c *Collection) QueryAll() (*Result, error) {
	ids := []Id{}
	for id_str := range c.store.Keys() {
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
