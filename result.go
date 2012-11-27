package epos

import (
	"fmt"
	"encoding/json"
	"github.com/peterbourgon/diskv"
	"log"
)

type Result struct{
	ids []Id
	i   int
	store *diskv.Diskv
}

func (r *Result) Next(id *Id, result interface{}) bool {
	if r.i >= len(r.ids) {
		return false
	}

	if id != nil {
		*id = r.ids[r.i]
	}

	jsondata, err := r.store.Read(fmt.Sprintf("%d", r.ids[r.i]))
	if err != nil {
		log.Printf("result.Next: retrieving %d failed: %v", r.ids[r.i], err)
		return false
	}

	if err := json.Unmarshal(jsondata, result); err != nil {
		log.Printf("result.Next: json.Unmarshal of entry %d failed: %v", r.ids[r.i], err)
		return false
	}

	r.i++
	return true
}

func newResult(c *Collection, ids []Id) *Result {
	return &Result{store: c.store, ids: ids, i: 0}
}
