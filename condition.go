package epos

import (
	"fmt"
)

type Condition interface {
	match(indexes map[string]*index) []Id
	getFields() []string
}

type And []Condition

func (c *And) match(indexes map[string]*index) []Id {
	var idSet map[Id]bool

	for i, cond := range *c {
		if i == 0 {
			idSet = makeSet(cond.match(indexes))
		} else {
			idSet = intersectSets(idSet, makeSet(cond.match(indexes)))
		}
	}

	return setToSlice(idSet)
}

func (c *And) getFields() []string {
	fields := []string{}
	for _, cond := range *c {
		fields = append(fields, cond.getFields()...)
	}

	return fields
}

type Or []Condition

func (c *Or) match(indexes map[string]*index) []Id {
	idSet := make(map[Id]bool)
	for _, cond := range *c {
		for _, id := range cond.match(indexes) {
			idSet[id] = true
		}
	}
	return setToSlice(idSet)
}

func (c *Or) getFields() []string {
	fields := []string{}
	for _, cond := range *c {
		fields = append(fields, cond.getFields()...)
	}

	return fields
}

type Equals struct {
	Field string
	Value interface{}
}

func (c *Equals) match(indexes map[string]*index) []Id {
	ids := []Id{}

	idx := indexes[c.Field]

	if idx == nil {
		return ids
	}

	entries := idx.data[fmt.Sprintf("%v", c.Value)]

	if entries == nil {
		return ids
	}

	for _, e := range entries {
		ids = append(ids, Id(e.id))
	}

	return ids
}

func (c *Equals) getFields() []string {
	return []string{c.Field}
}

func (c *Id) match(indexes map[string]*index) []Id {
	return []Id{*c}
}

func (c *Id) getFields() []string {
	return []string{}
}

func makeSet(ids []Id) map[Id]bool {
	set := make(map[Id]bool)
	for _, id := range ids {
		set[id] = true
	}
	return set
}

func setToSlice(set map[Id]bool) []Id {
	ids := []Id{}
	for id, _ := range set {
		ids = append(ids, id)
	}
	return ids
}

func intersectSets(a, b map[Id]bool) map[Id]bool {
	set := make(map[Id]bool)
	for k, _ := range a {
		set[k] = b[k]
	}
	return set
}
