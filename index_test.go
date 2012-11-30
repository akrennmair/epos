package epos

import (
	"bytes"
	"reflect"
	"testing"
)

func TestIndexSimple(t *testing.T) {
	db, err := OpenDatabase("testdb_index", STORAGE_AUTO)
	if err != nil {
		t.Fatalf("couldn't open testdb_index: %v", err)
	}
	defer db.Close()

	id, err := db.Coll("tbl").Insert(map[string]string{"foo": "bar", "baz": "quux", "bla": "fasel"})
	if err != nil {
		t.Error("Insert failed: ", err)
	}

	t.Logf("insert id = %d", id)
	if err = db.Coll("tbl").AddIndex("foo"); err != nil {
		t.Fatal("AddIndex failed: ", err)
	}

	id, err = db.Coll("tbl").Insert(map[string]string{"foo": "abc", "baz": "def", "bla": "asdfqwer"})
	if err != nil {
		t.Error("Insert failed: ", err)
	}

	// plausibility check on the internal data structures:
	if idx := db.Coll("tbl").indexes["foo"]; idx == nil {
		t.Error("no actual index has been created!")
	} else if len(idx.data) != 2 {
		t.Errorf("expected two entries to be in the index for foo, found only %d (index: %#v)", len(idx.data), idx)
	} else {
		t.Logf("index data for foo: %#v", idx.data)
	}

	db.Remove()
}

func TestReadWriteIndex(t *testing.T) {
	buf := bytes.NewBuffer([]byte{})

	idx_entry := &indexEntry{deleted: false, value: "value", id: 9001}

	n, err := idx_entry.WriteTo(buf)
	if err != nil {
		t.Fatalf("WriteTo failed: %v", err)
	}

	entry2 := &indexEntry{}

	m, err := entry2.ReadFrom(buf)
	if err != nil {
		t.Fatalf("ReadFrom failed: %v", err)
	}

	if n != m {
		t.Errorf("different amounts of data were written and read (%d vs. %d)", n, m)
	}

	if !reflect.DeepEqual(idx_entry, entry2) {
		t.Errorf("entry and entry2 are different! %#v vs. %#v", idx_entry, entry2)
	}
}

type entry struct {
	X string
	Y int
	Z float64
}

var testdata = []struct {
	Entry entry
	NewX  string
	NewY  int
	NewZ  float64
	Id    Id
}{
	{Entry: entry{X: "John Doe", Y: 23, Z: 1.85}, NewX: "Max Mustermann", NewY: 42, NewZ: 1.83, Id: Id(0)},
	{Entry: entry{X: "Jan Maier", Y: 17, Z: 1.75}, NewX: "Franz Huber", NewY: 19, NewZ: 1.97, Id: Id(0)},
	{Entry: entry{X: "Franz Haber", Y: 19, Z: 1.90}, NewX: "Franz Haber-Oettinger", NewY: 19, NewZ: 1.90, Id: Id(0)},
}

func TestIndexInsertUpdateDelete(t *testing.T) {
	db, err := OpenDatabase("testdb_index_iud", STORAGE_AUTO)
	if err != nil {
		t.Fatalf("couldn't open testdb_index_iud: %v", err)
	}
	defer db.Close()

	coll := db.Coll("persons")

	coll.AddIndex("X")

	for i, e := range testdata {
		id, err := coll.Insert(e.Entry)
		if err != nil {
			t.Errorf("%d. Insert failed: %v", i, err)
		}
		testdata[i].Id = id
		testdata[i].Entry.X = e.NewX
		testdata[i].Entry.Y = e.NewY
		testdata[i].Entry.Z = e.NewZ
	}

	if len(coll.indexes["X"].data) != 3 {
		t.Errorf("Index doesn't contain 3 entries for field X even though we just inserted 3 records, %d instead.", len(coll.indexes["X"].data))
	}

	coll.AddIndex("Y")

	if len(coll.indexes["Y"].data) != 3 {
		t.Errorf("Index doesn't contain 3 entries for field Y, %d instead.", len(coll.indexes["Y"].data))
	}

	for i, e := range testdata {
		err := coll.Update(e.Id, e.Entry)
		if err != nil {
			t.Errorf("%d. Update failed: %v", i, err)
		}
	}

	if len(coll.indexes["X"].data) != 3 {
		t.Errorf("Index doesn't contain 3 entries for field X even though we just updated 3 records, %d instead.", len(coll.indexes["X"].data))
		t.Logf("index: %#v", coll.indexes["X"].data)
	}
	if len(coll.indexes["Y"].data) != 2 {
		t.Errorf("Index doesn't contain 2 entries for field Y even though we just updated 3 records, %d instead.", len(coll.indexes["Y"].data))
		t.Logf("index: %#v", coll.indexes["Y"].data)
	}
	if len(coll.indexes["Y"].data["19"]) != 2 {
		t.Errorf("Index doesn't contain 2 IDs for data 19, %d instead.", len(coll.indexes["Y"].data["19"]))
		t.Logf("index: %#v", coll.indexes["Y"].data["19"])
	}

	for i, e := range testdata {
		found := false
		for k, v := range coll.indexes["X"].data {
			if k == e.Entry.X && len(v) == 1 && v[0].id == int64(e.Id) {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("%d. couldn't find entry in X index for data '%s'", i, e.Entry.X)
		}
	}

	for i, e := range testdata {
		err := coll.Delete(e.Id)
		if err != nil {
			t.Errorf("%d. Delete failed: %v", i, err)
		}
	}

	if len(coll.indexes["X"].data) != 0 {
		t.Errorf("Index for X isn't empty: %v", coll.indexes["X"].data)
	}
	if len(coll.indexes["Y"].data) != 0 {
		t.Errorf("Index for Y isn't empty: %v", coll.indexes["Y"].data)
	}

	if err = db.Vacuum(); err != nil {
		t.Errorf("Vacuum failed: %v", err)
	}
	db.Remove()
}
