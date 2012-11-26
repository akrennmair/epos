package epos

import (
	"testing"
)

func TestStore(t *testing.T) {
	db, err := OpenDatabase("testdb1")
	if err != nil {
		t.Fatalf("couldn't open testdb1: %v", err)
	}
	defer db.Close()

	id, err := db.Coll("foo").Insert([]string{"hello", "world!"})
	if err != nil {
		t.Error("couldn't insert string slice: %v", err)
	} else {
		t.Logf("string slice id = %d", id)
	}

	id, err = db.Coll("foo").Insert(struct { X, Y string } { X: "pan-galactic", Y: "gargle-blaster" })
	if err != nil {
		t.Errorf("couldn't insert struct: %v", err)
	} else {
		t.Logf("struct id = %d", id)
	}

	if err = db.Remove(); err != nil {
		t.Errorf("db.Remove failed: %v", err)
	}
}
