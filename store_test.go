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

var benchmarkData = struct {
		Name string
		Age uint
		SSN string
		LuckyNumbers []int
	} {
		Name: "John J. McWhackadoodle",
		Age: 29,
		SSN: "078-05-1120",
		LuckyNumbers: []int{23, 43},
	}

func BenchmarkInsert(b *testing.B) {
	b.StopTimer()

	db, _ := OpenDatabase("testdb_bench_insert")

	b.StartTimer()

	for i := 0 ; i < b.N ; i++ {
		_, err := db.Coll("bench").Insert(benchmarkData)
		if err != nil {
			b.Fatal("insert failed: ", err)
		}
	}

	b.StopTimer()
	db.Close()
	db.Remove()
}

func BenchmarkUpdate(b *testing.B) {
	b.StopTimer()

	db, _ := OpenDatabase("testdb_bench_update")

	id, err := db.Coll("bench").Insert(benchmarkData)
	if err != nil {
		b.Fatal("insert failed: ", err)
	}

	b.StartTimer()

	for i := 0; i < b.N ; i++ {
		benchmarkData.LuckyNumbers[0], benchmarkData.LuckyNumbers[1] = benchmarkData.LuckyNumbers[1], benchmarkData.LuckyNumbers[0]
		if err = db.Coll("bench").Update(id, data); err != nil {
			b.Fatal("update failed: ", err)
		}
	}

	b.StopTimer()
	db.Close()
	db.Remove()
}

func BenchmarkDelete(b *testing.B) {
	b.StopTimer()

	db, _ := OpenDatabase("testdb_bench_delete")

	b.StartTimer()

	for i := 0 ; i < b.N ; i++ {
		b.StopTimer()
		id, err := db.Coll("bench").Insert(benchmarkData)
		if err != nil {
			b.Fatal("insert failed: ", err)
		}
		b.StartTimer()
		if err = db.Coll("bench").Delete(id); err != nil {
			b.Fatal("delete failed: ", err)
		}
	}

	b.StopTimer()
	db.Close()
	db.Remove()

}
