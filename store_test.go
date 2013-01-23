package epos

import (
	"fmt"
	"testing"
)

func TestStore(t *testing.T) {
	db, err := OpenDatabase("testdb1", STORAGE_AUTO)
	if err != nil {
		t.Fatalf("couldn't open testdb1: %v", err)
	}
	defer db.Close()

	id, err := db.Coll("foo").Insert([]string{"hello", "world!"})
	if err != nil {
		t.Errorf("couldn't insert string slice: %v", err)
	}
	if id != 1 {
		t.Errorf("string slice id = %d (expected 1)", id)
	}

	id, err = db.Coll("foo").Insert(struct{ X, Y string }{X: "pan-galactic", Y: "gargle-blaster"})
	if err != nil {
		t.Errorf("couldn't insert struct: %v", err)
	}
	if id != 2 {
		t.Errorf("struct id = %d (expected 2)", id)
	}

	if err = db.Remove(); err != nil {
		t.Errorf("db.Remove failed: %v", err)
	}
}

var benchmarkData = struct {
	Name         string
	Age          uint
	SSN          string
	LuckyNumbers []int
}{
	Name:         "John J. McWhackadoodle",
	Age:          29,
	SSN:          "078-05-1120",
	LuckyNumbers: []int{23, 43},
}

func BenchmarkInsertDiskv(b *testing.B) {
	benchmarkInsert(b, STORAGE_DISKV)
}

func BenchmarkInsertLevelDB(b *testing.B) {
	benchmarkInsert(b, STORAGE_LEVELDB)
}

func BenchmarkInsertGoLevelDB(b *testing.B) {
	benchmarkInsert(b, STORAGE_GOLEVELDB)
}

func benchmarkInsert(b *testing.B, typ StorageType) {
	b.StopTimer()

	db, _ := OpenDatabase(fmt.Sprintf("testdb_bench_insert_%d", typ), typ)

	b.StartTimer()

	for i := 0; i < b.N; i++ {
		_, err := db.Coll("bench").Insert(benchmarkData)
		if err != nil {
			b.Fatal("insert failed: ", err)
		}
	}

	b.StopTimer()
	db.Close()
	db.Remove()
}

func BenchmarkUpdateDiskv(b *testing.B) {
	benchmarkUpdate(b, STORAGE_DISKV)
}

func BenchmarkUpdateLevelDB(b *testing.B) {
	benchmarkUpdate(b, STORAGE_LEVELDB)
}

func BenchmarkUpdateGoLevelDB(b *testing.B) {
	benchmarkUpdate(b, STORAGE_GOLEVELDB)
}

func benchmarkUpdate(b *testing.B, typ StorageType) {
	b.StopTimer()

	db, _ := OpenDatabase(fmt.Sprintf("testdb_bench_update_%d", typ), typ)

	id, err := db.Coll("bench").Insert(benchmarkData)
	if err != nil {
		b.Fatal("insert failed: ", err)
	}

	b.StartTimer()

	for i := 0; i < b.N; i++ {
		benchmarkData.LuckyNumbers[0], benchmarkData.LuckyNumbers[1] = benchmarkData.LuckyNumbers[1], benchmarkData.LuckyNumbers[0]
		if err = db.Coll("bench").Update(id, benchmarkData); err != nil {
			b.Fatal("update failed: ", err)
		}
	}

	b.StopTimer()
	db.Close()
	db.Remove()
}

func BenchmarkDeleteDiskv(b *testing.B) {
	benchmarkDelete(b, STORAGE_DISKV)
}

func BenchmarkDeleteLevelDB(b *testing.B) {
	benchmarkDelete(b, STORAGE_LEVELDB)
}

func BenchmarkDeleteGoLevelDB(b *testing.B) {
	benchmarkDelete(b, STORAGE_GOLEVELDB)
}

func benchmarkDelete(b *testing.B, typ StorageType) {
	b.StopTimer()

	db, _ := OpenDatabase(fmt.Sprintf("testdb_bench_delete_%s", typ), typ)

	b.StartTimer()

	for i := 0; i < b.N; i++ {
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
