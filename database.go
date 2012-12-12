// epos is a embeddable persistent object store, written in Go.
// It is meant to store, retrieve, query and delete Go objects to a local
// file store. In this respect, it is NoSQL database, but it only
// works on local files and is embeddable into existing Go programs,
// so it can be thought of as the SQLite of NoSQL databases.
//
// Here is a very basic overview how to use epos:
//
//	// open/create database:
//	db, err := epos.OpenDatabase("foo.db", epos.STORAGE_AUTO) // also available: STORAGE_DISKV, STORAGE_LEVELDB
//	// insert item:
//	id, err = db.Coll("users").Insert(new_user)
//	// update item:
//	err = db.Coll("users").Update(id, updated_user)
//	// index fields:
//	err = db.Coll("users").AddIndex("login")
//	// query items:
//	result, err = db.Coll("users").Query(epos.Expression("(eq username foobar)"))
//	for result.Next(&id, &data) {
//		// handle data
//	}
//
package epos

import (
	"fmt"
	"io/ioutil"
	"os"
)

type Database struct {
	path           string
	colls          map[string]*Collection
	storageFactory func(path string) StorageBackend
}

// OpenDatabase opens and if necessary creates a database identified by the
// provided path. It returns a database object and a non-nil error if an
// error occured while opening or creating the database.
func OpenDatabase(path string, typ StorageType) (*Database, error) {
	db := &Database{path: path, colls: make(map[string]*Collection)}

	for _, p := range []string{path, path + "/colls", path + "/indexes"} {
		if _, err := os.Stat(p); err != nil {
			if err := os.Mkdir(p, 0755); err != nil {
				return nil, err
			}
		}
	}

	if typ == STORAGE_AUTO {
		typ = STORAGE_LEVELDB
	}

	write_storage := false
	storage_type, err := ioutil.ReadFile(db.path + "/engine")
	if err == nil {
		db.storageFactory = storageBackends[StorageType(storage_type)]
	} else {
		db.storageFactory = storageBackends[typ]
		write_storage = true
	}

	if db.storageFactory == nil {
		return nil, fmt.Errorf("invalid storage type %s", string(storage_type))
	}

	if write_storage {
		ioutil.WriteFile(db.path+"/engine", []byte(typ), 0644)
	}

	return db, nil
}

// Close closes the database and frees the memory associated with all collections.
func (db *Database) Close() error {
	db.colls = nil
	return nil
}

// Remove physically removes the database from the filesystem. WARNING: unless you 
// have proper backups or snapshots from your filesystem, this operation is 
// irreversible and leads to permanent data loss.
func (db *Database) Remove() error {
	return os.RemoveAll(db.path)
}

// Coll returns the collection of the specified name. If the collection doesn't
// exist yet, it is opened and/or created on the fly.
func (db *Database) Coll(name string) *Collection {
	coll := db.colls[name]
	if coll == nil {
		coll = db.openColl(name)
		db.colls[name] = coll
	}
	return coll
}

// Collections returns a list of collection names that are currently in
// the database.
func (db *Database) Collections() ([]string, error) {
	dir, err := os.Open(db.path + "/colls")
	if err != nil {
		return nil, err
	}
	defer dir.Close()

	colls := []string{}

	fi, err := dir.Readdir(0)

	if err != nil {
		return nil, err
	}

	for _, e := range fi {
		if e.IsDir() {
			colls = append(colls, e.Name())
		}
	}

	return colls, nil
}

// Vacuum calls Vacuum on all open collections.
func (db *Database) Vacuum() error {
	for _, coll := range db.colls {
		if err := coll.Vacuum(); err != nil {
			return err
		}
	}
	return nil
}
