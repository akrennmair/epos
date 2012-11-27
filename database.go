package epos

import (
	"os"
)

type Database struct {
	path  string
	colls map[string]*Collection
}

func OpenDatabase(path string) (*Database, error) {
	db := &Database{path: path, colls: make(map[string]*Collection)}
	for _, p := range []string{path, path + "/colls", path + "/indexes"} {
		if _, err := os.Stat(p); err != nil {
			if err := os.Mkdir(p, 0755); err != nil {
				return nil, err
			}
		}
	}
	return db, nil
}

func (db *Database) Close() error {
	db.colls = nil
	return nil
}

func (db *Database) Remove() error {
	return os.RemoveAll(db.path)
}

func (db *Database) Coll(name string) *Collection {
	// TODO: maybe pre-open collections when opening database so that we can properly report errors?
	coll := db.colls[name]
	if coll == nil {
		coll = db.openColl(name)
		db.colls[name] = coll
	}
	return coll
}

