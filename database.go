package epos

import (
	"os"
)

type Database struct {
	path  string
	colls map[string]*Collection
}

// OpenDatabase opens and if necessary creates a database identified by the
// provided path. It returns a database object and a non-nil error if an
// error occured while opening or creating the database.
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

// Vacuum calls Vacuum on all open collections.
func (db *Database) Vacuum() error {
	for _, coll := range db.colls {
		if err := coll.Vacuum(); err != nil {
			return err
		}
	}
	return nil
}
