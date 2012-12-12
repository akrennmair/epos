# README for epos

[![Build Status][1]][2]

[1]: https://secure.travis-ci.org/akrennmair/epos.png
[2]: http://www.travis-ci.org/akrennmair/epos

## Introduction

epos is a embeddable persistent object store, written in Go.
It is meant to store, retrieve, query and delete Go objects to a local
file store. In this respect, it is NoSQL database, but it only
works on local files and is embeddable into existing Go programs,
so it can be thought of as the SQLite of NoSQL databases.

Here is a very basic overview how to use epos:

	// open/create database:
	db, err := epos.OpenDatabase("foo.db", epos.STORAGE_AUTO) // also available: STORAGE_DISKV, STORAGE_LEVELDB
	// insert item:
	id, err = db.Coll("users").Insert(new_user)
	// update item:
	err = db.Coll("users").Update(id, updated_user)
	// index fields:
	err = db.Coll("users").AddIndex("login")
	// query items:
	result, err = db.Coll("users").Query("(eq username foobar)")
	for result.Next(&id, &data) {
		// handle data
	}

## License

See file LICENSE for details.

## API Documentation

You can find the latest API documentation here: http://go.pkgdoc.org/github.com/akrennmair/epos

## Author

Andreas Krennmair <ak@synflood.at>

