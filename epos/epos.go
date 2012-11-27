package main

import (
	"fmt"
	"github.com/akrennmair/epos"
	"github.com/surma/goptions"
	"log"
	"os"
)

func main() {
	options := struct {
		Database string `goptions:"-d, --database, obligatory, description='Database to work on'"`
		goptions.Help   `goptions:"-h, --help, description='Show this help'"`

		goptions.Verbs
		Collections struct { } `goptions:"collections"`
		Insert struct {
			Collection string `goptions:"-c, --collection, obligatory, description='Collection to work on'"`
		} `goptions:"insert"`
		Update struct {
			Collection string `goptions:"-c, --collection, obligatory, description='Collection to work on'"`
			Id         int64  `goptions:"-i, --id, obligatory, description='ID of entry to update'"`
		} `goptions:"update"`
		Delete struct {
			Collection string `goptions:"-c, --collection, obligatory, description='Collection to work on'"`
			Id         int64  `goptions:"-i, --id, obligatory, description='ID of entry to delete'"`
		} `goptions:"delete"`
		Vacuum struct { } `goptions:"vacuum"`
		// TODO: add index, remove index, queries...
	}{ }

	goptions.ParseAndFail(&options)

	db, err := epos.OpenDatabase(options.Database)
	if err != nil {
		panic(err)
	}
	defer db.Close()

	switch options.Verbs {
		case "collections":
			log.Printf("list all collections")
		case "insert":
			log.Printf("read JSON document from stdin and save it to collection; print ID.")
		case "vacuum":
			err = db.Vacuum()
			if err != nil {
				fmt.Fprintf(os.Stderr, "Error while running vacuum: %v\n", err)
				os.Exit(1)
			}
		case "update":
			log.Printf("read JSON document from stdin and update specified ID.")
		default:
			log.Printf("unknown operation %s", options.Verbs)
	}
}
