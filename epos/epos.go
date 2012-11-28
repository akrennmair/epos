package main

import (
	"encoding/json"
	"fmt"
	"github.com/akrennmair/epos"
	"github.com/surma/goptions"
	"io"
	"log"
	"os"
)

func main() {
	options := struct {
		Database string `goptions:"-d, --database, obligatory, description='Database to work on'"`
		goptions.Help   `goptions:"-h, --help, description='Show this help'"`

		goptions.Verbs
		Dump struct {
			Collection string `goptions:"-c, --collection, obligatory, description='Collection to dump'"`
		} `goptions:"dump"`
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
		AddIndex struct {
			Collection string `goptions:"-c, --collection, obligatory, description='Collection to work on'"`
			Field      string `goptions:"-f, --field, obligatory, description='Field to create index on'"`
		} `goptions:"addindex"`
		// TODO: add index, remove index, queries...
	}{ }

	goptions.ParseAndFail(&options)

	db, err := epos.OpenDatabase(options.Database)
	if err != nil {
		panic(err)
	}
	defer db.Close()

	switch options.Verbs {
		case "dump":
			coll := db.Coll(options.Dump.Collection)
			result, _ := coll.QueryAll()
			var id epos.Id
			var data interface{}
			for result.Next(&id, &data) {
				jsondata, err := json.Marshal(data)
				if err != nil {
					fmt.Fprintf(os.Stderr, "Error: %v\n", err)
					continue
				}
				fmt.Printf("ID %d:\n%s\n\n", id, string(jsondata))
			}
		case "insert":
			decoder := json.NewDecoder(os.Stdin)
			for {
				var data interface{}
				err := decoder.Decode(&data)
				if err != nil {
					if err != io.EOF {
						fmt.Fprintf(os.Stderr, "Error while decoding JSON document: %v\n", err)
						break
					}
				}
				coll := db.Coll(options.Insert.Collection)
				id, err := coll.Insert(data)
				if err != nil {
					fmt.Fprintf(os.Stderr, "Error while inserting item: %v\n", err)
					break
				}
				fmt.Printf("ID = %d\n", id)
			}
		case "vacuum":
			err = db.Vacuum()
			if err != nil {
				fmt.Fprintf(os.Stderr, "Error while running vacuum: %v\n", err)
				os.Exit(1)
			}
		case "addindex":
			coll := db.Coll(options.AddIndex.Collection)
			if err := coll.AddIndex(options.AddIndex.Field); err != nil {
				fmt.Fprintf(os.Stderr, "Error while adding index: %v\n", err)
			}
		case "update":
			// TODO: implement
			log.Printf("read JSON document from stdin and update specified ID.")
		case "delete":
			// TODO: implement
		default:
			fmt.Fprintf(os.Stderr, "unknown operation %s", options.Verbs)
	}
}
