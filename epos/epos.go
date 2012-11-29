package main

import (
	"encoding/json"
	"fmt"
	"github.com/akrennmair/epos"
	"github.com/voxelbrain/goptions"
	"os"
	"runtime/pprof"
)

func main() {


	options := struct {
		Database string `goptions:"-d, --database, obligatory, description='Database to work on'"`
		CPUProfile string `goptions:"--cpuprofile, description='Record CPU profile for use with pprof.'"`
		goptions.Help   `goptions:"-h, --help, description='Show this help'"`

		goptions.Verbs
		Collections struct { } `goptions:"collections"`
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
		RemoveIndex struct {
			Collection string `goptions:"-c, --collection, obligatory, description='Collection to work on'"`
			Field      string `goptions:"-f, --field, obligatory, description='Field to remove index from'"`
		} `goptions:"rmindex"`
		// TODO: queries
	}{ }

	goptions.ParseAndFail(&options)

	if options.CPUProfile != "" {
		f, _ := os.Create(options.CPUProfile)
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}

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
					break
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
		case "rmindex":
			coll := db.Coll(options.RemoveIndex.Collection)
			if err := coll.RemoveIndex(options.RemoveIndex.Field); err != nil {
				fmt.Fprintf(os.Stderr, "Error while removing index: %v\n", err)
			}
		case "collections":
			if colls, err := db.Collections(); err != nil {
				fmt.Fprintf(os.Stderr, "Error while fetching collections: %v\n", err)
			} else {
				for _, collname := range colls {
					fmt.Printf("%s\n", collname)
				}
			}
		case "update":
			decoder := json.NewDecoder(os.Stdin)
			var data interface{}
			err := decoder.Decode(&data)
			if err != nil {
				 fmt.Fprintf(os.Stderr, "Error while decoding JSON document: %v\n", err)
				 return
			}
			coll := db.Coll(options.Update.Collection)
			if err := coll.Update(epos.Id(options.Update.Id), data); err != nil {
				fmt.Fprintf(os.Stderr, "Error while updating item %d: %v\n", options.Update.Id, err)
				return
			}
			fmt.Printf("Item %d updated successfully.\n", options.Update.Id)
		case "delete":
			coll := db.Coll(options.Delete.Collection)
			if err := coll.Delete(epos.Id(options.Delete.Id)); err != nil {
				fmt.Fprintf(os.Stderr, "Error while deleeting item %d: %v\n", options.Delete.Id, err)
				return
			}
			fmt.Printf("Item %d deleted.\n", options.Delete.Id)
		default:
			fmt.Fprintf(os.Stderr, "Error: unknown operation %s\n", options.Verbs)
	}
}
