package epos

import (
	"testing"
)

type book struct {
	Title  string
	Author string
	Price  float64
	Pages  int
}

var queryData = []book{
	{"Fables", "Aesop", 17.95, 239},
	{"Adventures of Huckleberry Finn", "Mark Twain", 7.95, 364},
	{"Alice's Adventures in Wonderland", "Lewis Caroll", 12.45, 375},
	{"Cinderella", "George Routledge & Sons", 8.95, 145},
	{"Dracula", "Bram Stoker", 23.95, 729},
	{"The Jungle Book", "Rudyard Kipling", 10.95, 396},
	{"Tom Sawyer Aboard", "Mark Twain", 9.99, 270},
}

func TestQueries(t *testing.T) {
	db, err := OpenDatabase("testdb_queries")
	if err != nil {
		t.Fatalf("couldn't open testdb_queries: %v", err)
	}
	defer db.Close()

	books := db.Coll("books")

	books.AddIndex("Author")

	for i, book := range queryData {
		_, err := books.Insert(book)
		if err != nil {
			t.Errorf("%d. Insert failed: %v", i, err)
		}
	}

	result, err := books.QueryId(1)
	if err != nil {
		t.Errorf("query for ID 1 failed: %v", err)
	}

	var b book
	if result.Next(nil, &b) != true {
		t.Errorf("couldn't fetch item with ID 1")
	}
	if b.Title != "Fables" {
		t.Errorf("expected book with ID 1 to be Fables, got %s instead.", b.Title)
	}
	if result.Next(nil, &b) != false {
		t.Errorf("expected end of results, got another result: %#v", b)
	}

	result, err = books.Query(&Equals{Field: "Author", Value: "Mark Twain"})
	var id Id
	i := 0
	for result.Next(&id, &b) {
		if b.Author != "Mark Twain" {
			t.Errorf("queried for books of Mark Twain, got author %s instead.", b.Author)
		}
		i++
	}
	if i != 2 {
		t.Errorf("expected 2 Mark Twain books, got %d instead.", i)
	}

	books.AddIndex("Pages")

	result, err = books.Query(&Or{&Equals{Field: "Author", Value: "Aesop"}, &Equals{Field: "Pages", Value: "270"}})
	i = 0
	for result.Next(nil, &b) {
		if b.Pages != 270 && b.Pages != 239 {
			t.Errorf("got unexpected book in query: %#v", b)
		}
		i++
	}
	if i != 2 {
		t.Errorf("expected 2 results from query, got %d instead.", i)
	}

	_, err = books.Query(&Equals{Field: "Name", Value: "Fables"})
	if err == nil {
		t.Errorf("queried for a field that isn't indexed and expected an error, but didn't get one.")
	}

	db.Remove()
}
