package BTree

import (
	"fmt"
	"testing"
)

func initTestDB() (*DB, error) {
	// Initialize a new DB instance
	db := &DB{
		Path:   "testdb",           // or use any path for testing
		kv:     KV{Path: "testdb"}, // Initialize the underlying KV store (assuming you have a NewKV() constructor)
		tables: make(map[string]*TableDef),
	}

	if err := db.kv.Open(); err != nil {
		fmt.Printf("KV.Open() failed: %v", err)
	}
	defer db.kv.Close()

	// Initialize internal metadata table (for storing table schemas, next prefixes, etc.)
	err := db.TableNew(TDEF_META)
	if err != nil {
		return nil, fmt.Errorf("failed to create @meta table: %v", err)
	}

	// Initialize internal table schema table (for storing table definitions)
	err = db.TableNew(TDEF_TABLE)
	if err != nil {
		return nil, fmt.Errorf("failed to create @table table: %v", err)
	}

	return db, err
}

func TestInsertAndRetrieveRow(t *testing.T) {
	db, _ := initTestDB() // assume a helper function that sets up a test DB
	tdef := &TableDef{
		Name:  "users",
		Types: []uint32{TYPE_INT64, TYPE_BYTES},
		Cols:  []string{"id", "name"},
		PKeys: 1,
	}
	err := db.TableNew(tdef)
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	// Insert a new record
	rec := (&Record{}).AddInt64("id", 123).AddStr("name", []byte("Alice"))
	added, err := db.Insert("users", *rec)
	if err != nil || !added {
		t.Fatalf("failed to insert row: %v", err)
	}

	// Try to retrieve the record by primary key
	rec2 := (&Record{}).AddInt64("id", 123)
	found, err := db.Get("users", rec2)
	if err != nil || !found {
		t.Fatalf("failed to retrieve row: %v", err)
	}

	if string(rec2.Get("name").Str) != "Alice" {
		t.Errorf("expected name 'Alice', got %s", string(rec2.Get("name").Str))
	}
}
