package relixdb

import (
	"fmt"
	"os"
	"testing"
)

func initDB(t *testing.T) *DB {
	// Initialize a new DB instance
	db := &DB{
		Path:   "testdb",
		kv:     KV{Path: "testdb"},
		tables: make(map[string]*TableDef),
	}

	defer os.Remove("testdb")

	if err := db.kv.Open(); err != nil {
		fmt.Printf("KV.Open() failed: %v", err)
	}
	defer db.kv.Close()

	// Initialize internal table schema table (for storing table definitions)
	err := db.TableNew(TDEF_TABLE)
	if err != nil {
		t.Fatalf("failed to create @table table: %v", err)
	}

	// Initialize internal metadata table (for storing table schemas, next prefixes, etc.)
	err = db.TableNew(TDEF_META)
	if err != nil {
		t.Fatalf("failed to create @meta table: %v", err)
	}

	return db
}

func TestInitTestDB(t *testing.T) {
	db := initDB(t)

	// // // Initialize a sample table for testing
	sampleTable := &TableDef{
		Name:  "test_table",                     // Name of the test table
		Types: []uint32{TYPE_INT64, TYPE_BYTES}, // Column types: int64 and bytes
		Cols:  []string{"id", "name"},           // Column names: "id" and "name"
		PKeys: 1,                                // The first column "id" is the primary key
	}

	// Create the sample table in the test DB
	err := db.TableNew(sampleTable)
	if err != nil {
		t.Fatalf("failed to create sample table: %v", err)
	}

	// Optionally, insert some test data into the sample table
	record := (&Record{}).
		AddInt64("id", 1).
		AddStr("name", []byte("Test Name"))

	_, err = db.Insert("test_table", *record)
	if err != nil {
		t.Fatalf("failed to insert test data: %v", err)
	}
}

func TestSetAndGet(t *testing.T) {
	db := initDB(t)

	table1 := &TableDef{
		Name:  "table1",
		Types: []uint32{TYPE_INT64, TYPE_BYTES},
		Cols:  []string{"id", "name"},
		PKeys: 1,
	}

	err := db.TableNew(table1)
	if err != nil {
		t.Fatalf("Failed to create table1: %v", err)
	}

	record1 := (&Record{}).
		AddInt64("id", 1).
		AddStr("name", []byte("Alice"))

	record2 := (&Record{}).
		AddInt64("id", 2).
		AddStr("name", []byte("Bob"))

	_, err = db.Insert("table1", *record1)
	if err != nil {
		t.Fatalf("failed to insert test data: %v", err)
	}

	_, err = db.Insert("table1", *record2)
	if err != nil {
		t.Fatalf("failed to insert test data: %v", err)
	}

	record := (&Record{}).AddInt64("id", 1)
	_, err = db.Get("table1", record)
	if err != nil {
		t.Fatalf("failed to get test data: %v", err)
	}

	if string(record.Get("name").Str) != string(record1.Get("name").Str) {
		t.Fatalf("failed to get the correct name")
	}
	fmt.Printf("Id: %v, Name: %v\n", (record.Get("id").I64), string(record.Get("name").Str))

	record = (&Record{}).AddInt64("id", 2)
	_, err = db.Get("table1", record)
	if err != nil {
		t.Fatalf("failed to get test data: %v", err)
	}

	if string(record.Get("name").Str) != string(record2.Get("name").Str) {
		t.Fatalf("failed to get the correct name")
	}
	fmt.Printf("Id: %v, Name: %v\n", (record.Get("id").I64), string(record.Get("name").Str))
}
