package BTree

import (
	"fmt"
	"os"
	"testing"
)

func TestInitTestDB(t *testing.T) {
	// Initialize a new DB instance
	db := &DB{
		Path:   "testdb",           // or use any path for testing
		kv:     KV{Path: "testdb"}, // Initialize the underlying KV store (assuming you have a NewKV() constructor)
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

	// // // Initialize a sample table for testing
	sampleTable := &TableDef{
		Name:  "test_table",                     // Name of the test table
		Types: []uint32{TYPE_INT64, TYPE_BYTES}, // Column types: int64 and bytes
		Cols:  []string{"id", "name"},           // Column names: "id" and "name"
		PKeys: 1,                                // The first column "id" is the primary key
	}

	// Create the sample table in the test DB
	err = db.TableNew(sampleTable)
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
