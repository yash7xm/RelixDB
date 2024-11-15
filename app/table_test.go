package relixdb

import (
	"fmt"
	"os"
	"testing"
)

func TestInitDB(t *testing.T) {
	// Initialize a new DB instance
	db := &DB{
		Path:   "testdb",
		kv:     KV{Path: "testdb"},
		tables: make(map[string]*TableDef),
	}

	// defer os.Remove("testdb")

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

	// Initialize a sample table for testing
	sampleTable := &TableDef{
		Name:  "test_table",                     // Name of the test table
		Types: []uint32{TYPE_BYTES, TYPE_BYTES}, // Column types: int64 and bytes
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
		AddStr("id", []byte("1")).
		AddStr("name", []byte("Test Name"))

	_, err = db.Insert("test_table", *record)
	if err != nil {
		t.Fatalf("failed to insert test data: %v", err)
	}
}

// func TestInitTestDB(t *testing.T) {
// 	db := initDB(t)

// 	// // // Initialize a sample table for testing
// 	sampleTable := &TableDef{
// 		Name:  "test_table",                     // Name of the test table
// 		Types: []uint32{TYPE_INT64, TYPE_BYTES}, // Column types: int64 and bytes
// 		Cols:  []string{"id", "name"},           // Column names: "id" and "name"
// 		PKeys: 1,                                // The first column "id" is the primary key
// 	}

// 	// Create the sample table in the test DB
// 	err := db.TableNew(sampleTable)
// 	if err != nil {
// 		t.Fatalf("failed to create sample table: %v", err)
// 	}

// 	// Optionally, insert some test data into the sample table
// 	record := (&Record{}).
// 		AddInt64("id", 1).
// 		AddStr("name", []byte("Test Name"))

// 	_, err = db.Insert("test_table", *record)
// 	if err != nil {
// 		t.Fatalf("failed to insert test data: %v", err)
// 	}
// }

func TestSetAndGet(t *testing.T) {
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

	table := &TableDef{
		Name:  "table",
		Types: []uint32{TYPE_BYTES, TYPE_BYTES, TYPE_BYTES},
		Cols:  []string{"col1", "col2", "col3"},
		PKeys: 1,
	}

	err = db.TableNew(table)
	if err != nil {
		t.Fatalf("Failed to create table1: %v", err)
	}

	record1 := (&Record{}).
		AddStr("col1", []byte("row11")).
		AddStr("col2", []byte("row12")).
		AddStr("col3", []byte("row13"))

	record2 := (&Record{}).
		AddStr("col1", []byte("row21")).
		AddStr("col2", []byte("row22")).
		AddStr("col3", []byte("row23"))

	_, err = db.Insert("table", *record1)
	if err != nil {
		t.Fatalf("failed to insert test data: %v", err)
	}

	_, err = db.Insert("table", *record2)
	if err != nil {
		t.Fatalf("failed to insert test data: %v", err)
	}

	record := (&Record{}).AddStr("col1", []byte("row11"))
	_, err = db.Get("table", record)
	if err != nil {
		t.Fatalf("failed to get test data: %v", err)
	}

	if string(record.Get("col2").Str) != string(record1.Get("col2").Str) {
		t.Fatalf("failed to get the correct name")
	}
	fmt.Printf("Id: %v, Name: %v Age: %v\n",
		string(record.Get("col1").Str),
		string(record.Get("col2").Str),
		string(record.Get("col3").Str))

	record = (&Record{}).AddStr("col1", []byte("row21"))
	_, err = db.Get("table", record)
	if err != nil {
		t.Fatalf("failed to get test data: %v", err)
	}

	if string(record.Get("col2").Str) != string(record2.Get("col2").Str) {
		t.Fatalf("failed to get the correct name")
	}
	fmt.Printf("Id: %v, Name: %v Age: %v\n",
		string(record.Get("col1").Str),
		string(record.Get("col2").Str),
		string(record.Get("col3").Str))
}
