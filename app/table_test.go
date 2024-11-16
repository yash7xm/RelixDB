package relixdb

import (
	"fmt"
	"testing"
)

const PATH = "../archive/testdb"

// func TestInitDB(t *testing.T) {
// 	db := &DB{
// 		Path:   PATH,
// 		kv:     KV{Path: PATH},
// 		tables: make(map[string]*TableDef),
// 	}

// 	if err := db.kv.Open(); err != nil {
// 		fmt.Printf("KV.Open() failed: %v", err)
// 	}
// 	defer db.kv.Close()

// 	err := db.TableNew(TDEF_TABLE)
// 	if err != nil {
// 		t.Fatalf("failed to create @table table: %v", err)
// 	}

// 	err = db.TableNew(TDEF_META)
// 	if err != nil {
// 		t.Fatalf("failed to create @meta table: %v", err)
// 	}
// }

func TestSampleTestData(t *testing.T) {
	db := &DB{
		Path:   PATH,
		kv:     KV{Path: PATH},
		tables: make(map[string]*TableDef),
	}

	if err := db.kv.Open(); err != nil {
		fmt.Printf("KV.Open() failed: %v", err)
	}
	defer db.kv.Close()

	// Initialize a sample table for testing
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
	db := &DB{
		Path:   PATH,
		kv:     KV{Path: PATH},
		tables: make(map[string]*TableDef),
	}

	if err := db.kv.Open(); err != nil {
		fmt.Printf("KV.Open() failed: %v", err)
	}
	defer db.kv.Close()

	table := &TableDef{
		Name:  "table",
		Types: []uint32{TYPE_INT64, TYPE_BYTES, TYPE_INT64},
		Cols:  []string{"id", "name", "age"},
		PKeys: 1,
	}

	err := db.TableNew(table)
	if err != nil {
		t.Fatalf("Failed to create table1: %v", err)
	}

	record1 := (&Record{}).
		AddInt64("id", 1).
		AddStr("name", []byte("yash")).
		AddInt64("age", 21)

	record2 := (&Record{}).
		AddInt64("id", 2).
		AddStr("name", []byte("yash2")).
		AddInt64("age", 22)

	_, err = db.Insert("table", *record1)
	if err != nil {
		t.Fatalf("failed to insert test data: %v", err)
	}

	_, err = db.Insert("table", *record2)
	if err != nil {
		t.Fatalf("failed to insert test data: %v", err)
	}

	record := (&Record{}).AddInt64("id", 1)
	_, err = db.Get("table", record)
	if err != nil {
		t.Fatalf("failed to get test data: %v", err)
	}

	if string(record.Get("name").Str) != string(record1.Get("name").Str) {
		t.Fatalf("failed to get the correct name")
	}
	fmt.Printf("Id: %v, Name: %v Age: %v\n",
		(record.Get("id").I64),
		string(record.Get("name").Str),
		(record.Get("age").I64))

	record = (&Record{}).AddInt64("id", 2)
	_, err = db.Get("table", record)
	if err != nil {
		t.Fatalf("failed to get test data: %v", err)
	}

	if string(record.Get("name").Str) != string(record2.Get("name").Str) {
		t.Fatalf("failed to get the correct name")
	}
	fmt.Printf("Id: %v, Name: %v Age: %v\n",
		(record.Get("id").I64),
		string(record.Get("name").Str),
		(record.Get("age").I64))
}
