package test

import (
	"testing"

	Table "github.com/yash7xm/RelixDB/app"
)

const PATH = "../archive/testdb"

func TestSampleTestData(t *testing.T) {
	// Initialize the DB correctly
	db := &Table.DB{}
	db = db.NewDB(PATH)

	// Open the database with proper error handling
	if err := db.Open(); err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Initialize a sample table for testing
	sampleTable := &Table.TableDef{
		Name:    "test_table",                                 // Name of the test table
		Types:   []uint32{Table.TYPE_INT64, Table.TYPE_BYTES}, // Column types: int64 and bytes
		Cols:    []string{"id", "name"},                       // Column names: "id" and "name"
		PKeys:   1,                                            // The first column "id" is the primary key
		Indexes: [][]string{},                                 // Empty indexes array to satisfy table definition
	}

	// Create the sample table in the test DB
	err := db.TableNew(sampleTable)
	if err != nil {
		t.Fatalf("Failed to create sample table: %v", err)
	}

	// Insert test data into the sample table
	record := (&Table.Record{}).
		AddInt64("id", 1).
		AddStr("name", []byte("Test Name"))

	_, err = db.Insert("test_table", *record)
	if err != nil {
		t.Fatalf("Failed to insert test data: %v", err)
	}

	// Verify the inserted data
	queryRecord := (&Table.Record{}).AddInt64("id", 1)
	found, err := db.Get("test_table", queryRecord)
	if err != nil {
		t.Fatalf("Failed to query test data: %v", err)
	}
	if !found {
		t.Fatal("Failed to find inserted record")
	}

	// Verify the record contents
	if string(queryRecord.Get("name").Str) != "Test Name" {
		t.Errorf("Expected name 'Test Name', got '%s'", string(queryRecord.Get("name").Str))
	}
}
