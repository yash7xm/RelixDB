package test

import (
	"fmt"
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

func TestSetAndGet(t *testing.T) {
	// Initialize DB with proper structure
	db := &Table.DB{}
	db = db.NewDB(PATH)

	// Open database with proper error handling
	if err := db.Open(); err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Define test table schema
	table := &Table.TableDef{
		Name:    "table",
		Types:   []uint32{Table.TYPE_INT64, Table.TYPE_BYTES, Table.TYPE_INT64},
		Cols:    []string{"id", "name", "age"},
		PKeys:   1,
		Indexes: [][]string{}, // Empty indexes array to satisfy table definition
	}

	// Create the test table
	err := db.TableNew(table)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Test data setup
	testCases := []struct {
		id   int64
		name string
		age  int64
	}{
		{1, "yash", 21},
		{2, "yash2", 22},
	}

	// Insert test records
	for _, tc := range testCases {
		record := (&Table.Record{}).
			AddInt64("id", tc.id).
			AddStr("name", []byte(tc.name)).
			AddInt64("age", tc.age)

		_, err = db.Insert("table", *record)
		if err != nil {
			t.Fatalf("Failed to insert record with id %d: %v", tc.id, err)
		}
	}

	// Test retrieval of records
	for _, tc := range testCases {
		t.Run(fmt.Sprintf("Retrieving record with ID %d", tc.id), func(t *testing.T) {
			// Query the record
			queryRecord := (&Table.Record{}).AddInt64("id", tc.id)
			found, err := db.Get("table", queryRecord)
			if err != nil {
				t.Fatalf("Failed to get record with id %d: %v", tc.id, err)
			}
			if !found {
				t.Fatalf("Record with id %d not found", tc.id)
			}

			// Verify record contents
			if string(queryRecord.Get("name").Str) != tc.name {
				t.Errorf("Expected name '%s', got '%s'", tc.name, string(queryRecord.Get("name").Str))
			}
			if queryRecord.Get("age").I64 != tc.age {
				t.Errorf("Expected age %d, got %d", tc.age, queryRecord.Get("age").I64)
			}

			// Optional: Log the retrieved record for debugging
			t.Logf("Retrieved Record - Id: %v, Name: %v, Age: %v",
				queryRecord.Get("id").I64,
				string(queryRecord.Get("name").Str),
				queryRecord.Get("age").I64)
		})
	}
}
