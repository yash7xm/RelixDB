package test

import (
	"fmt"
	"testing"

	Table "github.com/yash7xm/RelixDB/app"
)

func TestInsertSecondaryIndex(t *testing.T) {
	// Initialize DB with proper structure
	db := &Table.DB{}
	db = db.NewDB(TEST_DB_PATH)

	// Open database with proper error handling
	if err := db.Open(); err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Define test table schema
	table := &Table.TableDef{
		Name:    "table",
		Types:   []uint32{Table.TYPE_BYTES, Table.TYPE_BYTES, Table.TYPE_BYTES},
		Cols:    []string{"id", "name", "lastname"},
		PKeys:   1,
		Indexes: [][]string{{"name"}},
	}

	// Create the test table
	err := db.TableNew(table)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Test data setup
	testCases := []struct {
		id       string
		name     string
		lastname string
	}{
		{"a", "alice", "panga"},
		{"b", "bob", "changa"},
	}

	// Insert test records
	for _, tc := range testCases {
		record := (&Table.Record{}).
			AddStr("id", []byte(tc.id)).
			AddStr("name", []byte(tc.name)).
			AddStr("lastname", []byte(tc.lastname))

		_, err := db.Insert("table", *record)
		if err != nil {
			t.Fatalf("Failed to insert record with id %v: %v", string(tc.id), err)
		}
	}
}

func TestSetAndScanIndex(t *testing.T) {
	// Initialize DB with proper structure
	db := &Table.DB{}
	db = db.NewDB(TEST_DB_PATH)

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
		Indexes: [][]string{{"name"}, {"age"}},
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
		{1, "alice", 25},
		{2, "bob", 30},
		{3, "charlie", 35},
		{4, "david", 40},
		{5, "eve", 45},
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
			queryRecord := (&Table.Record{}).AddInt64("age", (tc.age))
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

func TestRangeQueriesWithSecondaryIndexes(t *testing.T) {
	// Initialize DB with proper structure
	db := &Table.DB{}
	db = db.NewDB(TEST_DB_PATH)

	// Open database with proper error handling
	if err := db.Open(); err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Define test table schema
	table := &Table.TableDef{
		Name:    "users",
		Types:   []uint32{Table.TYPE_INT64, Table.TYPE_BYTES, Table.TYPE_INT64},
		Cols:    []string{"id", "name", "age"},
		PKeys:   1,
		Indexes: [][]string{{"name"}, {"age"}},
	}

	// Create the test table
	err := db.TableNew(table)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Test data setup
	test := []struct {
		id   int64
		name string
		age  int64
	}{
		{1, "alice", 25},
		{2, "bob", 30},
		{3, "charlie", 35},
		{4, "david", 40},
		{5, "eve", 45},
	}

	// Insert test records
	for _, tc := range test {
		record := (&Table.Record{}).
			AddInt64("id", tc.id).
			AddStr("name", []byte(tc.name)).
			AddInt64("age", tc.age)

		_, err = db.Insert("users", *record)
		if err != nil {
			t.Fatalf("Failed to insert record with id %d: %v", tc.id, err)
		}
	}

	// Test range query scenarios
	testCases := []struct {
		name        string
		cmp1        int
		cmp2        int
		key1        *Table.Record
		key2        *Table.Record
		expectedIDs []int64
	}{
		{
			name:        "Range Query from 2 to 4",
			cmp1:        Table.CMP_GE,
			cmp2:        Table.CMP_LE,
			key1:        (&Table.Record{}).AddInt64("age", 30),
			key2:        (&Table.Record{}).AddInt64("age", 40),
			expectedIDs: []int64{2, 3, 4},
		},
		{
			name:        "Range Query Greater Than 3",
			cmp1:        Table.CMP_GT,
			cmp2:        Table.CMP_LE,
			key1:        (&Table.Record{}).AddInt64("age", 30),
			key2:        (&Table.Record{}).AddInt64("age", 45),
			expectedIDs: []int64{3, 4, 5},
		},
		{
			name:        "Range Query Less Than 2",
			cmp1:        Table.CMP_GE,
			cmp2:        Table.CMP_LE,
			key1:        (&Table.Record{}).AddStr("name", []byte("alice")),
			key2:        (&Table.Record{}).AddStr("name", []byte("bob")),
			expectedIDs: []int64{1, 2},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			scanner := &Table.Scanner{
				Cmp1: tc.cmp1,
				Cmp2: tc.cmp2,
				Key1: *tc.key1,
				Key2: *tc.key2,
			}

			err := db.Scan("users", scanner)
			if err != nil {
				t.Fatalf("Scan failed: %v", err)
			}

			var resultIDs []int64
			for scanner.Valid() {
				var rec Table.Record
				scanner.Deref(&rec)
				resultIDs = append(resultIDs, rec.Get("id").I64)
				scanner.Next()
			}

			if !compareIntSlices(resultIDs, tc.expectedIDs) {
				t.Errorf("Unexpected scan results.\nExpected: %v\nGot: %v", tc.expectedIDs, resultIDs)
			}
		})
	}
}
