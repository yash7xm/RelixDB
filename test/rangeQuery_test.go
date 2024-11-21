package test

import (
	"fmt"
	"os"
	"testing"

	Table "github.com/yash7xm/RelixDB/app"
)

const TEST_DB_PATH = "../test.db"

func TestSetAndScan(t *testing.T) {
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
		Indexes: [][]string{},
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
			key1:        (&Table.Record{}).AddInt64("id", 2),
			key2:        (&Table.Record{}).AddInt64("id", 4),
			expectedIDs: []int64{2, 3, 4},
		},
		{
			name:        "Range Query Greater Than 3",
			cmp1:        Table.CMP_GT,
			cmp2:        Table.CMP_LE,
			key1:        (&Table.Record{}).AddInt64("id", 1),
			key2:        (&Table.Record{}).AddInt64("id", 4),
			expectedIDs: []int64{2, 3, 4},
		},
		{
			name:        "Range Query Less Than 2",
			cmp1:        Table.CMP_GE,
			cmp2:        Table.CMP_LE,
			key1:        (&Table.Record{}).AddInt64("id", 1),
			key2:        (&Table.Record{}).AddInt64("id", 2),
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

// Helper function to compare integer slices
func compareIntSlices(a, b []int64) bool {
	if len(a) != len(b) {
		return false
	}
	for i, v := range a {
		if v != b[i] {
			return false
		}
	}
	return true
}

func TestScanner(t *testing.T) {
	// Initialize DB with proper structure
	db := &Table.DB{}
	db = db.NewDB(TEST_DB_PATH)

	// Open database with proper error handling
	if err := db.Open(); err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()
	defer os.Remove(TEST_DB_PATH)

	// Define test table schema
	table := &Table.TableDef{
		Name:    "users",
		Types:   []uint32{Table.TYPE_INT64, Table.TYPE_BYTES, Table.TYPE_INT64},
		Cols:    []string{"id", "name", "age"},
		PKeys:   1,
		Indexes: [][]string{},
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

		_, err := db.Insert("users", *record)
		if err != nil {
			t.Fatalf("Failed to insert record with id %d: %v", tc.id, err)
		}
	}

	scanner := &Table.Scanner{
		Cmp1: Table.CMP_GE,
		Cmp2: Table.CMP_LE,
		Key1: *(&Table.Record{}).AddInt64("id", 1),
		Key2: *(&Table.Record{}).AddInt64("id", 5),
	}

	err = db.Scan("users", scanner)
	if err != nil {
		t.Fatalf("Scan failed: %v", err)
	}

	var resultIDs []int64
	for scanner.Valid() {
		var rec Table.Record
		scanner.Deref(&rec)
		resultIDs = append(resultIDs, rec.Get("id").I64)
		fmt.Println(rec.Get("id").I64)
		scanner.Next()
	}

	for _, res := range resultIDs {
		fmt.Print(res)
	}
}
