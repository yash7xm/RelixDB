package test

import (
	"fmt"
	"testing"

	Table "github.com/yash7xm/RelixDB/app"
)

const TEST_DB_PATH = "../test.db"

func TestSampleTestData(t *testing.T) {
	// Initialize the DB correctly
	db := &Table.DB{}
	db = db.NewDB(TEST_DB_PATH)

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

// TestTableCreation tests table creation with various schemas
func TestTableCreation(t *testing.T) {
	// Initialize the DB correctly
	db := &Table.DB{}
	db = db.NewDB(TEST_DB_PATH)

	// Open the database with proper error handling
	if err := db.Open(); err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	tests := []struct {
		name    string
		table   *Table.TableDef
		wantErr bool
	}{
		{
			name: "Valid simple table",
			table: &Table.TableDef{
				Name:    "test_table101",
				Types:   []uint32{Table.TYPE_INT64, Table.TYPE_BYTES},
				Cols:    []string{"id", "name"},
				PKeys:   1,
				Indexes: [][]string{},
			},
			wantErr: false,
		},
		{
			name: "Valid complex table",
			table: &Table.TableDef{
				Name:    "test_table102",
				Types:   []uint32{Table.TYPE_INT64, Table.TYPE_BYTES, Table.TYPE_INT64, Table.TYPE_BYTES},
				Cols:    []string{"id", "name", "age", "email"},
				PKeys:   1,
				Indexes: [][]string{{"name"}, {"age"}},
			},
			wantErr: false,
		},
		{
			name: "Invalid - mismatched types and columns",
			table: &Table.TableDef{
				Name:    "test_table103",
				Types:   []uint32{Table.TYPE_INT64},
				Cols:    []string{"id", "name"},
				PKeys:   1,
				Indexes: [][]string{},
			},
			wantErr: true,
		},
		{
			name: "Invalid - no primary key",
			table: &Table.TableDef{
				Name:    "test_table104",
				Types:   []uint32{Table.TYPE_INT64, Table.TYPE_BYTES},
				Cols:    []string{"id", "name"},
				PKeys:   0,
				Indexes: [][]string{},
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := db.TableNew(tt.table)
			if (err != nil) != tt.wantErr {
				t.Errorf("TableNew() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

// TestCRUDOperations tests Create, Read, Update, Delete operations
func TestCRUDOperations(t *testing.T) {
	// Initialize the DB correctly
	db := &Table.DB{}
	db = db.NewDB(TEST_DB_PATH)

	// Open the database with proper error handling
	if err := db.Open(); err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Create test table
	table := &Table.TableDef{
		Name:    "crud_test",
		Types:   []uint32{Table.TYPE_INT64, Table.TYPE_BYTES, Table.TYPE_INT64},
		Cols:    []string{"id", "name", "age"},
		PKeys:   1,
		Indexes: [][]string{},
	}

	if err := db.TableNew(table); err != nil {
		t.Fatalf("Failed to create test table: %v", err)
	}

	// Test Insert
	t.Run("Insert", func(t *testing.T) {
		record := (&Table.Record{}).
			AddInt64("id", 1).
			AddStr("name", []byte("test user")).
			AddInt64("age", 25)

		_, err := db.Insert("crud_test", *record)
		if err != nil {
			t.Errorf("Insert failed: %v", err)
		}
	})

	// Test Get
	t.Run("Get", func(t *testing.T) {
		record := (&Table.Record{}).AddInt64("id", 1)
		found, err := db.Get("crud_test", record)
		if err != nil {
			t.Errorf("Get failed: %v", err)
		}
		if !found {
			t.Error("Record not found")
		}
		if string(record.Get("name").Str) != "test user" {
			t.Errorf("Expected name 'test user', got '%s'", string(record.Get("name").Str))
		}
	})

	// Test Get Non-existent Record
	t.Run("Get Non-existent", func(t *testing.T) {
		record := (&Table.Record{}).AddInt64("id", 999)
		found, err := db.Get("crud_test", record)
		if err != nil {
			t.Errorf("Get failed: %v", err)
		}
		if found {
			t.Error("Found non-existent record")
		}
	})
}

// TestEdgeCases tests various edge cases and error conditions
func TestEdgeCases(t *testing.T) {
	// Initialize the DB correctly
	db := &Table.DB{}
	db = db.NewDB(TEST_DB_PATH)

	// Open the database with proper error handling
	if err := db.Open(); err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Test getting from non-existent table
	t.Run("Get from non-existent table", func(t *testing.T) {
		record := (&Table.Record{}).AddInt64("id", 1)
		_, err := db.Get("non_existent_table", record)
		if err == nil {
			t.Error("Expected error when getting from non-existent table")
		}
	})

	// Test creating table with invalid name
	t.Run("Create table with invalid name", func(t *testing.T) {
		table := &Table.TableDef{
			Name:    "", // Empty name
			Types:   []uint32{Table.TYPE_INT64},
			Cols:    []string{"id"},
			PKeys:   1,
			Indexes: [][]string{},
		}
		err := db.TableNew(table)
		if err == nil {
			t.Error("Expected error when creating table with empty name")
		}
	})

	// Test duplicate table creation
	t.Run("Create duplicate table", func(t *testing.T) {
		table := &Table.TableDef{
			Name:    "duplicate_test",
			Types:   []uint32{Table.TYPE_INT64},
			Cols:    []string{"id"},
			PKeys:   1,
			Indexes: [][]string{},
		}

		// First creation should succeed
		err := db.TableNew(table)
		if err != nil {
			t.Fatalf("Failed to create initial table: %v", err)
		}

		// Second creation should fail
		err = db.TableNew(table)
		if err == nil {
			t.Error("Expected error when creating duplicate table")
		}
	})
}

// TestDataTypes tests handling of different data types
func TestDataTypes(t *testing.T) {
	// Initialize the DB correctly
	db := &Table.DB{}
	db = db.NewDB(TEST_DB_PATH)

	// Open the database with proper error handling
	if err := db.Open(); err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	table := &Table.TableDef{
		Name:    "types_test",
		Types:   []uint32{Table.TYPE_INT64, Table.TYPE_BYTES, Table.TYPE_INT64},
		Cols:    []string{"id", "data", "value"},
		PKeys:   1,
		Indexes: [][]string{},
	}

	if err := db.TableNew(table); err != nil {
		t.Fatalf("Failed to create test table: %v", err)
	}

	tests := []struct {
		name    string
		record  Table.Record
		wantErr bool
	}{
		{
			name: "Valid types",
			record: *(&Table.Record{}).
				AddInt64("id", 1).
				AddStr("data", []byte("test")).
				AddInt64("value", 100),
			wantErr: false,
		},
		{
			name: "Large values",
			record: *(&Table.Record{}).
				AddInt64("id", 2).
				AddStr("data", make([]byte, 1024*1024)). // 1MB of data
				AddInt64("value", 1<<60),
			wantErr: false,
		},
		// Add more test cases for different data types and edge cases
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := db.Insert("types_test", tt.record)
			if (err != nil) != tt.wantErr {
				t.Errorf("Insert() error = %v, wantErr %v", err, tt.wantErr)
			}

			if err == nil {
				// Verify the inserted data
				queryRecord := (&Table.Record{}).AddInt64("id", tt.record.Get("id").I64)
				found, err := db.Get("types_test1", queryRecord)
				if err != nil {
					t.Errorf("Get() error = %v", err)
				}
				if !found {
					t.Error("Record not found after insertion")
				}
			}
		})
	}
}
