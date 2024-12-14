package test

import (
	"testing"

	relixdb "github.com/yash7xm/RelixDB/app"
)

func TestTransactionCommitAndRollback(t *testing.T) {
	// Initialize the database
	db := &relixdb.DB{}
	db = db.NewDB(TEST_DB_PATH)

	// Open the database with proper error handling
	if err := db.Open(); err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Define a sample table for testing
	sampleTable := &relixdb.TableDef{
		Name:    "test_table",
		Types:   []uint32{relixdb.TYPE_INT64, relixdb.TYPE_BYTES},
		Cols:    []string{"id", "name"},
		PKeys:   1,
		Indexes: [][]string{},
	}

	// Create a transaction for table creation
	tx := &relixdb.DBTX{}
	db.Begin(tx)

	// Create the table
	if err := tx.TableNew(sampleTable); err != nil {
		t.Fatalf("Failed to create sample table: %v", err)
	}

	// Commit the table creation
	if err := db.Commit(tx); err != nil {
		t.Fatalf("Failed to commit table creation: %v", err)
	}

	// Begin a new transaction for data manipulation
	tx = &relixdb.DBTX{}
	db.Begin(tx)

	// Insert a record
	record := (&relixdb.Record{}).
		AddInt64("id", 1).
		AddStr("name", []byte("Test Name"))

	if _, err := tx.Set("test_table", *record, 0); err != nil {
		t.Fatalf("Failed to insert test data: %v", err)
	}

	// Verify the inserted record
	queryRecord := (&relixdb.Record{}).AddInt64("id", 1)
	found, err := tx.Get("test_table", queryRecord)
	if err != nil {
		t.Fatalf("Failed to query test data: %v", err)
	}
	if !found || string(queryRecord.Get("name").Str) != "Test Name" {
		t.Errorf("Expected name 'Test Name', got '%s'", string(queryRecord.Get("name").Str))
	}

	// Rollback the transaction
	db.Abort(tx)

	// Verify that the record does not exist after rollback
	tx = &relixdb.DBTX{}
	db.Begin(tx)
	queryRecord = (&relixdb.Record{}).AddInt64("id", 1)
	found, err = tx.Get("test_table", queryRecord)
	if err != nil {
		t.Fatalf("Failed to query test data after rollback: %v", err)
	}
	if found {
		t.Fatal("Record should not exist after rollback")
	}

	// Reinsert the record and commit
	db.Begin(tx)
	if _, err := tx.Set("test_table", *record, 0); err != nil {
		t.Fatalf("Failed to reinsert test data: %v", err)
	}
	if err := db.Commit(tx); err != nil {
		t.Fatalf("Failed to commit transaction: %v", err)
	}

	// Verify the record exists after commit
	tx = &relixdb.DBTX{}
	db.Begin(tx)
	queryRecord = (&relixdb.Record{}).AddInt64("id", 1)
	found, err = tx.Get("test_table", queryRecord)
	if err != nil {
		t.Fatalf("Failed to query test data after commit: %v", err)
	}
	if !found || string(queryRecord.Get("name").Str) != "Test Name" {
		t.Errorf("Expected name 'Test Name', got '%s'", string(queryRecord.Get("name").Str))
	}
}
