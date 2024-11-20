package test

// import (
// 	"fmt"
// 	"os"
// 	"testing"

// 	Table "github.com/yash7xm/RelixDB/app"
// )

// func TestSecondaryIndexOperations(t *testing.T) {
// 	// Initialize DB
// 	db := &Table.DB{}
// 	db = db.NewDB(TEST_DB_PATH)

// 	// Open database
// 	if err := db.Open(); err != nil {
// 		t.Fatalf("Failed to open database: %v", err)
// 	}
// 	defer db.Close()
// 	defer os.Remove(TEST_DB_PATH)

// 	// Define test table with secondary indexes
// 	table := &Table.TableDef{
// 		Name:  "users",
// 		Types: []uint32{Table.TYPE_INT64, Table.TYPE_BYTES, Table.TYPE_INT64},
// 		Cols:  []string{"id", "name", "age"},
// 		PKeys: 1,
// 		Indexes: [][]string{
// 			{"name"}, // index on name
// 			{"age"},  // index on age
// 		},
// 	}

// 	// Create the test table
// 	if err := db.TableNew(table); err != nil {
// 		t.Fatalf("Failed to create table: %v", err)
// 	}

// 	// Test cases for various operations
// 	testCases := []struct {
// 		id   int64
// 		name string
// 		age  int64
// 	}{
// 		{1, "Alice", 25},
// 		{2, "Bob", 30},
// 		{3, "Alice", 35},
// 		{4, "Charlie", 25},
// 	}

// 	// Test Insert with Secondary Indexes
// 	t.Run("TestInsertWithSecondaryIndexes", func(t *testing.T) {
// 		for _, tc := range testCases {
// 			record := &Table.Record{}
// 			record.AddInt64("id", tc.id)
// 			record.AddStr("name", []byte(tc.name))
// 			record.AddInt64("age", tc.age)

// 			added, err := db.Insert("users", *record)
// 			if err != nil {
// 				t.Fatalf("Failed to insert record with id %d: %v", tc.id, err)
// 			}
// 			if !added {
// 				t.Errorf("Expected record with id %d to be added", tc.id)
// 			}
// 		}
// 	})

// 	// Test Query by Secondary Index (Name)
// 	t.Run("TestQueryByNameIndex", func(t *testing.T) {
// 		nameQueries := []struct {
// 			name          string
// 			expectedCount int
// 		}{
// 			{"Alice", 2},
// 			{"Bob", 1},
// 			{"Charlie", 1},
// 			{"David", 0},
// 		}

// 		for _, query := range nameQueries {
// 			t.Run(fmt.Sprintf("Query name=%s", query.name), func(t *testing.T) {
// 				queryRecord := &Table.Record{}
// 				queryRecord.AddStr("name", []byte(query.name))

// 				records, err := db.GetByIndex("users","name", queryRecord)
// 				if err != nil {
// 					t.Fatalf("Failed to query by name: %v", err)
// 				}

// 				if len(records) != query.expectedCount {
// 					t.Errorf("Expected %d records for name %s, got %d",
// 						query.expectedCount, query.name, len(records))
// 				}

// 				for _, record := range records {
// 					name := string(record.Get("name").Str)
// 					if name != query.name {
// 						t.Errorf("Expected name %s, got %s", query.name, name)
// 					}
// 				}
// 			})
// 		}
// 	})

// 	// Test Query by Secondary Index (Age)
// 	t.Run("TestQueryByAgeIndex", func(t *testing.T) {
// 		ageQueries := []struct {
// 			age           int64
// 			expectedCount int
// 		}{
// 			{25, 2},
// 			{30, 1},
// 			{35, 1},
// 			{40, 0},
// 		}

// 		for _, query := range ageQueries {
// 			t.Run(fmt.Sprintf("Query age=%d", query.age), func(t *testing.T) {
// 				queryRecord := &Table.Record{}
// 				queryRecord.AddInt64("age", query.age)

// 				records, err := db.GetByIndex("users", "age", queryRecord)
// 				if err != nil {
// 					t.Fatalf("Failed to query by age: %v", err)
// 				}

// 				if len(records) != query.expectedCount {
// 					t.Errorf("Expected %d records for age %d, got %d",
// 						query.expectedCount, query.age, len(records))
// 				}

// 				for _, record := range records {
// 					age := record.Get("age").I64
// 					if age != query.age {
// 						t.Errorf("Expected age %d, got %d", query.age, age)
// 					}
// 				}
// 			})
// 		}
// 	})

// 	// Test Update with Secondary Indexes
// 	t.Run("TestUpdateWithSecondaryIndexes", func(t *testing.T) {
// 		// Update Alice's age
// 		newRecord := &Table.Record{}
// 		newRecord.AddInt64("id", 1)
// 		newRecord.AddStr("name", []byte("Alice"))
// 		newRecord.AddInt64("age", 26)

// 		updated, err := db.Update("users", *newRecord)
// 		if err != nil {
// 			t.Fatalf("Failed to update record: %v", err)
// 		}
// 		if !updated {
// 			t.Error("Expected record to be updated")
// 		}

// 		// Verify update through age index
// 		queryRecord := &Table.Record{}
// 		queryRecord.AddInt64("age", 25)

// 		records, err := db.GetByIndex("users", "age", queryRecord)
// 		if err != nil {
// 			t.Fatalf("Failed to query: %v", err)
// 		}

// 		if len(records) != 1 {
// 			t.Errorf("Expected 1 record with age 25, got %d", len(records))
// 		}

// 		for _, record := range records {
// 			if record.Get("id").I64 == 1 {
// 				t.Error("Found old index entry that should have been updated")
// 			}
// 		}
// 	})

// 	// Test Range Query with Secondary Index
// 	// t.Run("TestRangeQueryWithSecondaryIndex", func(t *testing.T) {
// 	// 	key1Record := &Table.Record{}
// 	// 	key1Record.AddInt64("age", 25)

// 	// 	key2Record := &Table.Record{}
// 	// 	key2Record.AddInt64("age", 30)

// 	// 	records, err := db.GetRange("users", key1Record, key2Record, false)
// 	// 	if err != nil {
// 	// 		t.Fatalf("Failed to perform range query: %v", err)
// 	// 	}

// 	// 	expectedCount := 3 // Records with age 25, 26, and 30
// 	// 	if len(records) != expectedCount {
// 	// 		t.Errorf("Expected %d records in range [25, 30], got %d", expectedCount, len(records))
// 	// 	}

// 	// 	for _, record := range records {
// 	// 		age := record.Get("age").I64
// 	// 		if age < 25 || age > 30 {
// 	// 			t.Errorf("Got record with age %d outside range [25, 30]", age)
// 	// 		}
// 	// 	}
// 	// })

// 	// // Test GetAll
// 	// t.Run("TestGetAll", func(t *testing.T) {
// 	// 	records, err := db.GetAll("users")
// 	// 	if err != nil {
// 	// 		t.Fatalf("Failed to get all records: %v", err)
// 	// 	}

// 	// 	expectedCount := len(testCases) // Minus one for deleted Bob
// 	// 	if len(records) != expectedCount {
// 	// 		t.Errorf("Expected %d total records, got %d", expectedCount, len(records))
// 	// 	}

// 	// 	// Verify records are sorted by primary key
// 	// 	for i := 1; i < len(records); i++ {
// 	// 		prevId := records[i-1].Get("id").I64
// 	// 		currId := records[i].Get("id").I64
// 	// 		if prevId >= currId {
// 	// 			t.Errorf("Records not properly sorted by primary key: %d >= %d", prevId, currId)
// 	// 		}
// 	// 	}
// 	// })

// 	// // Test Delete with Secondary Indexes
// 	// t.Run("TestDeleteWithSecondaryIndexes", func(t *testing.T) {
// 	// 	// Delete Bob's record
// 	// 	deleteRecord := &Table.Record{}
// 	// 	deleteRecord.AddInt64("id", 2)

// 	// 	deleted, err := db.Delete("users", *deleteRecord)
// 	// 	if err != nil {
// 	// 		t.Fatalf("Failed to delete record: %v", err)
// 	// 	}
// 	// 	if !deleted {
// 	// 		t.Error("Expected record to be deleted")
// 	// 	}

// 	// 	// Verify deletion through name index
// 	// 	queryRecord := &Table.Record{}
// 	// 	queryRecord.AddStr("name", []byte("Bob"))

// 	// 	records, err := db.GetByIndex("users", "name", queryRecord)
// 	// 	if err != nil {
// 	// 		t.Fatalf("Failed to query: %v", err)
// 	// 	}

// 	// 	if len(records) > 0 {
// 	// 		t.Error("Found record that should have been deleted")
// 	// 	}

// 	// 	// Verify deletion through age index
// 	// 	queryRecord = &Table.Record{}
// 	// 	queryRecord.AddInt64("age", 30)

// 	// 	records, err = db.GetByIndex("users", "age", queryRecord)
// 	// 	if err != nil {
// 	// 		t.Fatalf("Failed to query: %v", err)
// 	// 	}

// 	// 	if len(records) > 0 {
// 	// 		t.Error("Found index entry that should have been deleted")
// 	// 	}
// 	// })
// }
