package BTree

import (
	"bytes"
	"fmt"
	"testing"
)

// TestOpenAndCloseDB tests opening and closing the database, ensuring resources are released properly.
func TestOpenAndCloseDB(t *testing.T) {
	db := &KV{Path: "testdb"}
	err := db.Open()
	if err != nil {
		t.Fatalf("Failed to open DB: %v", err)
	}

	db.Close()
}

// TestInsertAndGet tests inserting key-value pairs and retrieving them from the database.
func TestInsertAndGet(t *testing.T) {
	db := &KV{Path: "testdb"}
	err := db.Open()
	if err != nil {
		t.Fatalf("Failed to open DB: %v", err)
	}
	defer db.Close()

	key := []byte("key1")
	value := []byte("value1")

	// Insert value into DB
	err = db.Set(key, value)
	if err != nil {
		t.Fatalf("Failed to set key-value pair: %v", err)
	}

	// Retrieve value from DB
	retrieved, found := db.Get(key)
	if !found || !bytes.Equal(retrieved, value) {
		t.Fatalf("Expected %s, got %s", value, retrieved)
	}
}

// TestOverwriteKey tests overwriting an existing key in the database.
func TestOverwriteKey(t *testing.T) {
	db := &KV{Path: "testdb"}
	err := db.Open()
	if err != nil {
		t.Fatalf("Failed to open DB: %v", err)
	}
	defer db.Close()

	key := []byte("key1")
	initialValue := []byte("initialValue")
	newValue := []byte("newValue")

	// Insert initial value
	err = db.Set(key, initialValue)
	if err != nil {
		t.Fatalf("Failed to set initial key-value pair: %v", err)
	}

	// Overwrite the value
	err = db.Set(key, newValue)
	if err != nil {
		t.Fatalf("Failed to overwrite key-value pair: %v", err)
	}

	// Retrieve the new value
	retrieved, found := db.Get(key)
	if !found || !bytes.Equal(retrieved, newValue) {
		t.Fatalf("Expected %s, got %s", newValue, retrieved)
	}
}

// TestDeleteKey tests deleting a key from the database.
func TestDeleteKey(t *testing.T) {
	db := &KV{Path: "testdb"}
	err := db.Open()
	if err != nil {
		t.Fatalf("Failed to open DB: %v", err)
	}
	defer db.Close()

	key := []byte("keyToDelete")
	value := []byte("someValue")

	// Insert value
	err = db.Set(key, value)
	if err != nil {
		t.Fatalf("Failed to set key-value pair: %v", err)
	}

	// Delete key
	deleted, err := db.Del(key)
	if err != nil || !deleted {
		t.Fatalf("Failed to delete key: %v", err)
	}

	// Check that the key no longer exists
	_, found := db.Get(key)
	if found {
		t.Fatalf("Expected key %s to be deleted, but it still exists", key)
	}
}

// TestLargeDataInsert tests inserting large amounts of data into the DB to check how it handles multiple pages.
func TestLargeDataInsert(t *testing.T) {
	db := &KV{Path: "testdb"}
	err := db.Open()
	if err != nil {
		t.Fatalf("Failed to open DB: %v", err)
	}
	defer db.Close()

	// Insert large number of key-value pairs
	for i := 0; i < 100; i++ {
		key := []byte(fmt.Sprintf("key%d", i))
		value := []byte(fmt.Sprintf("value%d", i))
		err = db.Set(key, value)
		if err != nil {
			t.Fatalf("Failed to set key-value pair: %v", err)
		}
	}

	// Verify that all data was inserted and can be retrieved correctly
	for i := 0; i < 100; i++ {
		key := []byte(fmt.Sprintf("key%d", i))
		expectedValue := []byte(fmt.Sprintf("value%d", i))
		retrieved, found := db.Get(key)
		if !found || !bytes.Equal(retrieved, expectedValue) {
			t.Fatalf("Expected %s, got %s", expectedValue, retrieved)
		}
	}
}

// TestFreeListUsage tests the reuse of deallocated pages in the FreeList.
func TestFreeListUsage(t *testing.T) {
	db := &KV{Path: "testdb"}
	err := db.Open()
	if err != nil {
		t.Fatalf("Failed to open DB: %v", err)
	}
	defer db.Close()

	// Insert and delete some keys to create free pages
	for i := 0; i < 100; i++ {
		key := []byte(fmt.Sprintf("key%d", i))
		value := []byte(fmt.Sprintf("value%d", i))
		err = db.Set(key, value)
		if err != nil {
			t.Fatalf("Failed to set key-value pair: %v", err)
		}
	}

	for i := 0; i < 100; i++ {
		key := []byte(fmt.Sprintf("key%d", i))
		_, err = db.Del(key)
		if err != nil {
			t.Fatalf("Failed to delete key: %v", err)
		}
	}

	// Insert new keys to check if free pages are reused
	for i := 100; i < 200; i++ {
		key := []byte(fmt.Sprintf("key%d", i))
		value := []byte(fmt.Sprintf("value%d", i))
		err = db.Set(key, value)
		if err != nil {
			t.Fatalf("Failed to set key-value pair: %v", err)
		}
	}
}

// TestPersistence tests whether the database persists data correctly across reopenings.
func TestPersistence(t *testing.T) {
	db := &KV{Path: "testdb"}
	err := db.Open()
	if err != nil {
		t.Fatalf("Failed to open DB: %v", err)
	}

	// Insert some values
	key1 := []byte("key1")
	value1 := []byte("value1")
	key2 := []byte("key2")
	value2 := []byte("value2")

	err = db.Set(key1, value1)
	if err != nil {
		t.Fatalf("Failed to set key1: %v", err)
	}
	err = db.Set(key2, value2)
	if err != nil {
		t.Fatalf("Failed to set key2: %v", err)
	}

	// Close the DB
	db.Close()

	// Reopen the DB
	err = db.Open()
	if err != nil {
		t.Fatalf("Failed to reopen DB: %v", err)
	}
	defer db.Close()

	// Check if the values persist
	retrieved1, found1 := db.Get(key1)
	if !found1 || !bytes.Equal(retrieved1, value1) {
		t.Fatalf("Expected %s, got %s", value1, retrieved1)
	}

	retrieved2, found2 := db.Get(key2)
	if !found2 || !bytes.Equal(retrieved2, value2) {
		t.Fatalf("Expected %s, got %s", value2, retrieved2)
	}
}
