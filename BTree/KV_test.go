package BTree

import (
	"os"
	"testing"
)

// Helper function to create a temporary file for testing
func createTempFile(t *testing.T) string {
	t.Helper()
	tmpfile, err := os.CreateTemp("", "btree_test.db")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	return tmpfile.Name()
}

// Test case to test the opening and closing of a database file.
func TestKV_OpenClose(t *testing.T) {
	// Create a temporary file for testing
	path := createTempFile(t)
	defer os.Remove(path) // Clean up

	kv := KV{Path: path}

	// Test opening the KV store
	if err := kv.Open(); err != nil {
		t.Fatalf("KV.Open() failed: %v", err)
	}

	// Test closing the KV store
	kv.Close()
}

// Test case for setting and getting a key-value pair.
func TestKV_SetGet(t *testing.T) {
	path := createTempFile(t)
	defer os.Remove(path)

	kv := KV{Path: path}

	if err := kv.Open(); err != nil {
		t.Fatalf("KV.Open() failed: %v", err)
	}
	defer kv.Close()


	// Insert key-value pair
	key := []byte("hello")
	val := []byte("world")

	if err := kv.Set(key, val); err != nil {
		t.Fatalf("KV.Set() failed: %v", err)
	}

	retrievedVal, found := kv.Get(key)
	if !found || string(retrievedVal) != string(val) {
		t.Fatalf("KV.Get() failed: expected %s, got %s", val, retrievedVal)
	}

	key = []byte("a")
	val = []byte("aa")

	if err := kv.Set(key, val); err != nil {
		t.Fatalf("KV.Set() failed: %v", err)
	}

	retrievedVal, found = kv.Get(key)
	if !found || string(retrievedVal) != string(val) {
		t.Fatalf("KV.Get() failed: expected %s, got %s", val, retrievedVal)
	}

	key = []byte("b")
	val = []byte("bb")

	if err := kv.Set(key, val); err != nil {
		t.Fatalf("KV.Set() failed: %v", err)
	}

	// Retrieve the value for the same key
	retrievedVal, found = kv.Get(key)
	if !found || string(retrievedVal) != string(val) {
		t.Fatalf("KV.Get() failed: expected %s, got %s", val, retrievedVal)
	}

	kv.Close()
	if err := kv.Open(); err != nil {
		t.Fatalf("KV.Open() failed: %v", err)
	}
}

// Test case for deleting a key and ensuring it is no longer accessible.
func TestKV_Delete(t *testing.T) {
	path := createTempFile(t)
	defer os.Remove(path)

	kv := KV{Path: path}

	if err := kv.Open(); err != nil {
		t.Fatalf("KV.Open() failed: %v", err)
	}
	defer kv.Close()

	// Insert a key-value pair
	key := []byte("delete_me")
	val := []byte("bye")

	if err := kv.Set(key, val); err != nil {
		t.Fatalf("KV.Set() failed: %v", err)
	}

	// Delete the key
	deleted, err := kv.Del(key)
	if err != nil {
		t.Fatalf("KV.Del() failed: %v", err)
	}
	if !deleted {
		t.Fatalf("KV.Del() failed: key was not deleted")
	}

	// Ensure the key is no longer accessible
	_, found := kv.Get(key)
	if found {
		t.Fatalf("KV.Get() after delete failed: expected key to be deleted")
	}
}

// Test case for attempting to get a non-existent key.
func TestKV_GetNonExistentKey(t *testing.T) {
	path := createTempFile(t)
	defer os.Remove(path)

	kv := KV{Path: path}

	if err := kv.Open(); err != nil {
		t.Fatalf("KV.Open() failed: %v", err)
	}
	defer kv.Close()

	// Attempt to retrieve a key that doesn't exist
	_, found := kv.Get([]byte("non_existent_key"))
	if found {
		t.Fatalf("KV.Get() failed: expected non-existent key to return false")
	}
}

// Test case for handling an empty database.
func TestKV_EmptyDB(t *testing.T) {
	path := createTempFile(t)
	defer os.Remove(path)

	kv := KV{Path: path}

	if err := kv.Open(); err != nil {
		t.Fatalf("KV.Open() failed: %v", err)
	}
	defer kv.Close()

	// The database should be empty
	_, found := kv.Get([]byte("any_key"))
	if found {
		t.Fatalf("KV.Get() failed: expected no entries in the empty database")
	}
}

// Test case to ensure master page loading and saving work correctly.
func TestKV_MasterPage(t *testing.T) {
	path := createTempFile(t)
	defer os.Remove(path)

	kv := KV{Path: path}

	if err := kv.Open(); err != nil {
		t.Fatalf("KV.Open() failed: %v", err)
	}
	defer kv.Close()

	// Insert key-value pair
	key := []byte("persist_key")
	val := []byte("persist_value")
	if err := kv.Set(key, val); err != nil {
		t.Fatalf("KV.Set() failed: %v", err)
	}

	// Close and re-open the database to test master page persistence
	kv.Close()
	if err := kv.Open(); err != nil {
		t.Fatalf("KV.Open() failed: %v", err)
	}

	// Ensure the key-value pair is still present
	retrievedVal, found := kv.Get(key)
	if !found || string(retrievedVal) != string(val) {
		t.Fatalf("KV.Get() after re-open failed: expected %s, got %s", val, retrievedVal)
	}
}
