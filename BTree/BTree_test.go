package BTree

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBTreeInsertLookup(t *testing.T) {
	c := newC()

	// Insert some key-value pairs
	c.add("apple", "red")
	c.add("banana", "yellow")
	c.add("grape", "purple")

	// Use assert.Equal to verify the values in the reference map
	assert.Equal(t, "red", c.ref["apple"], "Expected 'apple' to have value 'red'")
	assert.Equal(t, "yellow", c.ref["banana"], "Expected 'banana' to have value 'yellow'")
	assert.Equal(t, "purple", c.ref["grape"], "Expected 'grape' to have value 'purple'")

	// Use assert.Contains to check if a key exists in the reference map
	assert.Contains(t, c.ref, "apple", "Expected 'apple' to exist in B-Tree")
	assert.Contains(t, c.ref, "banana", "Expected 'banana' to exist in B-Tree")
	assert.Contains(t, c.ref, "grape", "Expected 'grape' to exist in B-Tree")
}

func TestBTreeInsertDuplicate(t *testing.T) {
	c := newC()

	// Insert a key-value pair
	c.add("apple", "red")

	// Insert the same key with a different value
	c.add("apple", "green")

	// Verify that the value for the duplicate key is updated
	assert.Equal(t, "green", c.ref["apple"], "Expected 'apple' to have updated value 'green'")
	assert.Equal(t, 1, len(c.ref), "Expected only one key in the B-Tree")
}

func TestBTreeDelete(t *testing.T) {
	c := newC()

	// Insert some key-value pairs
	c.add("apple", "red")
	c.add("banana", "yellow")
	c.add("grape", "purple")

	// Delete a key
	success := c.del("banana")
	assert.True(t, success, "Expected successful deletion of 'banana'")

	// Use assert.Contains to ensure "banana" no longer exists
	assert.NotContains(t, c.ref, "banana", "Expected 'banana' to not exist after deletion")
	assert.Equal(t, 2, len(c.ref), "Expected only two remaining keys after deletion")

	// Ensure the other keys still exist
	assert.Contains(t, c.ref, "apple", "Expected 'apple' to still exist after deletion")
	assert.Contains(t, c.ref, "grape", "Expected 'grape' to still exist after deletion")
}

func TestBTreeDeleteNonExistentKey(t *testing.T) {
	c := newC()

	// Try deleting a key that doesn't exist
	success := c.del("orange")
	assert.False(t, success, "Expected deletion of 'orange' to fail")
}

func TestBTreeInsertAndSplit(t *testing.T) {
	c := newC()

	// Insert multiple key-value pairs to trigger a split in the B-Tree
	keys := []string{"apple", "banana", "cherry", "date", "elderberry", "fig", "grape"}
	values := []string{"red", "yellow", "dark red", "brown", "purple", "green", "purple"}

	for i := range keys {
		c.add(keys[i], values[i])
	}

	// Check that all keys are correctly added and exist
	for i, key := range keys {
		assert.Equal(t, values[i], c.ref[key], "Expected value for key '%s' to be '%s'", key, values[i])
	}

	// Ensure the tree split happened properly by checking that all keys are still there
	assert.Equal(t, len(keys), len(c.ref), "Expected all keys to be present in the B-Tree after splitting")
}

func TestBTreeLookupNonExistentKey(t *testing.T) {
	c := newC()

	// Insert a key-value pair
	c.add("apple", "red")

	// Attempt to delete a non-existent key
	success := c.del("banana")
	assert.False(t, success, "Expected deletion of non-existent key 'banana' to fail")

	// Ensure the existing key is still present
	assert.Equal(t, "red", c.ref["apple"], "Expected 'apple' to still exist in B-Tree")
	assert.Equal(t, 1, len(c.ref), "Expected only one key in the B-Tree")
}

func TestBTreeInsertDeleteLarge(t *testing.T) {
	c := newC()

	// Insert a large number of keys
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("key_%d", i)
		value := fmt.Sprintf("value_%d", i)
		c.add(key, value)
	}

	// Verify that all keys were inserted correctly
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("key_%d", i)
		value := fmt.Sprintf("value_%d", i)
		assert.Equal(t, value, c.ref[key], "Expected value for '%s' to be '%s'", key, value)
	}

	// Delete some keys
	for i := 50; i < 100; i++ {
		key := fmt.Sprintf("key_%d", i)
		success := c.del(key)
		assert.True(t, success, "Expected successful deletion of '%s'", key)
	}

	// Ensure the deleted keys are no longer in the B-Tree
	for i := 50; i < 100; i++ {
		key := fmt.Sprintf("key_%d", i)
		assert.NotContains(t, c.ref, key, "Expected '%s' to be deleted", key)
	}

	// Ensure the remaining keys still exist
	for i := 0; i < 50; i++ {
		key := fmt.Sprintf("key_%d", i)
		value := fmt.Sprintf("value_%d", i)
		assert.Equal(t, value, c.ref[key], "Expected '%s' to still exist", key)
	}
}



