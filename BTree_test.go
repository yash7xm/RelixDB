package main

import (
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

    // Verify the reference map contains the updated value
    assert.Equal(t, "green", c.ref["apple"], "Expected 'apple' to be updated to 'green'")

    // Use assert.Contains to verify the key still exists in the reference map
    assert.Contains(t, c.ref, "apple", "Expected 'apple' to still exist in B-Tree")
}

func TestBTreeDelete(t *testing.T) {
    c := newC()

    // Insert some key-value pairs
    c.add("apple", "red")
    c.add("banana", "yellow")

    // Delete a key
    success := c.del("apple")
    assert.True(t, success, "Expected successful deletion of 'apple'")

    // Verify the key has been removed from the reference map
    assert.NotContains(t, c.ref, "apple", "Expected 'apple' to be deleted from reference map")

    // Ensure the other key still exists
    assert.Contains(t, c.ref, "banana", "Expected 'banana' to still exist in B-Tree")
}


