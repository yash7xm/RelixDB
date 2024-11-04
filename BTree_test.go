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
