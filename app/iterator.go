package relixdb

type BIter struct {
	tree   *BTree
	path   []BNode  // From root to leaf
	pos    []uint16 // Indexes into nodes
	maxKey []byte   // Max key in the BTree
	minKey []byte   // Min key in the BTree
}

// get the current KV pair
func (iter *BIter) Deref() ([]byte, []byte) {
	if !iter.Valid() {
		return nil, nil // Handle invalid state gracefully
	}

	node := iter.path[len(iter.path)-1]
	pos := iter.pos[len(iter.pos)-1]

	key := node.getKey(pos)
	value := node.getVal(pos)

	return key, value
}

// Validate the iterator
func (iter *BIter) Valid() bool {
	if iter == nil || iter.tree == nil {
		return false
	}

	if len(iter.path) == 0 || len(iter.pos) == 0 || len(iter.path) != len(iter.pos) {
		return false
	}

	for i := 0; i < len(iter.path); i++ {
		node := iter.path[i]
		pos := iter.pos[i]

		if node.nkeys() == 0 || pos >= node.nkeys() {
			return false
		}

		if i < len(iter.path)-1 && node.getPtr(pos) == 0 {
			return false
		}
	}

	return true
}

// Move forward
func (iter *BIter) Next() {
	if !iter.Valid() {
		return
	}

	if !iterNext(iter, len(iter.path)-1) {
		// If no more keys, move to maxKey
		iter.path = nil
		iter.pos = nil
	}
}

// Move backward
func (iter *BIter) Prev() {
	if !iter.Valid() {
		return
	}

	if !iterPrev(iter, len(iter.path)-1) {
		// If no more keys, move to minKey
		iter.path = nil
		iter.pos = nil
	}
}

func iterNext(iter *BIter, level int) bool {
	node := iter.path[level]
	if iter.pos[level] < node.nkeys()-1 {
		iter.pos[level]++
		return true
	} else if level > 0 {
		if iterNext(iter, level-1) {
			node := iter.path[level]
			kid := iter.tree.get(node.getPtr(iter.pos[level]))
			iter.path[level+1] = kid
			iter.pos[level+1] = 0
			return true
		}
	}
	return false // No more keys
}

func iterPrev(iter *BIter, level int) bool {
	// node := iter.path[level]
	if iter.pos[level] > 0 {
		iter.pos[level]--
		return true
	} else if level > 0 {
		if iterPrev(iter, level-1) {
			node := iter.path[level]
			kid := iter.tree.get(node.getPtr(iter.pos[level]))
			iter.path[level+1] = kid
			iter.pos[level+1] = kid.nkeys() - 1
			return true
		}
	}
	return false // No more keys
}

// Set max and min keys explicitly for boundary cases
func (iter *BIter) SetBounds(minKey, maxKey []byte) {
	iter.minKey = minKey
	iter.maxKey = maxKey
}

// Deref when the iterator is out of bounds
func (iter *BIter) DerefOutOfBounds() ([]byte, []byte) {
	if iter.path == nil || iter.pos == nil {
		if iter.minKey != nil && iter.maxKey != nil {
			if len(iter.path) == 0 { // Before the first key
				return iter.minKey, nil
			}
			return iter.maxKey, nil
		}
	}
	return nil, nil
}
