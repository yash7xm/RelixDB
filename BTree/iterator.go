package BTree


type BIter struct {
	tree *BTree
	path []BNode // from root to leaf
	pos  []uint16      // indexes into nodes
}

// get the current KV pair
func (iter *BIter) Deref() ([]byte, []byte) {
	if !iter.Valid() {
		return []byte(""), []byte("")
	}
	node := iter.path[len(iter.path)-1]
	key := node.getKey(iter.pos[len(iter.pos)-1])
	val := node.getVal(iter.pos[len(iter.pos)-1])
	v := []Value{}
	v = decodeValues(val, v)

	return key, v[0].Str
}

// precondition of the Deref()
func (iter *BIter) Valid() bool {
	return len(iter.path) != 0
}

// moving backward and forward
func (iter *BIter) Next() {
	iterNext(iter, len(iter.path)-1)
}

func (iter *BIter) Prev() {
	iterPrev(iter, len(iter.path)-1)
}

func iterNext(iter *BIter, level int) {
	// Check if we can move right within the current node at this level
	node := iter.path[level]
	if iter.pos[level] < node.nkeys()-1 {
		iter.pos[level]++ // Move right within this node
	} else if level > 0 {
		// If we are at the last key, move up to the parent and then continue
		iterNext(iter, level-1) // Move up to parent node
	} else {
		return // No more keys (we are done)
	}

	// If there are more levels, move to the leftmost child of the next key
	if level+1 < len(iter.pos) {
		node := iter.path[level]
		kid := iter.tree.get(node.getPtr(iter.pos[level])) // Get the child pointer
		iter.path[level+1] = kid                           // Move to the child node
		iter.pos[level+1] = 0                              // Set position at the first key
	}
}

func iterPrev(iter *BIter, level int) {
	if iter.pos[level] > 0 {
		iter.pos[level]-- // move within this node
	} else if level > 0 {
		iterPrev(iter, level-1) // move to a sibling node
	} else {
		return // dummy key
	}

	if level+1 < len(iter.pos) {
		// update the kid node
		node := iter.path[level]
		kid := iter.tree.get(node.getPtr(iter.pos[level]))
		iter.path[level+1] = kid
		iter.pos[level+1] = kid.nkeys() - 1
	}
}
