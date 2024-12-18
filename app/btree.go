package relixdb

import (
	"encoding/binary"
	"log"
)

type BNode struct {
	data []byte
}

type BTree struct {
	// pointer (a nonzero page number)
	root uint64
	// callbacks for managing on-disk pages
	get func(uint64) BNode // dereference a pointer
	new func(BNode) uint64 // allocate a new page
	del func(uint64)       // deallocate a page
}

func init() {
	node1max := HEADER + 8 + 2 + 4 + BTREE_MAX_KEY_SIZE + BTREE_MAX_VAL_SIZE
	if node1max > BTREE_PAGE_SIZE {
		log.Fatalf("Error: node1max (%d) exceeds BTREE_PAGE_SIZE (%d)", node1max, BTREE_PAGE_SIZE)
	}
}

// Implementing an Assert function
func Assert(cond bool, msg string) {
	if !cond {
		log.Fatalf("Assertion failed: %s", msg)
	}
}

// header
func (node BNode) btype() uint16 {
	return binary.LittleEndian.Uint16(node.data)
}

func (node BNode) nkeys() uint16 {
	return binary.LittleEndian.Uint16(node.data[2:4])
}

func (node BNode) setHeader(btype uint16, nkeys uint16) {
	binary.LittleEndian.PutUint16(node.data[0:2], btype)
	binary.LittleEndian.PutUint16(node.data[2:4], nkeys)
}

// pointers
func (node BNode) getPtr(idx uint16) uint64 {
	Assert(idx < node.nkeys(), "Index out of bounds in getPtr")
	pos := HEADER + 8*int(idx)
	return binary.LittleEndian.Uint64(node.data[pos:])
}

func (node BNode) setPtr(idx uint16, val uint64) {
	Assert(idx < node.nkeys(), "Index out of bounds in setPtr")
	pos := HEADER + 8*int(idx)
	binary.LittleEndian.PutUint64(node.data[pos:], val)
}

// offset list
func offsetPos(node BNode, idx uint16) uint16 {
	Assert(1 <= idx && idx <= node.nkeys(), "Index out of bounds in offsetPos")
	return HEADER + 8*node.nkeys() + 2*(idx-1)
}

func (node BNode) getOffset(idx uint16) uint16 {
	if idx == 0 {
		return 0
	}
	return binary.LittleEndian.Uint16(node.data[offsetPos(node, idx):])
}

func (node BNode) setOffset(idx uint16, offset uint16) {
	binary.LittleEndian.PutUint16(node.data[offsetPos(node, idx):], offset)
}

// key-values
func (node BNode) kvPos(idx uint16) uint16 {
	Assert(idx <= node.nkeys(), "Index out of bounds in kvPos")
	return HEADER + 8*node.nkeys() + 2*node.nkeys() + node.getOffset(idx)
}

func (node BNode) getKey(idx uint16) []byte {
	Assert(idx < node.nkeys(), "Index out of bounds in getKey")
	pos := node.kvPos(idx)
	klen := binary.LittleEndian.Uint16(node.data[pos:])
	return node.data[pos+4 : pos+4+klen]
}

func (node BNode) getVal(idx uint16) []byte {
	Assert(idx < node.nkeys(), "Index out of bounds in getVal")
	pos := node.kvPos(idx)
	klen := binary.LittleEndian.Uint16(node.data[pos:])
	vlen := binary.LittleEndian.Uint16(node.data[pos+2:])
	return node.data[pos+4+klen : pos+4+klen+vlen]
}

// node size in bytes
func (node BNode) nbytes() uint16 {
	return node.kvPos(node.nkeys())
}
