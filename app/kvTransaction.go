package relixdb

import (
	"bytes"
	"container/heap"
	"fmt"
)

// KV transaction
type KVTX struct {
	KVReader
	db   *KV
	free FreeList
	page struct {
		nappend int // number of pages to be appended
		// newly allocated or deallocated pages keyed by the pointer.
		// nil value denotes a deallocated page.
		updates map[uint64][]byte
	}
}

// begin a transaction
func (kv *KV) Begin(tx *KVTX) {
	tx.db = kv
	tx.page.updates = map[uint64][]byte{}
	tx.mmap.chunks = kv.mmap.chunks
	kv.writer.Lock()
	tx.version = kv.version
	// btree
	tx.tree.root = kv.tree.root
	tx.tree.get = tx.pageGet
	tx.tree.new = tx.pageNew
	tx.tree.del = tx.pageDel
	// freelist
	tx.free.FreeListData = kv.free.FreeListData
	tx.free.version = kv.version
	tx.free.get = tx.pageGet
	tx.free.new = tx.pageAppend
	tx.free.use = tx.pageUse
	tx.free.minReader = kv.version
	kv.mu.Lock()
	if len(kv.readers) > 0 {
		tx.free.minReader = kv.readers[0].version
	}
	kv.mu.Unlock()
}

// roolback the tree and other in-memory data structures.
func roolbackTX(tx *KVTX) {
	kv := tx.db
	kv.tree.root = tx.tree.root
	kv.free.head = tx.free.head
	kv.page.nfree = 0
	kv.page.nappend = 0
	kv.page.updates = make(map[uint64][]byte)
}

// end a transaction: roolback
func (kv *KV) Abort(tx *KVTX) {
	kv.writer.Unlock()
}

// end a transaction: commit updates
func (kv *KV) Commit(tx *KVTX) error {
	defer kv.writer.Unlock()
	if kv.tree.root == tx.tree.root {
		return nil // no updates?
	}

	// phase 1: persist the page data to disk.
	if err := writePages(kv); err != nil {
		roolbackTX(tx)
		return err
	}

	// the page data must reach disk before the master page.
	// the `fsync` serves as a barrier here.
	if err := kv.fp.Sync(); err != nil {
		roolbackTX(tx)
		return fmt.Errorf("fsync: %w", err)
	}

	// the transaction is visible at this point.
	// save the new version of in-memory data structures.
	kv.page.flushed += uint64(tx.page.nappend)
	kv.free = tx.free
	kv.mu.Lock()
	kv.tree.root = tx.tree.root
	kv.version++
	kv.mu.Unlock()

	// phase 2: update the master page to point to the new tree.
	// NOTE: Cannot rollback the tree to the old version if phase 2 fails.
	//		 Because there is no way to know the state of the master page.
	//		 Updating from an old root can cause corruption.

	if err := masterStore(kv); err != nil {
		return err
	}
	if err := kv.fp.Sync(); err != nil {
		return fmt.Errorf("fsync: %w", err)
	}
	return nil
}

// KV operations
func (tx *KVTX) Get(key []byte) ([]byte, bool) {
	return tx.db.tree.Get(key)
}

func (tx *KVTX) Seek(key []byte, cmp int) *BIter {
	return tx.db.tree.Seek(key, cmp)
}

func (tx *KVTX) Update(req *InsertReq) bool {
	tx.db.InsertEx(req)
	return req.Added
}

func (tx *KVTX) Del(key []byte) (bool, error) {
	return tx.db.Del(key)
}

// read-only KV transactions
type KVReader struct {
	// the snapshot
	version uint64
	tree    BTree
	mmap    struct {
		chunks [][]byte // copied from struct KV. read-only.
	}
	// for removing from the heap
	index int
}

func (kv *KV) BeginRead(tx *KVReader) {
	kv.mu.Lock()
	tx.mmap.chunks = kv.mmap.chunks
	tx.tree.root = kv.tree.root
	tx.tree.get = tx.pageGetMapped
	tx.version = kv.version
	heap.Push(&kv.readers, tx)
	kv.mu.Unlock()
}

func (kv *KV) EndRead(tx *KVReader) {
	kv.mu.Lock()
	heap.Remove(&kv.readers, tx.index)
	kv.mu.Unlock()
}

// callback for BTree & FreeList, dereference a pointer.
func (tx *KVReader) pageGetMapped(ptr uint64) BNode {
	return BNode{}
}

// Get retrieves the value associated with the key from the read-only transaction.
func (tx *KVReader) Get(key []byte) ([]byte, bool) {
	// We use the `Seek` method for KVReader which returns the iterator to find the key
	iter := tx.Seek(key, CMP_LE)
	if iter.Valid() {
		currKey, currVal := iter.Deref()
		if bytes.Equal(currKey, key) {
			return currVal, true
		}
	}
	return nil, false
}

// Seek returns an iterator to the closest position based on the comparison.
func (tx *KVReader) Seek(key []byte, cmp int) *BIter {
	return tx.tree.Seek(key, cmp)
}

// btree utility functions
func (tx *KVTX) pageGet(ptr uint64) BNode {
	return tx.db.pageGet(ptr)
}

func (tx *KVTX) pageNew(node BNode) uint64 {
	return tx.db.pageNew(node)
}

func (tx *KVTX) pageDel(ptr uint64) {
	tx.db.pageDel(ptr)
}

func (tx *KVTX) pageAppend(node BNode) uint64 {
	return tx.db.pageAppend(node)
}

func (tx *KVTX) pageUse(ptr uint64, node BNode) {
	tx.db.pageUse(ptr, node)
}
