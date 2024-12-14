package relixdb

import (
	"fmt"
)

// KV transaction
type KVTX struct {
	db *KV
	// for the roolback
	tree struct {
		root uint64
	}
	free struct {
		head uint64
	}
}

// begin a transaction
func (kv *KV) Begin(tx *KVTX) {
	tx.db = kv
	tx.tree.root = kv.tree.root
	tx.free.head = kv.free.head
}

// roolback the tree and other in-memory data structures.
func roolbackTX(tx *KVTX) {
	kv := tx.db
	kv.tree.root = tx.tree.root
	kv.free.head = tx.free.head
	kv.page.nfree = 0
	kv.page.nappend = 0
	kv.page.updates = map[uint64][]byte{}
}

// end a transaction: roolback
func (kv *KV) Abort(tx *KVTX) {
	roolbackTX(tx)
}

// end a transaction: commit updates
func (kv *KV) Commit(tx *KVTX) error {
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
	kv.page.flushed += uint64(kv.page.nappend)
	kv.page.nfree = 0
	kv.page.nappend = 0
	kv.page.updates = map[uint64][]byte{}

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
	// tx.version = kv.version
	// heap.Push(&kv.readers, tx)
	kv.mu.Unlock()
}

func (kv *KV) EndRead(tx *KVReader) {
	kv.mu.Lock()
	// heap.Remove(&kv.readers, tx.index)
	kv.mu.Unlock()
}

// callback for BTree & FreeList, dereference a pointer.
func (tx *KVReader) pageGetMapped(ptr uint64) BNode {
	return BNode{}
}

// func (tx *KVReader) Get(key []byte) ([]byte, bool)
// func (tx *KVReader) Seek(key []byte, cmp int) *BIter
