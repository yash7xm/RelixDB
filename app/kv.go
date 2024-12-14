package relixdb

import (
	"fmt"
	"os"
	"sync"
	"syscall"
)

type KV struct {
	Path string
	// internals
	fp   *os.File
	tree BTree
	mmap struct {
		file   int      // file size, can be larger than the database size
		total  int      // mmap size, can be larger than the file size
		chunks [][]byte // mutliple mmaps, can be non-continuous
	}
	page struct {
		flushed uint64 // database size in number of pages
		nfree   int    // number of pages taken from the free list
		nappend int    // number of pages to be appended
		// newly allocated or deallocated pages keyed by the pointer.
		// nil value denotes a deallocated page
		updates map[uint64][]byte
	}
	free   FreeList
	mu     sync.Mutex
	writer sync.Mutex
	// version number and reader list
	version uint64
	readers ReaderList // heap, for tracking the minimum reader version
}

// implements heap.Interface
type ReaderList []*KVReader

// Len returns the number of readers.
func (r *ReaderList) Len() int {
    return len(*r)
}

// Less compares reader versions for heap ordering.
func (r *ReaderList) Less(i, j int) bool {
    return (*r)[i].version < (*r)[j].version
}

// Swap swaps two elements in the reader list.
func (r *ReaderList) Swap(i, j int) {
    (*r)[i], (*r)[j] = (*r)[j], (*r)[i]
}

// Push adds a new reader to the heap.
func (r *ReaderList) Push(x any) {
    *r = append(*r, x.(*KVReader))
}

// Pop removes the minimum reader (lowest version).
func (r *ReaderList) Pop() any {
    old := *r
    n := len(old)
    x := old[n-1]
    *r = old[0 : n-1]
    return x
}

func (db *KV) Open() (err error) {
	// open or create the DB file
	fp, err := os.OpenFile(db.Path, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return fmt.Errorf("OpenFile: %w", err)
	}
	db.fp = fp

	// create the initial mmap
	sz, chunk, err := mmapInit(db.fp)
	if err != nil {
		db.Close() // Ensure resources are released
		return fmt.Errorf("mmap init error: %w", err)
	}

	db.mmap.file = sz
	db.mmap.total = len(chunk)
	db.mmap.chunks = [][]byte{chunk}

	db.page.updates = make(map[uint64][]byte)

	// BTree callbacks
	db.tree.get = db.pageGet
	db.tree.new = db.pageNew
	db.tree.del = db.pageDel

	// FreeList callbacks
	db.free.get = db.pageGet
	db.free.new = db.pageAppend
	db.free.use = db.pageUse

	// read the master page
	err = masterLoad(db)
	if err != nil {
		db.Close() // Ensure resources are released
		return fmt.Errorf("master load error: %w", err)
	}

	// No errors, return nil
	return nil
}

// cleanups
func (db *KV) Close() {
	for _, chunk := range db.mmap.chunks {
		err := syscall.Munmap(chunk)
		Assert(err == nil, "unable to unmap")
	}
	_ = db.fp.Close()
}

// read the db
func (db *KV) Get(key []byte) ([]byte, bool) {
	return db.tree.Get(key)
}

// update the db
func (db *KV) Set(key []byte, val []byte) error {
	db.tree.Insert(key, val)
	return flushPages(db)
}

func (db *KV) Del(key []byte) (bool, error) {
	deleted := db.tree.Delete(key)
	return deleted, flushPages(db)
}
