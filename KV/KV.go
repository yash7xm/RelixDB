package kv

import (
	"errors"
	"fmt"
	"os"
	"syscall"

	BTree "github.com/yash7xm/RelixDB/BTree"
)

func mmapInit(fp *os.File) (int, []byte, error) {
	fi, err := fp.Stat()
	if err != nil {
		return 0, nil, fmt.Errorf("stat: %w", err)
	}

	if fi.Size()%BTree.BTREE_PAGE_SIZE != 0 {
		return 0, nil, errors.New("file size is not a multiple of page size")
	}

	mmapSize := 64 << 20
	BTree.Assert(mmapSize%BTree.BTREE_PAGE_SIZE == 0, "mmap size is not a multiple of page size.")
	for mmapSize < int(fi.Size()) {
		mmapSize *= 2
	}
	// mmap size can be larger than the file

	chunk, err := syscall.Mmap(
		int(fp.Fd()), 0, mmapSize,
		syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED,
	)
	if err != nil {
		return 0, nil, fmt.Errorf("mmap: %w", err)
	}

	return int(fi.Size()), chunk, nil
}

type KV struct {
	Path string
	// internals
	fp   *os.File
	tree BTree.BTree
	mmap struct {
		file   int      // file size, can be larger than the database size
		total  int      // mmap size, can be larger than the file size
		chunks [][]byte // mutliple mmaps, can be non-continuous
	}
	page struct {
		flushed uint64   // database size in number of pages
		temp    [][]byte // newly allocated pages
	}
}

// extend the mmap by adding new mappings
func extendMmap(db *KV, npages int) error {
	if db.mmap.total >= npages*BTree.BTREE_PAGE_SIZE {
		return nil
	}

	// double the address space
	chunk, err := syscall.Mmap(
		int(db.fp.Fd()), int64(db.mmap.total), db.mmap.total,
		syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED,
	)
	if err != nil {
		return fmt.Errorf("mmap: %w", err)
	}

	db.mmap.total += db.mmap.total
	db.mmap.chunks = append(db.mmap.chunks, chunk)
	return nil
}
