package relixdb

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"syscall"
)

func mmapInit(fp *os.File) (int, []byte, error) {
	fi, err := fp.Stat()
	if err != nil {
		return 0, nil, fmt.Errorf("stat: %w", err)
	}

	if fi.Size()%BTREE_PAGE_SIZE != 0 {
		return 0, nil, errors.New("file size is not a multiple of page size")
	}

	mmapSize := 64 << 20
	Assert(mmapSize%BTREE_PAGE_SIZE == 0, "mmap size is not a multiple of page size.")
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

// extend the mmap by adding new mappings
func extendMmap(db *KV, npages int) error {
	if db.mmap.total >= npages*BTREE_PAGE_SIZE {
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

// extend the file to atleadt `npages`.
func extendFile(db *KV, npages int) error {
	filePages := db.mmap.file / BTREE_PAGE_SIZE
	if filePages >= npages {
		return nil
	}

	for filePages < npages {
		// the file size is increased exponentially
		// so that we don't have to extend the size for every update.
		inc := filePages / 8
		if inc < 1 {
			inc = 1
		}
		filePages += inc
	}

	fileSize := filePages * BTREE_PAGE_SIZE
	err := syscall.Fallocate(int(db.fp.Fd()), 0, 0, int64(fileSize))
	if err != nil {
		return fmt.Errorf("fallocate: %w", err)
	}

	db.mmap.file = fileSize
	return nil
}

const DB_SIG = "RelxDBYashPoonia"

// the master page format.
// it contains the pointer to the root and other important bits.
// | sig | btree_root | page_used |
// | 16B |    8B      |    8B     |

func masterLoad(db *KV) error {
	// If the file is empty, initialize the master page
	if db.mmap.file == 0 {
		// empty file, the master page will be created on the first write.
		db.page.flushed = 1 // reserved for the master page
		return nil
	}
	data := db.mmap.chunks[0]
	root := binary.LittleEndian.Uint64(data[16:])
	used := binary.LittleEndian.Uint64(data[24:])

	// verify the page
	if !bytes.Equal([]byte(DB_SIG), data[:16]) {
		return errors.New("bad signature")
	}
	bad := !(1 <= used && used <= uint64(db.mmap.file/BTREE_PAGE_SIZE))
	bad = bad || !(root < used)
	if bad {
		return errors.New("bad master page")
	}
	db.tree.root = root
	db.page.flushed = used
	return nil
}

// update the master page. it must be atomic.
func masterStore(db *KV) error {
	var data [32]byte
	copy(data[:16], []byte(DB_SIG))
	binary.LittleEndian.PutUint64(data[16:], db.tree.root)
	binary.LittleEndian.PutUint64(data[24:], db.page.flushed)
	// NOTE: Updating the page via mmap is not atomic.
	// Use the `pwrite()` syscall instead.
	_, err := syscall.Pwrite(int(db.fp.Fd()), data[:], 0)
	// _, err := db.fp.WriteAt(data[:], 0)
	if err != nil {
		return fmt.Errorf("write master page: %w", err)
	}
	return nil
}
