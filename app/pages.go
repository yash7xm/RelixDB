package relixdb

import "fmt"

// callback for BTree & FreeList, dereference a pointer.
func (db *KV) pageGet(ptr uint64) BNode {
	if page, ok := db.page.updates[ptr]; ok {
		Assert(page != nil, "page not found")
		return BNode{page} // for new pages
	}
	return pageGetMapped(db, ptr) // for written pages
}

func pageGetMapped(db *KV, ptr uint64) BNode {
	start := uint64(0)
	for _, chunk := range db.mmap.chunks {
		end := start + uint64(len(chunk))/BTREE_PAGE_SIZE
		if ptr < end {
			offset := BTREE_PAGE_SIZE * (ptr - start)
			return BNode{chunk[offset : offset+BTREE_PAGE_SIZE]}
		}
		start = end
	}
	panic("bad ptr")
}

// callback for BTree, allocate a new page.
func (db *KV) pageNew(node BNode) uint64 {
	Assert(len(node.data) <= BTREE_PAGE_SIZE, "node data excceds page size")
	ptr := uint64(0)
	if db.page.nfree < db.free.Total() {
		// reuse a deallocated page
		ptr = db.free.Get(db.page.nfree)
		db.page.nfree++
	} else {
		// append a new page
		ptr = db.page.flushed + uint64(db.page.nappend)
		db.page.nappend++
	}
	db.page.updates[ptr] = node.data
	return ptr
}

// callback for BTree, deallocate a page
func (db *KV) pageDel(ptr uint64) {
	db.page.updates[ptr] = nil
}

// callback for FreeList, allocate a new page.
func (db *KV) pageAppend(node BNode) uint64 {
	Assert(len(node.data) <= BTREE_PAGE_SIZE, "node data excceds page size")
	ptr := db.page.flushed + uint64(db.page.nappend)
	db.page.nappend++
	db.page.updates[ptr] = node.data
	return ptr
}

// callback for FreeList, reuse a page.
func (db *KV) pageUse(ptr uint64, node BNode) {
	db.page.updates[ptr] = node.data
}

// persist the newly allocated pages after updates
func flushPages(db *KV) error {
	if err := writePages(db); err != nil {
		return err
	}

	return syncPages(db)
}

func writePages(db *KV) error {
	// update the free list
	freed := []uint64{}
	for ptr, page := range db.page.updates {
		if page == nil {
			freed = append(freed, ptr)
		}
	}
	db.free.Update(db.page.nfree, freed)

	// extend the file and mmap if needed
	npages := int(db.page.flushed) + len(db.page.updates)
	if err := extendFile(db, npages); err != nil {
		return err
	}
	if err := extendMmap(db, npages); err != nil {
		return err
	}

	// copy pages to the file
	for ptr, page := range db.page.updates {
		if page != nil {
			copy(pageGetMapped(db, ptr).data, page)
		}
	}

	return nil
}

func syncPages(db *KV) error {
	// flush data to the disk. must be done before updating the master page.
	if err := db.fp.Sync(); err != nil {
		return fmt.Errorf("fscync: %w", err)
	}

	db.page.flushed += uint64(db.page.nappend)
	db.page.nappend = 0
	// db.page.flushed += uint64(len(db.page.updates))
	// think this
	// db.page.updates = db.page.updates[0]

	// update and flush the master page
	if err := masterStore(db); err != nil {
		return err
	}

	if err := db.fp.Sync(); err != nil {
		return fmt.Errorf("fscync: %w", err)
	}

	return nil
}
