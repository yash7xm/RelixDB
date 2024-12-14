package relixdb

import "encoding/binary"

type FreeList struct {
	head uint64
	FreeListData
	// for each transaction
	varsion   uint64 // current version
	minReader uint64 // minimum reader version
	// callbacks for managing on-disk pages
	get func(uint64) BNode  // dereference a pointer
	new func(BNode) uint64  // append a new page
	use func(uint64, BNode) // reuse a page
}

// the in-memory data structure that is updated and committed by transactions
type FreeListData struct {
	head uint64
}

// Functions for accessing the list node:
func flnSize(node BNode) int {
	size := binary.LittleEndian.Uint16(node.data[2:4])
	return int(size)
}

func flnNext(node BNode) uint64 {
	nextPtr := binary.LittleEndian.Uint64(node.data[12:20])
	return nextPtr
}

func flnPtr(node BNode, idx int) uint64 {
	baseOffset := 20
	ptrSize := 8
	offset := baseOffset + idx*ptrSize

	ptr := binary.LittleEndian.Uint64(node.data[offset : offset+ptrSize])
	return ptr
}

func flnSetPtr(node BNode, idx int, ptr uint64) {
	baseOffset := 20
	ptrSize := 8
	offset := baseOffset + idx*ptrSize

	binary.LittleEndian.PutUint64(node.data[offset:offset+ptrSize], ptr)
}

func flnSetHeader(node BNode, size uint16, next uint64) {
	binary.LittleEndian.PutUint16(node.data[2:4], size)
	binary.LittleEndian.PutUint64(node.data[12:20], next)
}

func flnSetTotal(node BNode, total uint64) {
	totalOffset := 4
	binary.LittleEndian.PutUint64(node.data[totalOffset:totalOffset+8], total)
}

// number of items in the list
func (fl *FreeList) Total() int {
	if fl.head == 0 {
		return 0
	}

	firstNode := fl.get(fl.head)
	total := flnSize(firstNode)

	return total
}

// get the nth pointer
func (fl *FreeList) Get(topn int) uint64 {
	Assert(0 <= topn && topn < fl.Total(), "index out of bound")
	node := fl.get(fl.head)
	for flnSize(node) <= topn {
		topn -= flnSize(node)
		next := flnNext(node)
		Assert(next != 0, "end of list")
		node = fl.get(next)
	}
	return flnPtr(node, flnSize(node)-topn-1)
}

// remove `popn` pointers and some new pointers
func (fl *FreeList) Update(popn int, freed []uint64) {
	Assert(popn <= fl.Total(), "not enough pages")
	if popn == 0 && len(freed) == 0 {
		return // nothing to do
	}

	// prepare to construct the new list
	total := fl.Total()
	reuse := []uint64{}
	for fl.head != 0 && len(reuse)*FREE_LIST_CAP < len(freed) {
		node := fl.get(fl.head)
		freed = append(freed, fl.head) // recycle the node itself
		if popn >= flnSize(node) {
			// phase 1
			// remove all pointers in this node
			popn -= flnSize(node)
		} else {
			// phase 2
			// remove some pointers
			remain := flnSize(node) - popn
			popn = 0
			// resuse pointers from the list itself
			for remain > 0 && len(reuse)*FREE_LIST_CAP < len(freed)+remain {
				remain--
				reuse = append(reuse, flnPtr(node, remain))
			}
			// move the node into the `freed` list
			for i := 0; i < remain; i++ {
				freed = append(freed, flnPtr(node, i))
			}
		}

		// discard the node and move to the next node
		total -= flnSize(node)
		fl.head = flnNext(node)
	}

	Assert(len(reuse)*FREE_LIST_CAP >= len(freed) || fl.head == 0, "error in update free list")

	// phase 3 : prepend new nodes
	flPush(fl, freed, reuse)
	// done
	flnSetTotal(fl.get(fl.head), uint64(total+len(freed)))
}

func flPush(fl *FreeList, freed []uint64, reuse []uint64) {
	for len(freed) > 0 {
		new := BNode{make([]byte, BTREE_PAGE_SIZE)}

		// construct a new node
		size := len(freed)
		if size > FREE_LIST_CAP {
			size = FREE_LIST_CAP
		}

		flnSetHeader(new, uint16(size), fl.head)
		for i, ptr := range freed[:size] {
			flnSetPtr(new, i, ptr)
		}
		freed = freed[size:]

		if len(reuse) > 0 {
			// reuse a pointer from the list
			fl.head, reuse = reuse[0], reuse[1:]
			fl.use(fl.head, new)
		} else {
			// or append a page to house the new node
			fl.head = fl.new(new)
		}
	}

	Assert(len(reuse) == 0, "unable to push correctly")
}

// try to remove an item from the tail. returns 0 on failure.
// the removed pointer must not be reachable by the minimum version reader.
func (fl *FreeList) Pop() uint64
// add some new pointers to the head and finalize the update
func (fl *FreeList) Add(freed []uint64)
