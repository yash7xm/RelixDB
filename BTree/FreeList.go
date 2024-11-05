package BTree

type FreeList struct {
	head uint64
	// callbacks for managing on-disk pages
	get func(uint64) BNode  // dereference a pointer
	new func(BNode) uint64  // append a new page
	use func(uint64, BNode) // reuse a page
}

// Functions for accessing the list node:
func flnSize(node BNode) int
func flnNext(node BNode) uint64
func flnPtr(node BNode, idx int) uint64
func flnSetPtr(node BNode, idx int, ptr uint64)
func flnSetHeader(node BNode, size uint16, next uint64)
func flnSetTotal(node BNode, total uint64)

// number of items in the list
func (fl *FreeList) Total() int

// remove `popn` pointers and add some new pointers
func (fl *FreeList) Update(popn int, freed []uint64)

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
