package BTree

import "unsafe"

type C struct {
	tree  BTree
	ref   map[string]string
	pages map[uint64]BNode
}

func newC() *C {
	pages := map[uint64]BNode{}
	return &C{
		tree: BTree{
			get: func(ptr uint64) BNode {
				node, ok := pages[ptr]
				Assert(ok, "unable to get node")
				return node
			},
			new: func(node BNode) uint64 {
				Assert(node.nbytes() <= BTREE_PAGE_SIZE, "number of bytes excedes the page limit size")
				key := uint64(uintptr(unsafe.Pointer(&node.data[0])))
				Assert(pages[key].data == nil, "unable to create a page")
				pages[key] = node
				return key
			},
			del: func(ptr uint64) {
				_, ok := pages[ptr]
				Assert(ok, "unable to get page")
				delete(pages, ptr)
			},
		},
		ref:   map[string]string{},
		pages: pages,
	}
}

func (c *C) add(key string, val string) {
	c.tree.Insert([]byte(key), []byte(val))
	c.ref[key] = val
}
func (c *C) del(key string) bool {
	delete(c.ref, key)
	return c.tree.Delete([]byte(key))
}
