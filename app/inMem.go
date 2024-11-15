package relixdb

import (
	"unsafe"
)

type C struct {
	tree  BTree
	Ref   map[string]string
	pages map[uint64]BNode
}

func NewC() *C {
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
		Ref:   map[string]string{},
		pages: pages,
	}
}

func (c *C) Add(key string, val string) {
	c.tree.Insert([]byte(key), []byte(val))
	c.Ref[key] = val
}

func (c *C) Del(key string) bool {
	delete(c.Ref, key)
	return c.tree.Delete([]byte(key))
}

func (c *C) Get(key string) string {
	val, found := c.tree.Get([]byte(key))
	if found && string(val) == c.Ref[key] {
		return string(val)
	}
	return ""
}
