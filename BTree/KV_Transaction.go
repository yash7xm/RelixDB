package BTree

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
