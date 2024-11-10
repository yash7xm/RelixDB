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