package relixdb

import (
	"bytes"
	"fmt"
)

type InsertReq struct {
	tree *BTree
	// out
	Added   bool   // added a new key
	Updated bool   // added a new key or an old key was changed
	Old     []byte // the value before the update
	// in
	Key  []byte
	Val  []byte
	Mode int
}

// add a record
func (db *DB) Set(table string, rec Record, mode int) (bool, error) {
	tdef := getTableDef(db, table)
	if tdef == nil {
		return false, fmt.Errorf("table not found: %s", table)
	}
	return dbUpdate(db, tdef, rec, mode)
}

func (db *DB) Insert(table string, rec Record) (bool, error) {
	return db.Set(table, rec, MODE_INSERT_ONLY)
}

func (db *DB) Update(table string, rec Record) (bool, error) {
	return db.Set(table, rec, MODE_UPDATE_ONLY)
}

func (db *DB) Upsert(table string, rec Record) (bool, error) {
	return db.Set(table, rec, MODE_UPSERT)
}

func (db *KV) Update(req *InsertReq) (bool, error) {
	req.tree = &db.tree
	db.InsertEx(req)
	return req.Added, nil
}

func (db *DB) Delete(table string, rec Record) (bool, error) {
	tdef := getTableDef(db, table)
	if tdef == nil {
		return false, fmt.Errorf("table not found: %s", table)
	}
	return dbDelete(db, tdef, rec)
}

func (db *KV) InsertEx(req *InsertReq) {
	// Retrieve the current value associated with the key, if any
	_, found := db.Get(req.Key)

	switch req.Mode {
	case MODE_UPSERT:
		if found {
			// Replace the existing value
			db.Set(req.Key, req.Val)
			// tree.Set(req.Key, req.Val)return db.Set(table, rec, MODE_INSERT_ONLY)
			req.Added = false // no new key was added
			req.Updated = true
		} else {
			// Insert the new key-value pair
			db.Set(req.Key, req.Val)
			req.Added = true // a new key was added
			req.Updated = true
		}
	case MODE_UPDATE_ONLY:
		if found {
			// Update the existing value
			db.Set(req.Key, req.Val)
			req.Added = false
			req.Updated = true
		} else {
			req.Added = false // no key was added
			req.Updated = true
		}
	case MODE_INSERT_ONLY:
		if !found {
			// Insert the new key-value pair
			db.Set(req.Key, req.Val)
			req.Added = true
			req.Updated = true
		} else {
			req.Added = false // no key was added
			req.Updated = true
		}
	default:
		panic("unsupported mode")
	}
}

// add a row to the table
func dbUpdate(db *DB, tdef *TableDef, rec Record, mode int) (bool, error) {
	values, err := checkRecord(tdef, rec, len(tdef.Cols))
	if err != nil {
		return false, err
	}
	key := encodeKey(nil, tdef.Prefix, values[:tdef.PKeys])
	val := encodeValues(nil, values[tdef.PKeys:])
	req := InsertReq{Key: key, Val: val, Mode: mode}
	added, err := db.kv.Update(&req)
	if err != nil || !req.Updated || len(tdef.Indexes) == 0 {
		return added, err
	}
	// maintain indexes
	if req.Updated && !req.Added {
		decodeValues(req.Old, values[tdef.PKeys:]) // get the old row
		indexOp(db, tdef, Record{tdef.Cols, values}, INDEX_DEL)
	}
	if req.Updated {
		indexOp(db, tdef, rec, INDEX_ADD)
	}
	return added, nil
}

// delete a record by its primary key
func dbDelete(db *DB, tdef *TableDef, rec Record) (bool, error) {
	values, err := checkRecord(tdef, rec, tdef.PKeys)
	if err != nil {
		return false, err
	}
	key := encodeKey(nil, tdef.Prefix, values[:tdef.PKeys])

	deleted, err := db.kv.Del(key)
	if err != nil || !deleted || len(tdef.Indexes) == 0 {
		return deleted, err
	}
	// maintain indexes
	if deleted {
		indexOp(db, tdef, rec, INDEX_DEL)
	}
	return true, nil
}

// find the closest position that is less or equal to the input key
func (tree *BTree) SeekLE(key []byte) *BIter {
	iter := &BIter{tree: tree}
	for ptr := tree.root; ptr != 0; {
		node := tree.get(ptr)
		idx := nodeLookupLE(node, key)
		iter.path = append(iter.path, node)
		iter.pos = append(iter.pos, idx)
		if node.btype() == BNODE_NODE {
			ptr = node.getPtr(idx)
		} else {
			ptr = 0
		}
	}
	return iter
}

// find the closest position to a key with respect to `cmp` relation
func (tree *BTree) Seek(key []byte, cmp int) *BIter {
	iter := tree.SeekLE(key)
	if cmp != CMP_LE && iter.Valid() {
		curr, _ := iter.Deref()
		if !cmpOK(curr, cmp, key) {
			// off by one
			if cmp > 0 {
				iter.Next()
			} else {
				iter.Prev()
			}
		}
	}

	return iter
}

// key cmp ref
func cmpOK(key []byte, cmp int, ref []byte) bool {
	r := bytes.Compare(key, ref)
	switch cmp {
	case CMP_GE:
		return r >= 0
	case CMP_GT:
		return r > 0
	case CMP_LT:
		return r < 0
	case CMP_LE:
		return r <= 0
	default:
		panic("what?")
	}
}
