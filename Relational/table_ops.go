package relational

import (
	"fmt"

	BTree "github.com/yash7xm/RelixDB/BTree"
)

type InsertReq struct {
	tree *BTree.BTree
	// out
	Added   bool   // added a new key
	Updated bool   // added a new key or an old key was changed
	Old     []byte // the value before the update
	// in
	Key  []byte
	Val  []byte
	Mode int
}

func (db DB) InsertEx(req *InsertReq) {
	// Retrieve the current value associated with the key, if any
	_, found := db.kv.Get(req.Key)

	switch req.Mode {
	case MODE_UPSERT:
		if found {
			// Replace the existing value
			db.kv.Set(req.Key, req.Val)
			// tree.Set(req.Key, req.Val)
			req.Added = false // no new key was added
		} else {
			// Insert the new key-value pair
			db.kv.Set(req.Key, req.Val)
			req.Added = true // a new key was added
		}
	case MODE_UPDATE_ONLY:
		if found {
			// Update the existing value
			db.kv.Set(req.Key, req.Val)
			req.Added = false
		} else {
			req.Added = false // no key was added
		}
	case MODE_INSERT_ONLY:
		if !found {
			// Insert the new key-value pair
			db.kv.Set(req.Key, req.Val)
			req.Added = true
		} else {
			req.Added = false // no key was added
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
	added, err := db.KvUpdate(&req)
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

func (db DB) KvUpdate(req *InsertReq) (bool, error) {
	// req := &InsertReq{
	// 	tree: &db.tree,
	// 	Key:  key,
	// 	Val:  val,
	// 	Mode: mode,
	// }
	db.InsertEx(req)
	return req.Added, nil
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

func (db *DB) Delete(table string, rec Record) (bool, error) {
	tdef := getTableDef(db, table)
	if tdef == nil {
		return false, fmt.Errorf("table not found: %s", table)
	}
	return dbDelete(db, tdef, rec)
}
