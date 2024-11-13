package relixdb

import (
	"fmt"
)

// the iterator for range queries
type Scanner struct {
	// the range, from Key1 to Key2
	Cmp1 int // CMP_??
	Cmp2 int
	Key1 Record
	Key2 Record
	// internal
	db      *DB
	tdef    *TableDef
	indexNo int    // -1: use the primary key; >= 0: use an index
	iter    *BIter // the underlying B-tree iterator
	keyEnd  []byte // the encoded Key2
}

// fetch the current row
func (sc *Scanner) Deref(rec *Record) {
	Assert(sc.Valid(), "scanner is not valid")

	tdef := sc.tdef
	rec.Cols = tdef.Cols
	rec.Vals = rec.Vals[:0]
	key, val := sc.iter.Deref()

	if sc.indexNo < 0 {
		// primary key decode the KV pair
		rec.Vals = make([]Value, len(tdef.Cols))
		for i := 0; i < len(tdef.Cols); i++ {
			rec.Vals[i] = Value{Type: tdef.Types[i]}
		}
		decodeValues(val, rec.Vals[tdef.PKeys:])
		for i := tdef.PKeys; i < len(tdef.Cols); i++ {
			fmt.Printf("Val: %v\n", string(rec.Vals[i].Str))
		}
	} else {
		// secondary index
		// the "value" part of the KV store is not used by indexes
		Assert(len(val) == 0, "value is present to deref index")

		// decode the primary key first
		index := tdef.Indexes[sc.indexNo]
		ival := make([]Value, len(index))
		for i, c := range index {
			ival[i].Type = tdef.Types[colIndex(tdef, c)]
		}
		decodeValues(key[4:], ival)
		icol := Record{index, ival}
		// fetch the row by the primary key
		rec.Cols = tdef.Cols[:tdef.PKeys]
		for _, c := range rec.Cols {
			rec.Vals = append(rec.Vals, *icol.Get(c))
		}
		// TODO: skip this if the index contains all the columns
		ok, err := dbGet(sc.db, tdef, rec)
		Assert(ok && err == nil, "error encoutered while dereferencing the current row by secondary index")
	}
}

func (db *DB) Scan(table string, req *Scanner) error {
	tdef := getTableDef(db, table)
	if tdef == nil {
		return fmt.Errorf("table not found: %s", table)
	}
	return dbScan(db, tdef, req)
}

func dbScan(db *DB, tdef *TableDef, req *Scanner) error {
	// sanity checks
	switch {
	case req.Cmp1 > 0 && req.Cmp2 < 0:
	case req.Cmp2 > 0 && req.Cmp1 < 0:
	default:
		return fmt.Errorf("bad range")
	}

	//  select an index
	indexNo, err := findIndex(tdef, req.Key1.Cols)
	if err != nil {
		return err
	}
	index, prefix := tdef.Cols[:tdef.PKeys], tdef.Prefix
	if indexNo >= 0 {
		index, prefix = tdef.Indexes[indexNo], tdef.IndexPrefixes[indexNo]
	}
	req.db = db
	req.tdef = tdef
	req.indexNo = -1

	// seek to the start key
	keyStart := encodeKeyPartial(
		nil, prefix, req.Key1.Vals, tdef, index, req.Cmp1)
	req.keyEnd = encodeKeyPartial(
		nil, prefix, req.Key2.Vals, tdef, index, req.Cmp2)
	req.iter = db.kv.tree.Seek(keyStart, req.Cmp1)
	return nil
}

// within the range or not?
func (sc *Scanner) Valid() bool {
	if !sc.iter.Valid() {
		return false
	}
	key, _ := sc.iter.Deref()
	return cmpOK(key, sc.Cmp2, sc.keyEnd)
}

// move the underlying B-tree iterator
func (sc *Scanner) Next() {
	Assert(sc.Valid(), "scanner is not valid")
	if sc.Cmp1 > 0 {
		sc.iter.Next()
	} else {
		sc.iter.Prev()
	}
}

// get a single row by the primary key
func dbGet(db *DB, tdef *TableDef, rec *Record) (bool, error) {
	// just a shortcut for the scan operation
	sc := Scanner{
		Cmp1: CMP_GE,
		Cmp2: CMP_LE,
		Key1: *rec,
		Key2: *rec,
	}

	if err := dbScan(db, tdef, &sc); err != nil {
		return false, err
	}

	if sc.Valid() {
		sc.Deref(rec)
		return true, nil
	} else {
		return false, nil
	}
}
