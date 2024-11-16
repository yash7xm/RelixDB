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
	key, val := sc.iter.Deref()

	// Reset the record's state
	rec.Cols = make([]string, 0, len(tdef.Cols))
	rec.Vals = make([]Value, 0, len(tdef.Cols))

	if sc.indexNo < 0 {
		// primary key decode the KV pair
		// Primary key scan
		// First, decode the primary key portion from the key bytes
		pkValues := make([]Value, tdef.PKeys)
		for i := 0; i < tdef.PKeys; i++ {
			pkValues[i].Type = tdef.Types[i]
		}
		// Skip the 4-byte prefix when decoding key
		decodeValues(key[4:], pkValues)

		// Now decode the remaining columns from the value bytes
		remaining := make([]Value, len(tdef.Cols)-tdef.PKeys)
		for i := 0; i < len(remaining); i++ {
			remaining[i].Type = tdef.Types[i+tdef.PKeys]
		}
		decodeValues(val, remaining)

		// Combine everything into the record
		rec.Cols = append(rec.Cols, tdef.Cols...)
		rec.Vals = append(rec.Vals, pkValues...)
		rec.Vals = append(rec.Vals, remaining...)
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

// GetByIndex retrieves all rows that match the given index values.
// indexName specifies which index to use, and rec contains the index column values.
func (db *DB) GetByIndex(table string, indexName string, rec *Record) ([]*Record, error) {
	tdef := getTableDef(db, table)
	if tdef == nil {
		return nil, fmt.Errorf("table not found: %s", table)
	}

	// Find the specified index
	indexNo := -1
	for i, idx := range tdef.Indexes {
		if len(idx) > 0 && idx[0] == indexName {
			indexNo = i
			break
		}
	}
	if indexNo == -1 {
		return nil, fmt.Errorf("index not found: %s", indexName)
	}

	// Create a scanner for the index
	sc := Scanner{
		Cmp1: CMP_GE,
		Cmp2: CMP_LE,
		Key1: *rec,
		Key2: *rec,
	}

	if err := dbScan(db, tdef, &sc); err != nil {
		return nil, err
	}

	// Collect all matching rows
	var results []*Record
	for sc.Valid() {
		result := &Record{}
		sc.Deref(result)
		results = append(results, result)
		sc.Next()
	}

	return results, nil
}

// GetAll retrieves all rows from the table.
// It returns them sorted by primary key.
func (db *DB) GetAll(table string) ([]*Record, error) {
	tdef := getTableDef(db, table)
	if tdef == nil {
		return nil, fmt.Errorf("table not found: %s", table)
	}

	// Create an empty scanner that will cover the entire table
	sc := Scanner{
		Cmp1: CMP_GE,
		Cmp2: CMP_LE,
		Key1: Record{
			Cols: tdef.Cols[:tdef.PKeys],
			Vals: make([]Value, tdef.PKeys),
		},
		Key2: Record{
			Cols: tdef.Cols[:tdef.PKeys],
			Vals: make([]Value, tdef.PKeys),
		},
	}

	// Initialize the values based on their types
	for i := 0; i < tdef.PKeys; i++ {
		sc.Key1.Vals[i].Type = tdef.Types[i]
		sc.Key2.Vals[i].Type = tdef.Types[i]

		// Set minimum value for Key1
		sc.Key1.Vals[i].Str = []byte{}
		sc.Key1.Vals[i].I64 = 0

		// Set maximum value for Key2
		if tdef.Types[i] == TYPE_BYTES {
			sc.Key2.Vals[i].Str = []byte{0xff, 0xff, 0xff, 0xff}
		} else { // TYPE_INT64
			sc.Key2.Vals[i].I64 = 1<<63 - 1 // Max int64
		}
	}

	if err := dbScan(db, tdef, &sc); err != nil {
		return nil, err
	}

	// Collect all rows
	var results []*Record
	for sc.Valid() {
		result := &Record{}
		sc.Deref(result)
		results = append(results, result)
		sc.Next()
	}

	return results, nil
}

// GetRange retrieves all rows within a given range.
// startRec and endRec must contain values for the same columns (either primary key or index columns).
// inclusive determines whether the range bounds are inclusive.
func (db *DB) GetRange(table string, startRec, endRec *Record, inclusive bool) ([]*Record, error) {
	tdef := getTableDef(db, table)
	if tdef == nil {
		return nil, fmt.Errorf("table not found: %s", table)
	}

	// Verify that start and end records have the same columns
	if !equalCols(startRec.Cols, endRec.Cols) {
		return nil, fmt.Errorf("start and end records must have the same columns")
	}

	// Create a scanner for the range
	sc := Scanner{
		Cmp1: CMP_GE,
		Cmp2: CMP_LE,
		Key1: *startRec,
		Key2: *endRec,
	}

	// Adjust comparison operators if range should be exclusive
	if !inclusive {
		sc.Cmp1 = CMP_GT
		sc.Cmp2 = CMP_LT
	}

	if err := dbScan(db, tdef, &sc); err != nil {
		return nil, err
	}

	// Collect all matching rows
	var results []*Record
	for sc.Valid() {
		result := &Record{}
		sc.Deref(result)
		results = append(results, result)
		sc.Next()
	}

	return results, nil
}

// Helper function to check if two string slices contain the same elements
func equalCols(cols1, cols2 []string) bool {
	if len(cols1) != len(cols2) {
		return false
	}
	for i := range cols1 {
		if cols1[i] != cols2[i] {
			return false
		}
	}
	return true
}
