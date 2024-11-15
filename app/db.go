package relixdb

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
)

type DB struct {
	Path string
	// internals
	kv     KV
	tables map[string]*TableDef // cached table defination
}

// table defination
type TableDef struct {
	// user defined
	Name    string
	Types   []uint32 // column types
	Cols    []string // column names
	PKeys   int      // the first `PKeys` columns are the primary key
	Indexes [][]string
	// auto-assigned B-tree key prefixes for different tables
	Prefix        uint32
	IndexPrefixes []uint32
}

// internal table : metadata
var TDEF_META = &TableDef{
	Name:  "@meta",
	Types: []uint32{TYPE_BYTES, TYPE_BYTES},
	Cols:  []string{"key", "val"},
	PKeys: 1,
}

// internal table: table schemas
var TDEF_TABLE = &TableDef{
	Name:  "@table",
	Types: []uint32{TYPE_BYTES, TYPE_BYTES},
	Cols:  []string{"name", "def"},
	PKeys: 1,
}

// reorder a record and check for missing columns.
// n == tdef.PKeys: record is excatly a primary key
// n == len(tdef.Cols): record containse all columns.
func checkRecord(tdef *TableDef, rec Record, n int) ([]Value, error) {
	if len(rec.Cols) < n {
		return nil, fmt.Errorf("missing columns in the record: expected at least %d, got %d", n, len(rec.Cols))
	}

	values := make([]Value, len(tdef.Cols))

	// Check that all necessary columns are present in the record
	colMap := map[string]Value{}
	for i, col := range rec.Cols {
		colMap[col] = rec.Vals[i]
	}

	// Rearrange columns according to table definition
	for i, col := range tdef.Cols {
		val, ok := colMap[col]
		if !ok && i < tdef.PKeys {
			// Primary key column is missing, return error
			return nil, fmt.Errorf("missing primary key column: %s", col)
		}

		values[i] = val
	}

	return values, nil
}

// get a single row by the primary key
func (db *DB) Get(table string, rec *Record) (bool, error) {
	tdef := getTableDef(db, table)
	if tdef == nil {
		return false, fmt.Errorf("table not found: %s", table)
	}
	return dbGet(db, tdef, rec)
}

// get the table defination by name
func getTableDef(db *DB, name string) *TableDef {
	tdef, ok := db.tables[name]
	if !ok {
		if db.tables == nil {
			db.tables = map[string]*TableDef{}
		}
		tdef = getTableDefDB(db, name)
		if tdef != nil {
			db.tables[name] = tdef
		}
	}
	return tdef
}

func getTableDefDB(db *DB, name string) *TableDef {
	rec := (&Record{}).AddStr("name", []byte(name))
	ok, err := dbGet(db, TDEF_TABLE, rec)
	Assert(err == nil, "unable to get def")
	if !ok {
		return nil
	}

	tdef := &TableDef{}
	err = json.Unmarshal(rec.Get("def").Str, tdef)
	Assert(err == nil, "unable to get def")
	return tdef
}

// Create new table
func (db *DB) TableNew(tdef *TableDef) error {
	if err := tableDefCheck(tdef); err != nil {
		return err
	}
	// check the existing table
	table := (&Record{}).AddStr("name", []byte(tdef.Name))
	ok, err := dbGet(db, TDEF_TABLE, table)
	Assert(err == nil, "error in getting table def")
	if ok {
		return fmt.Errorf("table exists: %s", tdef.Name)
	}

	// allocate a new prefix
	Assert(tdef.Prefix == 0, "error in tdef prefix")
	tdef.Prefix = TABLE_PREFIX_MIN
	meta := (&Record{}).AddStr("key", []byte("next_prefix"))
	ok, err = dbGet(db, TDEF_META, meta)
	Assert(err == nil, "error in getting meta table")
	if ok {
		tdef.Prefix = binary.LittleEndian.Uint32(meta.Get("val").Str)
		Assert(tdef.Prefix > TABLE_PREFIX_MIN, "prefix lower than min")
	} else {
		meta.AddStr("val", make([]byte, 4))
	}
	for i := range tdef.Indexes {
		prefix := tdef.Prefix + 1 + uint32(i)
		tdef.IndexPrefixes = append(tdef.IndexPrefixes, prefix)
	}

	// update the next prefix
	ntree := 1 + uint32(len(tdef.Indexes))
	binary.LittleEndian.PutUint32(meta.Get("val").Str, tdef.Prefix+ntree)
	_, err = dbUpdate(db, TDEF_META, *meta, 0)
	if err != nil {
		return err
	}

	// store the definition
	val, err := json.Marshal(tdef)
	Assert(err == nil, "unable to marshall tdef")
	table.AddStr("def", val)
	_, err = dbUpdate(db, TDEF_TABLE, *table, 0)
	return err
}

func tableDefCheck(tdef *TableDef) error {
	// verify the table definition
	if len(tdef.Cols) == 0 || len(tdef.Types) == 0 {
		return fmt.Errorf("table definition must have at least one column and one type")
	}

	if len(tdef.Cols) != len(tdef.Types) {
		return fmt.Errorf("number of columns does not match number of types")
	}

	if tdef.PKeys <= 0 || tdef.PKeys > len(tdef.Cols) {
		return fmt.Errorf("invalid number of primary keys")
	}

	for i := 0; i < tdef.PKeys; i++ {
		if tdef.Types[i] != TYPE_INT64 && tdef.Types[i] != TYPE_BYTES {
			return fmt.Errorf("primary key columns must be of type int64 or bytes")
		}
	}

	// verify the indexes
	for i, index := range tdef.Indexes {
		index, err := checkIndexKeys(tdef, index)
		if err != nil {
			return err
		}
		tdef.Indexes[i] = index
	}

	return nil
}
