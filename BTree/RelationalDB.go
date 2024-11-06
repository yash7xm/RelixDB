package BTree

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
)

const (
	TYPE_ERROR = 0
	TYPE_BYTES = 1
	TYPE_INT64 = 2
)

// table cell
type Value struct {
	Type uint32
	I64  int64
	Str  []byte
}

// table row
type Record struct {
	Cols []string
	Vals []Value
}

func (rec *Record) AddStr(key string, val []byte) *Record {
	Val := Value{
		Type: 1,
		Str:  val,
	}
	new := &Record{}
	for i, colName := range rec.Cols {
		new.Cols[i] = colName
		if colName != key {
			new.Vals[i] = rec.Vals[i]
		} else {
			new.Vals[i] = Val
		}
	}
	return new
}

func (rec *Record) AddInt64(key string, val int64) *Record {
	Val := Value{
		Type: 2,
		I64:  val,
	}
	new := &Record{}
	for i, colName := range rec.Cols {
		new.Cols[i] = colName
		if colName != key {
			new.Vals[i] = rec.Vals[i]
		} else {
			new.Vals[i] = Val
		}
	}
	return new
}

func (rec *Record) Get(key string) *Value {
	idx := 0
	for i, colName := range rec.Cols {
		if colName == key {
			idx = i
			break
		}
	}

	return &rec.Vals[idx]
}

type DB struct {
	Path string
	// internals
	kv     KV
	tables map[string]*TableDef // cached table defination
}

// table defination
type TableDef struct {
	// user defined
	Name  string
	Types []uint32 // column types
	Cols  []string // column names
	PKeys int      // the first `PKeys` columns are the primary key
	// auto-assigned B-tree key prefixes for different tables
	Prefix uint32
}

// internal table : metadata
var TDEF_META = &TableDef{
	Prefix: 1,
	Name:   "@meta",
	Types:  []uint32{TYPE_BYTES, TYPE_BYTES},
	Cols:   []string{"key", "val"},
	PKeys:  1,
}

// internal table: table schemas
var TDEF_TABLE = &TableDef{
	Prefix: 2,
	Name:   "@table",
	Types:  []uint32{TYPE_BYTES, TYPE_BYTES},
	Cols:   []string{"name", "def"},
	PKeys:  1,
}

// get a single row by primary key
func dbGet(db *DB, tdef *TableDef, rec *Record) (bool, error) {
	values, err := checkRecord(tdef, *rec, tdef.PKeys)
	if err != nil {
		return false, err
	}

	key := encodeKey(nil, tdef.Prefix, values[:tdef.PKeys])
	val, ok := db.kv.Get(key)
	if !ok {
		return false, nil
	}

	for i := tdef.PKeys; i < len(tdef.Cols); i++ {
		values[i].Type = tdef.Types[i]
	}

	decodeValues(val, values[tdef.PKeys:])

	rec.Cols = append(rec.Cols, tdef.Cols[tdef.PKeys:]...)
	rec.Vals = append(rec.Vals, values[tdef.PKeys:]...)

	return true, nil
}

// reorder a record and check for missing columns.
// n == tdef.PKeys: record is excatly a primary key
// n == len(tdef.Cols): record containse all columns.
func checkRecord(tdef *TableDef, rec Record, n int) ([]Value, error) {
	if n == tdef.PKeys {
		return rec.Vals, nil
	}

	if n == len(tdef.Cols) {
		return rec.Vals, nil
	}

	return rec.Vals, fmt.Errorf("record is not valid")
}

func encodeValues(out []byte, vals []Value) []byte
func decodeValues(in []byte, out []Value)

// for primary keys
func encodeKey(out []byte, prefix uint32, vals []Value) []byte {
	var buf [4]byte
	binary.BigEndian.PutUint32(buf[:], prefix)
	out = append(out, buf[:]...)
	out = encodeValues(out, vals)
	return out
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
