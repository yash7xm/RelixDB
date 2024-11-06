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
	// Create a new Value for the string (byte array)
	value := Value{
		Type: TYPE_BYTES,
		Str:  val,
	}

	// Add key and value to the record's columns and values
	rec.Cols = append(rec.Cols, key)
	rec.Vals = append(rec.Vals, value)

	return rec
}

func (rec *Record) AddInt64(key string, val int64) *Record {
	// Create a new Value for the int64
	value := Value{
		Type: TYPE_INT64,
		I64:  val,
	}

	// Add key and value to the record's columns and values
	rec.Cols = append(rec.Cols, key)
	rec.Vals = append(rec.Vals, value)

	return rec
}

func (rec *Record) Get(key string) *Value {
	for i, col := range rec.Cols {
		if col == key {
			// Return the pointer to the value if the key matches
			return &rec.Vals[i]
		}
	}
	// If key not found, return nil
	return nil
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

func encodeValues(out []byte, vals []Value) []byte {
	for _, val := range vals {
		// Append the type
		buf := make([]byte, 4)
		binary.BigEndian.PutUint32(buf, val.Type)
		out = append(out, buf...)

		switch val.Type {
		case TYPE_INT64:
			// Append int64 as 8 bytes
			buf := make([]byte, 8)
			binary.BigEndian.PutUint64(buf, uint64(val.I64))
			out = append(out, buf...)
		case TYPE_BYTES:
			// Append length and then the byte array
			buf := make([]byte, 4)
			binary.BigEndian.PutUint32(buf, uint32(len(val.Str)))
			out = append(out, buf...)
			out = append(out, val.Str...)
		default:
			panic("unsupported value type")
		}
	}
	return out
}

func decodeValues(in []byte, out []Value) {
	offset := 0
	for i := range out {
		// Read the type (4 bytes)
		out[i].Type = binary.BigEndian.Uint32(in[offset : offset+4])
		offset += 4

		switch out[i].Type {
		case TYPE_INT64:
			// Read int64 (8 bytes)
			out[i].I64 = int64(binary.BigEndian.Uint64(in[offset : offset+8]))
			offset += 8
		case TYPE_BYTES:
			// Read the length of the byte array (4 bytes)
			length := binary.BigEndian.Uint32(in[offset : offset+4])
			offset += 4
			// Read the byte array
			out[i].Str = in[offset : offset+int(length)]
			offset += int(length)
		default:
			panic("unsupported value type")
		}
	}
}

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

// modes of the updates
const (
	MODE_UPSERT      = 0 // insert or replace
	MODE_UPDATE_ONLY = 1 // update existing keys
	MODE_INSERT_ONLY = 2 // only add new keys
)

type InsertReq struct {
	tree *BTree
	// out
	Added bool // added a new key
	// in
	Key  []byte
	Val  []byte
	Mode int
}

func (tree *BTree) InsertEx(req *InsertReq) {
	// Retrieve the current value associated with the key, if any
	_, found := tree.Get(req.Key)

	switch req.Mode {
	case MODE_UPSERT:
		if found {
			// Replace the existing value
			tree.Insert(req.Key, req.Val)
			// tree.Set(req.Key, req.Val)
			req.Added = false // no new key was added
		} else {
			// Insert the new key-value pair
			tree.Insert(req.Key, req.Val)
			req.Added = true // a new key was added
		}
	case MODE_UPDATE_ONLY:
		if found {
			// Update the existing value
			tree.Insert(req.Key, req.Val)
			req.Added = false
		} else {
			req.Added = false // no key was added
		}
	case MODE_INSERT_ONLY:
		if !found {
			// Insert the new key-value pair
			tree.Insert(req.Key, req.Val)
			req.Added = true
		} else {
			req.Added = false // no key was added
		}
	default:
		panic("unsupported mode")
	}
}

func (db *KV) Update(key []byte, val []byte, mode int) (bool, error) {
	req := &InsertReq{
		tree: &db.tree,
		Key:  key,
		Val:  val,
		Mode: mode,
	}
	db.tree.InsertEx(req)
	return req.Added, nil
}

// add a row to the table
func dbUpdate(db *DB, tdef *TableDef, rec Record, mode int) (bool, error) {
	values, err := checkRecord(tdef, rec, len(tdef.Cols))
	if err != nil {
		return false, err
	}
	key := encodeKey(nil, tdef.Prefix, values[:tdef.PKeys])
	val := encodeValues(nil, values[tdef.PKeys:])
	return db.kv.Update(key, val, mode)
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
	return db.kv.Del(key)
}

func (db *DB) Delete(table string, rec Record) (bool, error) {
	tdef := getTableDef(db, table)
	if tdef == nil {
		return false, fmt.Errorf("table not found: %s", table)
	}
	return dbDelete(db, tdef, rec)
}

// Create new table

const TABLE_PREFIX_MIN = 1

func (db *DB) TableNew(tdef *TableDef) error {
	if err := tableDefCheck(tdef); err != nil {
		return err
	}

	// check the existing table
	table := (&Record{}).AddStr("name", []byte(tdef.Name))
	ok, err := dbGet(db, TDEF_TABLE, table)
	Assert(err == nil, "error get table def	")
	if ok {
		return fmt.Errorf("table exists: %s", tdef.Name)
	}

	// allocate a new prefix
	Assert(tdef.Prefix == 0, "error in tdef prefix")
	tdef.Prefix = TABLE_PREFIX_MIN
	meta := (&Record{}).AddStr("key", []byte("next_prefix"))
	ok, err = dbGet(db, TDEF_META, meta)
	Assert(err == nil, "unable to get meta table")
	if ok {
		tdef.Prefix = binary.LittleEndian.Uint32(meta.Get("val").Str)
		Assert(tdef.Prefix > TABLE_PREFIX_MIN, "table prefix is lower the excepted")
	} else {
		meta.AddStr("val", make([]byte, 4))
	}

	// update the next prefix
	binary.LittleEndian.PutUint32(meta.Get("val").Str, tdef.Prefix+1)
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

	return nil
}
