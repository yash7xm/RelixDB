package relixdb

import (
	"bytes"
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
	// Find index of the column if it already exists
	for i, col := range rec.Cols {
		if col == key {
			// Update existing column's value
			rec.Vals[i] = Value{Type: TYPE_BYTES, Str: val}
			return rec
		}
	}
	// If column does not exist, add new column
	rec.Cols = append(rec.Cols, key)
	rec.Vals = append(rec.Vals, Value{Type: TYPE_BYTES, Str: val})
	return rec
}

func (rec *Record) AddInt64(key string, val int64) *Record {
	// Find index of the column if it already exists
	for i, col := range rec.Cols {
		if col == key {
			// Update existing column's value
			rec.Vals[i] = Value{Type: TYPE_INT64, I64: val}
			return rec
		}
	}
	// If column does not exist, add new column
	rec.Cols = append(rec.Cols, key)
	rec.Vals = append(rec.Vals, Value{Type: TYPE_INT64, I64: val})
	return rec
}

func (rec *Record) Get(key string) *Value {
	// Find the value for the corresponding column
	for i, col := range rec.Cols {
		if col == key {
			return &rec.Vals[i]
		}
	}
	// Return nil if the column is not found
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

func encodeValues(out []byte, vals []Value) []byte {
	for _, v := range vals {
		switch v.Type {
		case TYPE_INT64:
			var buf [8]byte
			u := uint64(v.I64) + (1 << 63)
			binary.BigEndian.PutUint64(buf[:], u)
			out = append(out, buf[:]...)
		case TYPE_BYTES:
			out = append(out, escapeString(v.Str)...)
			out = append(out, 0) // null-terminated
		default:
			panic("what?")
		}
	}
	return out
}

// 1. strings are encoded as null-terminated strings,
// escape the null byte so that strings contain no null byte.
// 2. "\xff" represents the highest order in key comparisons,
// also escape the first byte if it's 0xff.
func escapeString(in []byte) []byte {
	zeros := bytes.Count(in, []byte{0})
	ones := bytes.Count(in, []byte{1})
	if zeros+ones == 0 {
		return in
	}
	out := make([]byte, len(in)+zeros+ones)
	pos := 0
	if len(in) > 0 && in[0] >= 0xfe {
		out[0] = 0xfe
		out[1] = in[0]
		pos += 2
		in = in[1:]
	}
	for _, ch := range in {
		if ch <= 1 {
			out[pos+0] = 0x01
			out[pos+1] = ch + 1
			pos += 2
		} else {
			out[pos] = ch
			pos += 1
		}
	}
	return out
}

func decodeValues(in []byte, out []Value) []Value {
	str, _ := unescapeString(in[:])
	out = append(out, Value{Type: TYPE_BYTES, Str: []byte(str)})
	return out
}

// unescapeString reverses the escapeString process
func unescapeString(in []byte) (string, int) {
	out := make([]byte, 0, len(in))
	i := 0
	for i < len(in) {
		if in[i] == 0x01 {
			if in[i+1] == 0x01 {
				out = append(out, 0) // "\x01\x01" -> "\x00"
			} else if in[i+1] == 0x02 {
				out = append(out, 0x01) // "\x01\x02" -> "\x01"
			} else {
				panic("invalid escape sequence")
			}
			i += 2 // Move past the escape sequence
		} else if in[i] == 0 {
			break // Null-terminator found, end of string
		} else {
			out = append(out, in[i])
			i++
		}
	}
	return string(out), i
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
	Added   bool   // added a new key
	Updated bool   // added a new key or an old key was changed
	Old     []byte // the value before the update
	// in
	Key  []byte
	Val  []byte
	Mode int
}

func (db *KV) InsertEx(req *InsertReq) {
	// Retrieve the current value associated with the key, if any
	_, found := db.Get(req.Key)

	switch req.Mode {
	case MODE_UPSERT:
		if found {
			// Replace the existing value
			db.Set(req.Key, req.Val)
			// tree.Set(req.Key, req.Val)
			req.Added = false // no new key was added
		} else {
			// Insert the new key-value pair
			db.Set(req.Key, req.Val)
			req.Added = true // a new key was added
		}
	case MODE_UPDATE_ONLY:
		if found {
			// Update the existing value
			db.Set(req.Key, req.Val)
			req.Added = false
		} else {
			req.Added = false // no key was added
		}
	case MODE_INSERT_ONLY:
		if !found {
			// Insert the new key-value pair
			db.Set(req.Key, req.Val)
			req.Added = true
		} else {
			req.Added = false // no key was added
		}
	default:
		panic("unsupported mode")
	}
}

func (db *KV) Update(req *InsertReq) (bool, error) {
	req.tree = &db.tree
	db.InsertEx(req)
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

// Create new table
const TABLE_PREFIX_MIN = 1

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

const (
	CMP_GE = +3 // >=
	CMP_GT = +2 // >
	CMP_LT = -2 // <
	CMP_LE = -3 // <=
)

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

func checkIndexKeys(tdef *TableDef, index []string) ([]string, error) {
	icols := map[string]bool{}
	for _, c := range index {
		// check the index columns
		// omitted...
		icols[c] = true
	}
	// add the primary key to the index
	for _, c := range tdef.Cols[:tdef.PKeys] {
		if !icols[c] {
			index = append(index, c)
		}
	}
	Assert(len(index) < len(tdef.Cols), "index length is larger than columns length")
	return index, nil
}

func colIndex(tdef *TableDef, col string) int {
	for i, c := range tdef.Cols {
		if c == col {
			return i
		}
	}
	return -1
}

const (
	INDEX_ADD = 1
	INDEX_DEL = 2
)

// maintain indexes after a record is added or removed
func indexOp(db *DB, tdef *TableDef, rec Record, op int) {
	key := make([]byte, 0, 256)
	irec := make([]Value, len(tdef.Cols))

	for i, index := range tdef.Indexes {
		// the indexed key
		for j, c := range index {
			irec[j] = *rec.Get(c)
		}

		// update the key value store
		key = encodeKey(key[:0], tdef.IndexPrefixes[i], irec[:len(index)])
		done, err := false, error(nil)
		switch op {
		case INDEX_ADD:
			done, err = db.kv.Update(&InsertReq{Key: key})
		case INDEX_DEL:
			done, err = db.kv.Del(key)
		default:
			panic("what?")
		}
		Assert(err == nil, "error encountered in indexOP") // XXX: will fix this in later chapters
		Assert(done, "error encountered in doing update or del in indexOp")
	}
}

func findIndex(tdef *TableDef, keys []string) (int, error) {
	pk := tdef.Cols[:tdef.PKeys]
	if isPrefix(pk, keys) {
		// use the primary key.
		// also works for full table scans without a key.
		return -1, nil
	}

	// find a suitable index
	winner := -2
	for i, index := range tdef.Indexes {
		if !isPrefix(index, keys) {
			continue
		}
		if winner == -2 || len(index) < len(tdef.Indexes[winner]) {
			winner = i
		}
	}
	if winner == -2 {
		return -2, fmt.Errorf("no index found")
	}
	return winner, nil
}

func isPrefix(long []string, short []string) bool {
	if len(long) < len(short) {
		return false
	}
	for i, c := range short {
		if long[i] != c {
			return false
		}
	}
	return true
}

// The range key can be a prefix of the index key,
// we may have to encode missing columns to make the comparison work.
func encodeKeyPartial(
	out []byte, prefix uint32, values []Value,
	tdef *TableDef, keys []string, cmp int,
) []byte {
	out = encodeKey(out, prefix, values)
	// Encode the missing columns as either minimum or maximum values,
	// depending on the comparison operator.
	// 1. The empty string is lower than all possible value encodings,
	// thus we don't need to add anything for CMP_LT and CMP_GE.
	// 2. The maximum encodings are all 0xff bytes.
	max := cmp == CMP_GT || cmp == CMP_LE
loop:
	for i := len(values); max && i < len(keys); i++ {
		switch tdef.Types[colIndex(tdef, keys[i])] {
		case TYPE_BYTES:
			out = append(out, 0xff)
			break loop // stops here since no string encoding starts with 0xff
		case TYPE_INT64:
			out = append(out, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff)
		default:
			panic("what?")
		}
	}
	return out
}