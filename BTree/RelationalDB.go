package BTree

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
		fmt.Println(col)
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
	Name  string
	Types []uint32 // column types
	Cols  []string // column names
	PKeys int      // the first `PKeys` columns are the primary key
	// auto-assigned B-tree key prefixes for different tables
	Prefix uint32
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

// get a single row by primary key
// func dbGet(db *DB, tdef *TableDef, rec *Record) (bool, error) {
// 	values, err := checkRecord(tdef, *rec, tdef.PKeys)
// 	if err != nil {
// 		return false, err
// 	}
// 	key := encodeKey(nil, tdef.Prefix, values[:tdef.PKeys])
// 	val, ok := db.kv.Get(key)
// 	if !ok {
// 		return false, nil
// 	}
// 	for i := tdef.PKeys; i < len(tdef.Cols); i++ {
// 		values[i].Type = tdef.Types[i]
// 	}
// 	decodeValues(val, values[tdef.PKeys:])
// 	rec.Cols = append(rec.Cols, tdef.Cols[tdef.PKeys:]...)
// 	rec.Vals = append(rec.Vals, values[tdef.PKeys:]...)
// 	return true, nil
// }

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

// Strings are encoded as nul terminated strings,
// escape the nul byte so that strings contain no nul byte.
func escapeString(in []byte) []byte {
	zeros := bytes.Count(in, []byte{0})
	ones := bytes.Count(in, []byte{1})
	if zeros+ones == 0 {
		return in
	}
	out := make([]byte, len(in)+zeros+ones)
	pos := 0
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
	fmt.Printf("EscapedStr: %v \n", str)
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
	Added bool // added a new key
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

func (db *KV) Update(key []byte, val []byte, mode int) (bool, error) {
	req := &InsertReq{
		tree: &db.tree,
		Key:  key,
		Val:  val,
		Mode: mode,
	}
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
		// pr := string(meta.Get("val").Str)
		// prefix, _ := strconv.Atoi(pr)
		// tdef.Prefix = uint32(prefix)
		tdef.Prefix = binary.LittleEndian.Uint32(meta.Get("val").Str)
		Assert(tdef.Prefix > TABLE_PREFIX_MIN, "prefix lower than min")
	} else {
		meta.AddStr("val", make([]byte, 4))
	}

	// update the next prefix
	binary.LittleEndian.PutUint32(meta.Get("val").Str, tdef.Prefix+1)
	// str := strconv.Itoa(int(tdef.Prefix) + 1)
	// meta.AddStr("val", []byte(str))

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

// Range queries

type BIter struct {
	tree *BTree
	path []BNode  // from root to leaf
	pos  []uint16 // indexes into nodes
}

// get the current KV pair
func (iter *BIter) Deref() ([]byte, []byte) {
	if !iter.Valid() {
		return []byte(""), []byte("")
	}
	node := iter.path[len(iter.path)-1]
	key := node.getKey(iter.pos[len(iter.pos)-1])
	val := node.getVal(iter.pos[len(iter.pos)-1])
	v := []Value{}
	v = decodeValues(val, v)

	return key, v[0].Str
}

// precondition of the Deref()
func (iter *BIter) Valid() bool {
	return len(iter.path) != 0
}

// moving backward and forward
func (iter *BIter) Next() {
	iterNext(iter, len(iter.path)-1)
}

func (iter *BIter) Prev() {
	iterPrev(iter, len(iter.path)-1)
}

func iterNext(iter *BIter, level int) {
	// Check if we can move right within the current node at this level
	node := iter.path[level]
	if iter.pos[level] < node.nkeys()-1 {
		iter.pos[level]++ // Move right within this node
	} else if level > 0 {
		// If we are at the last key, move up to the parent and then continue
		iterNext(iter, level-1) // Move up to parent node
	} else {
		return // No more keys (we are done)
	}

	// If there are more levels, move to the leftmost child of the next key
	if level+1 < len(iter.pos) {
		node := iter.path[level]
		kid := iter.tree.get(node.getPtr(iter.pos[level])) // Get the child pointer
		iter.path[level+1] = kid                           // Move to the child node
		iter.pos[level+1] = 0                              // Set position at the first key
	}
}

func iterPrev(iter *BIter, level int) {
	if iter.pos[level] > 0 {
		iter.pos[level]-- // move within this node
	} else if level > 0 {
		iterPrev(iter, level-1) // move to a sibling node
	} else {
		return // dummy key
	}

	if level+1 < len(iter.pos) {
		// update the kid node
		node := iter.path[level]
		kid := iter.tree.get(node.getPtr(iter.pos[level]))
		iter.path[level+1] = kid
		iter.pos[level+1] = kid.nkeys() - 1
	}
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

// the iterator for range queries
type Scanner struct {
	// the range, from Key1 to Key2
	Cmp1 int // CMP_??
	Cmp2 int
	Key1 Record
	Key2 Record
	// internal
	tdef   *TableDef
	iter   *BIter // the underlying B-tree iterator
	keyEnd []byte // the encoded Key2
}

// fetch the current row
func (sc *Scanner) Deref(rec *Record) {
	for i, col := range rec.Cols {
		fmt.Printf("%v | %s\n", col, string(rec.Vals[i].Str))
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

	values1, err := checkRecord(tdef, req.Key1, tdef.PKeys)
	if err != nil {
		return err
	}

	values2, err := checkRecord(tdef, req.Key2, tdef.PKeys)
	if err != nil {
		return err
	}

	req.tdef = tdef
	// seek to the start key
	keyStart := encodeKey(nil, tdef.Prefix, values1[:tdef.PKeys])
	req.keyEnd = encodeKey(nil, tdef.Prefix, values2[:tdef.PKeys])
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
