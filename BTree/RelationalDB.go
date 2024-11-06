package BTree

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
// func dbGet(db *DB, tdef *TableDef, rec *Record) (bool, error) {
// 	values, err := checkRecord(tdef, *rec, tdef.PKeys)
// 	if err != nil {
// 		return false, err
// 	}

// 	fmt.Println(values)

// 	return true, nil
// }

// reorder a record and check for missing columns.
// n == tdef.PKeys: record is excatly a primary key
// n == len(tdef.Cols): record containse all columns.
// func checkRecord(tdef *TableDef, rec Record, n int) ([]Value, error) {

// }
