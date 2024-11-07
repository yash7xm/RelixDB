package relational

import "fmt"

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
