package relixdb

import "fmt"

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
