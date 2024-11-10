package indexes

import (
	"fmt"

	BTree "github.com/yash7xm/RelixDB/BTree"
	Encoder "github.com/yash7xm/RelixDB/encoder"
	Table "github.com/yash7xm/RelixDB/table"
)

func checkIndexKeys(tdef *Table.TableDef, index []string) ([]string, error) {
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
	BTree.Assert(len(index) < len(tdef.Cols), "index length is larger than columns length")
	return index, nil
}

func ColIndex(tdef *Table.TableDef, col string) int {
	for i, c := range tdef.Cols {
		if c == col {
			return i
		}
	}
	return -1
}

// maintain indexes after a record is added or removed
func indexOp(db *Table.DB, tdef *Table.TableDef, rec Table.Record, op int) {
	key := make([]byte, 0, 256)
	irec := make([]Table.Value, len(tdef.Cols))

	for i, index := range tdef.Indexes {
		// the indexed key
		for j, c := range index {
			irec[j] = *rec.Get(c)
		}

		// update the key value store
		key = Encoder.EncodeKey(key[:0], tdef.IndexPrefixes[i], irec[:len(index)])
		done, err := false, error(nil)
		switch op {
		case Table.INDEX_ADD:
			done, err = db.KvUpdate(&Table.InsertReq{Key: key})
		case Table.INDEX_DEL:
			done, err = db.kv.Del(key)
		default:
			panic("what?")
		}
		BTree.Assert(err == nil, "error encountered in indexOP") // XXX: will fix this in later chapters
		BTree.Assert(done, "error encountered in doing update or del in indexOp")
	}
}

func findIndex(tdef *Table.TableDef, keys []string) (int, error) {
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
