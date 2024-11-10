package BTree

type DBTX struct {
	kv KVTX
	db *DB
}

func (db *DB) Begin(tx *DBTX) {
	tx.db = db
	db.kv.Begin(&tx.kv)
}

func (db *DB) Commit(tx *DBTX) error {
	return db.kv.Commit(&tx.kv)
}

func (db *DB) Abort(tx *DBTX) {
	db.kv.Abort(&tx.kv)
}

func (tx *DBTX) TableNew(tdef *TableDef) error {
	return tx.db.TableNew(tdef)
}

func (tx *DBTX) Get(table string, rec *Record) (bool, error) {
	return tx.db.Get(table, rec)
}

func (tx *DBTX) Set(table string, rec Record, mode int) (bool, error) {
	return tx.db.Set(table, rec, mode)
}

func (tx *DBTX) Delete(table string, rec Record) (bool, error) {
	return tx.db.Delete(table, rec)
}

func (tx *DBTX) Scan(table string, req *Scanner) error {
	return tx.db.Scan(table, req)
}
