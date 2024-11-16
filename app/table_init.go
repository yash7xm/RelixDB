package relixdb

import (
	"log"
)

const PATH = "../archive/testdb"

func InitDB(db *DB) {
	err := db.TableNew(TDEF_TABLE)
	if err != nil {
		log.Fatalf("failed to create @table table: %v", err)
	}

	err = db.TableNew(TDEF_META)
	if err != nil {
		log.Fatalf("failed to create @meta table: %v", err)
	}
}
