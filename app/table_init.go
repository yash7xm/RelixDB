package relixdb

import (
	"fmt"
	"log"
)

const PATH = "../archive/testdb"

func InitDB() {
	db := &DB{
		Path:   PATH,
		kv:     KV{Path: PATH},
		tables: make(map[string]*TableDef),
	}

	if err := db.kv.Open(); err != nil {
		fmt.Printf("KV.Open() failed: %v", err)
	}
	defer db.kv.Close()

	err := db.TableNew(TDEF_TABLE)
	if err != nil {
		log.Fatalf("failed to create @table table: %v", err)
	}

	err = db.TableNew(TDEF_META)
	if err != nil {
		log.Fatalf("failed to create @meta table: %v", err)
	}
}
