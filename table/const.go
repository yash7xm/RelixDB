package table

const (
	TYPE_ERROR = 0
	TYPE_BYTES = 1
	TYPE_INT64 = 2
)

// modes of the updates
const (
	MODE_UPSERT      = 0 // insert or replace
	MODE_UPDATE_ONLY = 1 // update existing keys
	MODE_INSERT_ONLY = 2 // only add new keys
)

const (
	INDEX_ADD = 1
	INDEX_DEL = 2
)
