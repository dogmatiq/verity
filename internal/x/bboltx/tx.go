package bboltx

import (
	"go.etcd.io/bbolt"
)

// BeginRead starts a read-only transaction.
func BeginRead(db *bbolt.DB) *bbolt.Tx {
	tx, err := db.Begin(false)
	Must(err)
	return tx
}

// BeginWrite starts a read-only transaction.
func BeginWrite(db *bbolt.DB) *bbolt.Tx {
	tx, err := db.Begin(true)
	Must(err)
	return tx
}

// Commit commits the given transaction.
func Commit(tx *bbolt.Tx) {
	Must(tx.Commit())
}
