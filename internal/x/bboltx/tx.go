package bboltx

import "go.etcd.io/bbolt"

// View runs a read-only transaction.
func View(db *bbolt.DB, fn func(tx *bbolt.Tx)) {
	Must(
		db.View(
			func(tx *bbolt.Tx) error {
				fn(tx)
				return nil
			},
		),
	)
}

// Update runs a write transaction.
func Update(db *bbolt.DB, fn func(tx *bbolt.Tx)) {
	Must(
		db.Update(
			func(tx *bbolt.Tx) error {
				fn(tx)
				return nil
			},
		),
	)
}
