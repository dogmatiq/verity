package boltdb

import (
	"context"

	"github.com/dogmatiq/infix/internal/x/bboltx"
	"github.com/dogmatiq/infix/internal/x/syncx"
	"go.etcd.io/bbolt"
)

// database wraps a BoltDB database with a context-aware mutex.
//
// The transaction type acquires a lock on the database before starting the
// underlying BoltDB transaction.
type database struct {
	m      syncx.RWMutex
	actual *bbolt.DB
	close  func(*bbolt.DB) error
}

// newDatabase returns a new empty database.
func newDatabase(
	db *bbolt.DB,
	c func(*bbolt.DB) error,
) *database {
	return &database{
		actual: db,
		close:  c,
	}
}

// Begin starts a write transaction by acquiring a write-lock on the database.
func (db *database) Begin(ctx context.Context) *bbolt.Tx {
	err := db.m.Lock(ctx)
	bboltx.Must(err)

	tx, err := db.actual.Begin(true)
	if err != nil {
		db.m.Unlock()
		bboltx.Must(err)
	}

	return tx
}

// End releases the lock acquired by Begin().
func (db *database) End() {
	db.m.Unlock()
}

// View executes a function within the context of a managed read-only
// transaction.
func (db *database) View(
	ctx context.Context,
	fn func(tx *bbolt.Tx),
) {
	bboltx.Must(db.m.RLock(ctx))
	defer db.m.RUnlock()

	bboltx.Must(
		db.actual.View(
			func(tx *bbolt.Tx) (err error) {
				defer bboltx.Recover(&err)
				fn(tx)
				return nil
			},
		),
	)
}

// Close closes the database.
func (db *database) Close() error {
	return db.close(db.actual)
}
