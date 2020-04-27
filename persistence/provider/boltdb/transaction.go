package boltdb

import (
	"context"

	"github.com/dogmatiq/infix/persistence"
	"go.etcd.io/bbolt"
)

// transaction is an implementation of persistence.Transaction for BoltDB
// data stores.
type transaction struct {
	ds     *dataStore
	appKey []byte
	actual *bbolt.Tx
}

// Commit applies the changes from the transaction.
func (t *transaction) Commit(ctx context.Context) error {
	defer t.end()

	if t.ds == nil {
		return persistence.ErrTransactionClosed
	}

	if err := t.ds.checkOpen(); err != nil {
		return err
	}

	if t.actual != nil {
		return t.actual.Commit()
	}

	return nil
}

// Rollback aborts the transaction.
func (t *transaction) Rollback() (err error) {
	defer t.end()

	if t.ds == nil {
		return persistence.ErrTransactionClosed
	}

	if err := t.ds.checkOpen(); err != nil {
		return err
	}

	if t.actual != nil {
		return t.actual.Rollback()
	}

	return nil
}

// begin acquires a write-lock on the database and begins an actual BoltDB
// transaction.
func (t *transaction) begin(ctx context.Context) error {
	if t.ds == nil {
		return persistence.ErrTransactionClosed
	}

	if err := t.ds.checkOpen(); err != nil {
		return err
	}

	if t.actual == nil {
		t.actual = t.ds.db.Begin(ctx)
	}

	return nil
}

// end rolls-back the actual transaction, releases the database lock, and marks
// the transaction as ended.
func (t *transaction) end() {
	if t.actual != nil {
		t.actual.Rollback()
		t.ds.db.End()
		t.actual = nil
	}

	t.ds = nil
}
