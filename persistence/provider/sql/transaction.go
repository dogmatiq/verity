package sql

import (
	"context"
	"database/sql"

	"github.com/dogmatiq/infix/persistence"
)

// transaction is an implementation of persistence.Transaction for SQL data
// stores.
type transaction struct {
	ds     *dataStore
	actual *sql.Tx
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
func (t *transaction) Rollback() error {
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

func (t *transaction) begin(ctx context.Context) error {
	var err error

	if t.actual == nil {
		t.actual, err = t.ds.db.BeginTx(ctx, nil)
	}

	return err
}

// end rolls-back the actual transaction and marks the transaction as ended.
func (t *transaction) end() {
	if t.actual != nil {
		t.actual.Rollback()
		t.actual = nil
	}

	t.ds = nil
}
