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
	result persistence.TransactionResult
}

// Commit applies the changes from the transaction.
func (t *transaction) Commit(
	ctx context.Context,
) (*persistence.TransactionResult, error) {
	defer t.end()

	if t.ds == nil {
		return nil, persistence.ErrTransactionClosed
	}

	if err := t.ds.checkOpen(); err != nil {
		return nil, err
	}

	if t.actual != nil {
		return &t.result, t.actual.Commit()
	}

	return t.result, nil
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

	if t.ds == nil {
		return persistence.ErrTransactionClosed
	}

	if t.actual == nil {
		t.actual, err = t.ds.driver.Begin(ctx, t.ds.db)
	}

	t.result = &persistence.TransactionResult{}

	return err
}

// end rolls-back the actual transaction and marks the transaction as ended.
func (t *transaction) end() {
	if t.actual != nil {
		t.actual.Rollback()
		t.actual = nil
	}

	t.ds = nil
	t.result = nil
}
