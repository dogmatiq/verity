package memory

import (
	"context"
	"errors"
)

// transaction is an implementation of persistence.Transaction for in-memory
// data stores.
type transaction struct {
	ds *dataStore
}

// Commit applies the changes from the transaction.
func (t *transaction) Commit(ctx context.Context) error {
	_, err := t.ds.get()
	if err != nil {
		return err
	}

	return errors.New("not implemented")
}

// Rollback aborts the transaction.
func (t *transaction) Rollback() error {
	return errors.New("not implemented")
}
