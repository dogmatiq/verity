package boltdb

import (
	"context"
	"errors"
)

// transaction is an implementation of persistence.Transaction for BoltDB
// data stores.
type transaction struct {
}

// Commit applies the changes from the transaction.
func (t *transaction) Commit(ctx context.Context) error {
	return errors.New("not implemented")
}

// Rollback aborts the transaction.
func (t *transaction) Rollback() error {
	return errors.New("not implemented")
}
