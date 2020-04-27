package common

import (
	"context"

	"github.com/dogmatiq/infix/persistence"
)

// WithTransactionRollback is a variant of persistence.WithTransaction() for
// testing that rolls the transaction back instead of committing.
func WithTransactionRollback(
	ctx context.Context,
	ds persistence.DataStore,
	fn func(tx persistence.ManagedTransaction) error,
) error {
	tx, err := ds.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if err := fn(tx); err != nil {
		return err
	}

	return tx.Rollback()
}
