package persistence

import (
	"context"
	"errors"

	"github.com/dogmatiq/infix/persistence/subsystem/eventstore"
	"github.com/dogmatiq/infix/persistence/subsystem/queue"
)

// ErrTransactionClosed is returned by all methods on Transaction once the
// transaction is committed or rolled-back.
var ErrTransactionClosed = errors.New("transaction already committed or rolled-back")

// Transaction exposes persistence operations that can be performed atomically.
//
// Transactions are not safe for concurrent use.
type Transaction interface {
	eventstore.Transaction
	queue.Transaction

	// Commit applies the changes from the transaction.
	Commit(ctx context.Context) error

	// Rollback aborts the transaction.
	Rollback() error
}

// WithTransaction executes fn inside a transaction.
//
// If fn returns nil the transaction is committed, Otherwise, the transaction is
// rolled-back and the error is returned.
func WithTransaction(
	ctx context.Context,
	ds DataStore,
	fn func(Transaction) error,
) error {
	tx, err := ds.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if err := fn(tx); err != nil {
		return err
	}

	return tx.Commit(ctx)
}
