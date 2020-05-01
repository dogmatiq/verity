package persistence

import (
	"context"
	"errors"

	"github.com/dogmatiq/infix/persistence/subsystem/aggregatestore"
	"github.com/dogmatiq/infix/persistence/subsystem/eventstore"
	"github.com/dogmatiq/infix/persistence/subsystem/offsetstore"
	"github.com/dogmatiq/infix/persistence/subsystem/queuestore"
)

// ErrTransactionClosed is returned by all methods on Transaction once the
// transaction is committed or rolled-back.
var ErrTransactionClosed = errors.New("transaction already committed or rolled-back")

// Transaction exposes persistence operations that can be performed atomically.
// Transactions are not safe for concurrent use.
type Transaction interface {
	aggregatestore.Transaction
	eventstore.Transaction
	queuestore.Transaction
	offsetstore.Transaction

	// Commit applies the changes from the transaction.
	Commit(ctx context.Context) (TransactionResult, error)

	// Rollback aborts the transaction.
	Rollback() error
}

// TransactionResult contains information about a successfully committed transaction.
type TransactionResult struct {
	// EventStoreItems contains the events persisted within the transaction.
	EventStoreItems []*eventstore.Item
}

// ManagedTransaction is a Transaction that can not be commit or rolled-back
// directly because its life-time is managed for the user.
type ManagedTransaction interface {
	aggregatestore.Transaction
	eventstore.Transaction
	queuestore.Transaction
	offsetstore.Transaction
}

// WithTransaction executes fn inside a transaction.
//
// If fn returns nil the transaction is committed, Otherwise, the transaction is
// rolled-back and the error is returned.
func WithTransaction(
	ctx context.Context,
	ds DataStore,
	fn func(ManagedTransaction) error,
) (TransactionResult, error) {
	tx, err := ds.Begin(ctx)
	if err != nil {
		return TransactionResult{}, err
	}
	defer tx.Rollback()

	if err := fn(tx); err != nil {
		return TransactionResult{}, err
	}

	return tx.Commit(ctx)
}
