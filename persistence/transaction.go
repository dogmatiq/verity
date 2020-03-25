package persistence

import (
	"context"
)

// Transaction exposes persistence operations that can be performed atomically.
type Transaction interface {
	// AggregateTransaction
	// ProcessTransaction
	// QueueTransaction
	// eventstore.Transaction

	// Commit applies the changes from the transaction.
	Commit(ctx context.Context) error

	// Rollback aborts the transaction.
	Rollback() error
}
