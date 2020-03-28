package persistence

import (
	"context"
	"errors"

	"github.com/dogmatiq/infix/persistence/eventstore"
)

// ErrTransactionClosed is returned by all methods on Transaction once the
// transaction is committed or rolled-back.
var ErrTransactionClosed = errors.New("transaction already committed or rolled-back")

// Transaction exposes persistence operations that can be performed atomically.
//
// Transactions are not safe for concurrent use.
type Transaction interface {
	eventstore.Transaction

	// Commit applies the changes from the transaction.
	Commit(ctx context.Context) error

	// Rollback aborts the transaction.
	Rollback() error
}
