package persistence

import (
	"context"

	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/infix/envelope"
)

// Transaction exposes persistence operations that can be performed atomically
// in order to handle a single message.
type Transaction interface {
	// LoadAggregate loads an aggregate instance.
	LoadAggregate(ctx context.Context, hk, id string) (dogma.AggregateRoot, uint64, error)

	// PersistAggregate updates (or creates) an aggregate instance.
	PersistAggregate(ctx context.Context, hk, id string, rev uint64, r dogma.AggregateRoot) error

	// DeleteAggregate deletes an aggregate instance.
	DeleteAggregate(ctx context.Context, hk, id string, rev uint64) error

	// LoadProcess loads a process instance.
	LoadProcess(ctx context.Context, hk, id string) (dogma.ProcessRoot, uint64, error)

	// SaveProcess updates (or creates) a process instance.
	SaveProcess(ctx context.Context, hk, id string, rev uint64, r dogma.ProcessRoot) error

	// DeleteProcess deletes a process instance.
	DeleteProcess(ctx context.Context, hk, id string, rev uint64) error

	// Enqueue adds a message to the application's message queue.
	Enqueue(ctx context.Context, env *envelope.Envelope) error

	// Append adds a message to the application's event stream.
	Append(ctx context.Context, env *envelope.Envelope) error

	// Commit applies the changes from the transaction.
	Commit(ctx context.Context) error

	// Rollback cancels the transaction due to an error.
	Rollback(ctx context.Context, err error) error

	// Close closes the transaction. It must be called regardless of whether the
	// transactions is committed or rolled-back.
	Close() error
}
