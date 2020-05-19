package persistence

import (
	"context"
	"errors"

	"github.com/dogmatiq/infix/draftspecs/envelopespec"
	"github.com/dogmatiq/infix/persistence/subsystem/aggregatestore"
	"github.com/dogmatiq/infix/persistence/subsystem/queuestore"
)

// ErrTransactionClosed is returned by all methods on Transaction once the
// transaction is committed or rolled-back.
var ErrTransactionClosed = errors.New("transaction already committed or rolled-back")

// Transaction exposes persistence operations that can be performed atomically.
// Transactions are not safe for concurrent use.
type Transaction interface {
	// SaveAggregateMetaData persists meta-data about an aggregate instance.
	//
	// md.Revision must be the revision of the instance as currently persisted,
	// otherwise an optimistic concurrency conflict has occurred, the meta-data
	// is not saved and ErrConflict is returned.
	SaveAggregateMetaData(
		ctx context.Context,
		md *aggregatestore.MetaData,
	) error

	// SaveEvent persists an event in the application's event store.
	//
	// It returns the event's offset.
	SaveEvent(
		ctx context.Context,
		env *envelopespec.Envelope,
	) (uint64, error)

	// SaveMessageToQueue persists a message to the application's message queue.
	//
	// If the message is already on the queue its meta-data is updated.
	//
	// i.Revision must be the revision of the message as currently persisted,
	// otherwise an optimistic concurrency conflict has occurred, the message
	// is not saved and ErrConflict is returned.
	SaveMessageToQueue(
		ctx context.Context,
		i *queuestore.Item,
	) error

	// RemoveMessageFromQueue removes a specific message from the application's
	// message queue.
	//
	// i.Revision must be the revision of the message as currently persisted,
	// otherwise an optimistic concurrency conflict has occurred, the message
	// remains on the queue and ErrConflict is returned.
	RemoveMessageFromQueue(
		ctx context.Context,
		i *queuestore.Item,
	) error

	// SaveOffset persists the "next" offset to be consumed for a specific
	// application.
	//
	// ak is the application's identity key.
	//
	// c must be the offset currently associated with ak, otherwise an
	// optimistic concurrency conflict has occurred, the offset is not saved and
	// ErrConflict is returned.
	SaveOffset(
		ctx context.Context,
		ak string,
		c, n uint64,
	) error

	// Commit applies the changes from the transaction.
	Commit(ctx context.Context) (TransactionResult, error)

	// Rollback aborts the transaction.
	Rollback() error
}

// TransactionResult contains information about a successfully committed
// transaction.
type TransactionResult = Result
