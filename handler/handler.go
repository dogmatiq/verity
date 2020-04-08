package handler

import (
	"context"

	"github.com/dogmatiq/infix/envelope"
	"github.com/dogmatiq/infix/persistence"
	"github.com/dogmatiq/infix/persistence/subsystem/eventstore"
)

// Handler is a function that handles the dogma.Message in the given envelope.
type Handler func(context.Context, Scope, *envelope.Envelope) error

// Scope exposes operations that a handler can perform within the context of
// handling a specific message.
//
// The operations are performed atomically, only taking effect if the handler
// succeeds.
type Scope interface {
	// Tx returns the low-level transaction used to persist data atomically.
	//
	// The operations via this transaction are committed atomically with the
	// operations performed directly on the scope.
	Tx(ctx context.Context) (persistence.ManagedTransaction, error)

	// EnqueueMessage adds a message to the queue.
	EnqueueMessage(ctx context.Context, env *envelope.Envelope) error

	// RecordEvent appends an event to the event stream.
	RecordEvent(ctx context.Context, env *envelope.Envelope) (eventstore.Offset, error)

	// Log records an informational message within the context of the message
	// that is being handled.
	Log(f string, v ...interface{})
}
