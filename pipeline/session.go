package pipeline

import (
	"context"
	"time"

	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/infix/draftspecs/envelopespec"
	"github.com/dogmatiq/infix/persistence"
)

// A Session is a specialized transaction that encapsulates an atomic
// application state change brought about by a single message.
type Session interface {
	// MessageID returns the ID of the message that started the session.
	MessageID() string

	// FailureCount returns the number of times this message has already been
	// attempted without success, not including this attempt.
	FailureCount() uint

	// Envelope returns the message envelope.
	Envelope() *envelopespec.Envelope

	// Message returns the Dogma message that is to be handled.
	//
	// It returns an error if the message can not be unpacked.
	Message() (dogma.Message, error)

	// Tx returns the transaction used to persist data within this session.
	//
	// It starts the transaction if it has not already been started.
	Tx(ctx context.Context) (persistence.ManagedTransaction, error)

	// Ack acknowledges successful handling of the message.
	//
	// It commits the changes performed in the session's transaction.
	Ack(ctx context.Context) error

	// Nack indicates an error while handling the message.
	//
	// It discards the changes performed in the session's transaction and defers
	// handling of the message until n.
	Nack(ctx context.Context, n time.Time) error

	// Close releases the session.
	//
	// It must be called regardless of whether Ack() or Nack() are called.
	Close() error
}
