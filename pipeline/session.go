package pipeline

import (
	"context"
	"time"

	"github.com/dogmatiq/infix/envelope"
	"github.com/dogmatiq/infix/persistence"
)

// A Session is a specialized transaction that encapsulates an atomic
// application state change brought about by a single message.
type Session interface {
	// MessageID returns the ID of the message that started the session.
	MessageID() string

	// FailureCount returns the number of times this message has already been
	// attempted, not including this attempt.
	FailureCount() uint

	// Envelope returns the envelope containing the message to be handled.
	Envelope() (*envelope.Envelope, error)

	// Tx returns the transaction used to persist data within this session.
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
