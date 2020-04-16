package pipeline

import (
	"context"
	"time"

	"github.com/dogmatiq/infix/draftspecs/envelopespec"
	"github.com/dogmatiq/infix/parcel"
	"github.com/dogmatiq/infix/persistence"
)

// Request is a request for a pipeline to process a particular message.
type Request interface {
	// MessageID returns the ID of the message in the request.
	MessageID() string

	// FailureCount returns the number of times this message has already been
	// attempted without success, not including this request.
	FailureCount() uint

	// Envelope returns the message envelope.
	Envelope() *envelopespec.Envelope

	// Parcel returns a parcel containing the original Dogma message.
	Parcel() (*parcel.Parcel, error)

	// Tx returns the transaction used to persist data within this request.
	//
	// It starts the transaction if it has not already been started.
	Tx(ctx context.Context) (persistence.ManagedTransaction, error)

	// Ack acknowledges successful handling of the request.
	//
	// It commits the changes performed in the request's transaction.
	Ack(ctx context.Context) error

	// Nack indicates an error while handling the message.
	//
	// It discards the changes performed in the request's transaction and defers
	// handling of the message until n.
	Nack(ctx context.Context, n time.Time) error

	// Close releases the request.
	//
	// It must be called regardless of whether Ack() or Nack() are called.
	Close() error
}
