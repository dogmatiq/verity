package eventstore

import (
	"context"

	"github.com/dogmatiq/infix/envelope"
)

// Transaction defines the primitive persistence operations for manipulating the
// event store.
type Transaction interface {
	// SaveEvents persists events in the application's event store.
	//
	// It returns the next unused on the stream.
	SaveEvents(
		ctx context.Context,
		envelopes ...*envelope.Envelope,
	) error
}
