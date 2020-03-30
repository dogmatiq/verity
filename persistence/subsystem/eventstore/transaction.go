package eventstore

import (
	"context"

	"github.com/dogmatiq/infix/draftspecs/envelopespec"
)

// Transaction defines the primitive persistence operations for manipulating the
// event store.
type Transaction interface {
	// SaveEvent persists an event in the application's event store.
	//
	// It returns the event's offset.
	SaveEvent(
		ctx context.Context,
		env *envelopespec.Envelope,
	) (Offset, error)
}
