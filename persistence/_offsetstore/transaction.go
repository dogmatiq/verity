package offsetstore

import (
	"context"

	"github.com/dogmatiq/infix/eventstream"
)

// Transaction defines the primitive persistence operations for manipulating
// the offset store.
type Transaction interface {
	// SaveOffset returns the offset of the next event from a specific source
	// application that is to be handled by a specific handler.
	//
	// hk is the identity key of the message handler. ak is the identity key of
	// the source application.
	//
	// c must be the offset for this handler and application as currently
	// persisted, otherwise an optimistic concurrency conflict has occurred, the
	// offset is not saved and ok is false.
	//
	// o is the offset of the next event to be consumed from the application's
	// stream for this process handler.
	SaveOffset(
		ctx context.Context,
		hk, ak string,
		c, o eventstream.Offset,
	) (ok bool, err error)
}
