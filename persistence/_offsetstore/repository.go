package offsetstore

import (
	"context"

	"github.com/dogmatiq/infix/eventstream"
)

// Repository is an interface for reading stream offsets.
type Repository interface {
	// LoadOffset returns the offset of the next event from a specific source
	// application that is to be handled by a specific handler.
	//
	// hk is the identity key of the message handler. ak is the identity key of
	// the source application.
	LoadOffset(ctx context.Context, hk, ak string) (eventstream.Offset, error)
}
