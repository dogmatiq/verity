package offsetstore

import (
	"context"

	"github.com/dogmatiq/infix/eventstream"
)

// Repository is an interface for reading persisted offsets.
type Repository interface {
	// LoadOffset loads the offset associated with a specific application.
	//
	// ak is the application's identity key.
	LoadOffset(ctx context.Context, ak string) (eventstream.Offset, error)
}
