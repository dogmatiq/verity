package offsetstore

import (
	"context"
)

// Repository is an interface for reading a persisted event stream offset of the
// application in question.
type Repository interface {
	// LoadOffset loads the offset of the application with the key ak from the
	// repository.
	LoadOffset(ctx context.Context, ak string) (Offset, error)
}
