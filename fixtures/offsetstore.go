package fixtures

import (
	"context"

	"github.com/dogmatiq/infix/eventstream"
	"github.com/dogmatiq/infix/persistence/subsystem/offsetstore"
)

// OffsetStoreRepositoryStub is a test implementation of the
// offsetstore.Repository interface.
type OffsetStoreRepositoryStub struct {
	offsetstore.Repository

	LoadOffsetFunc func(ctx context.Context, ak string) (eventstream.Offset, error)
}

// LoadOffset loads the offset associated with a specific application.
//
// ak is the application's identity key.
func (r *OffsetStoreRepositoryStub) LoadOffset(
	ctx context.Context,
	ak string,
) (eventstream.Offset, error) {
	if r.LoadOffsetFunc != nil {
		return r.LoadOffsetFunc(ctx, ak)
	}

	if r.Repository != nil {
		return r.Repository.LoadOffset(ctx, ak)
	}

	return 0, nil
}
