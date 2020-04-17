package fixtures

import (
	"context"

	"github.com/dogmatiq/infix/persistence/subsystem/aggregatestore"
)

// AggregateStoreRepositoryStub is a test implementation of the
// aggregatestore.Repository interface.
type AggregateStoreRepositoryStub struct {
	aggregatestore.Repository

	LoadRevisionFunc func(context.Context, string, string) (aggregatestore.Revision, error)
}

// LoadRevision loads the current revision of an aggregate instance.
func (r *AggregateStoreRepositoryStub) LoadRevision(ctx context.Context, hk, id string) (aggregatestore.Revision, error) {
	if r.LoadRevisionFunc != nil {
		return r.LoadRevisionFunc(ctx, hk, id)
	}

	if r.Repository != nil {
		return r.Repository.LoadRevision(ctx, hk, id)
	}

	return 0, nil
}
