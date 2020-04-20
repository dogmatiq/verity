package fixtures

import (
	"context"

	"github.com/dogmatiq/infix/persistence/subsystem/aggregatestore"
)

// AggregateStoreRepositoryStub is a test implementation of the
// aggregatestore.Repository interface.
type AggregateStoreRepositoryStub struct {
	aggregatestore.Repository

	LoadMetaDataFunc func(context.Context, string, string) (*aggregatestore.MetaData, error)
}

// LoadMetaData loads the meta-data for an aggregate instance.
func (r *AggregateStoreRepositoryStub) LoadMetaData(ctx context.Context, hk, id string) (*aggregatestore.MetaData, error) {
	if r.LoadMetaDataFunc != nil {
		return r.LoadMetaDataFunc(ctx, hk, id)
	}

	if r.Repository != nil {
		return r.Repository.LoadMetaData(ctx, hk, id)
	}

	return nil, nil
}
