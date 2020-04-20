package aggregatestore

import (
	"context"
)

// Repository is an interface for reading persisted aggregate state.
type Repository interface {
	// LoadMetaData loads the meta-data for an aggregate instance.
	//
	// ak is the aggregate handler's identity key, id is the instance ID.
	LoadMetaData(ctx context.Context, hk, id string) (*MetaData, error)
}
