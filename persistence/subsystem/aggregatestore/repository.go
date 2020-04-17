package aggregatestore

import (
	"context"
)

// Repository is an interface for reading persisted aggregate state.
type Repository interface {
	// LoadRevision loads the current revision of an aggregate instance.
	//
	// ak is the aggregate handler's identity key, id is the instance ID.
	LoadRevision(ctx context.Context, hk, id string) (Revision, error)
}
