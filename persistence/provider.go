package persistence

import (
	"context"
	"errors"
)

// ErrDataStoreLocked indicates that an application's data store can not be
// opened because it is locked by another engine instance.
var ErrDataStoreLocked = errors.New("data store is locked")

// Provider is an interface used by the engine to obtain application-specific
// DataStore instances.
type Provider interface {
	// Open returns a data-store for a specific application.
	//
	// k is the identity key of the application.
	//
	// Data stores are opened for exclusive use. If another engine instance has
	// already opened this application's data-store, ErrDataStoreLocked is
	// returned.
	Open(ctx context.Context, k string) (DataStore, error)
}
