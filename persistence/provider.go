package persistence

import (
	"context"

	"github.com/dogmatiq/configkit"
	"github.com/dogmatiq/marshalkit"
)

// Provider is an interface used by the engine to obtain application-specific
// DataStore instances.
type Provider interface {
	// Open returns a data-store for a specific application.
	Open(
		ctx context.Context,
		cfg configkit.RichApplication,
		m marshalkit.Marshaler,
	) (DataStore, error)
}

// DataStore is an interface used by the engine to persist and retrieve
// application state.
type DataStore interface {
	// EventStream returns the event stream for the given application.
	EventStream(ctx context.Context) (Stream, error)

	// Close closes the data store.
	Close() error
}
