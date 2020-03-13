package persistence

import (
	"context"

	"github.com/dogmatiq/configkit"
	"github.com/dogmatiq/marshalkit"
)

// Provider is an interface used be the engine to persist and retrieve
// application state.
type Provider interface {
	// Initialize prepare the provider for use.
	Initialize(ctx context.Context, m marshalkit.Marshaler) error

	// EventStream returns the event stream for the given application.
	EventStream(ctx context.Context, app configkit.Identity) (Stream, error)
}
