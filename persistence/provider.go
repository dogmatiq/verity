package persistence

import (
	"context"

	"github.com/dogmatiq/configkit"
)

// Provider is an interface used be the engine to persist and retrieve
// application state.
type Provider interface {
	// EventStream returns the event stream for the given application.
	EventStream(ctx context.Context, app configkit.Identity) error
}
