package process

import (
	"context"

	"github.com/dogmatiq/infix/eventstream"
)

// Repository is an interface for reading process data.
type Repository interface {
	// Load loads a process instance.
	//
	// If the instance does not exist, ok is false.
	Load(ctx context.Context, id ID) (i *Instance, ok bool, err error)

	// LoadOffset returns the offset of the next event to be consumed from a
	// specific application's event stream for a specific process message
	// handler.
	//
	// hk is the identity key of the process message handler. ak is the identity
	// key of the source application.
	LoadOffset(ctx context.Context, hk, ak string) (eventstream.Offset, error)
}
