package persistence

import (
	"context"
)

// A Queue is a set of messages that are yet to be handled.
type Queue interface {
	// Begin starts a transaction for a message on the application's message
	// queue that is ready to be handled.
	//
	// If no messages are ready to be handled, it blocks until one becomes
	// ready, ctx is canceled, or an error occurs.
	Begin(ctx context.Context) (Transaction, error)
}
