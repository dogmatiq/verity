package queue

import (
	"context"
)

// Repository is an interface for reading persisted event messages.
type Repository interface {
	// LoadQueuedMessages loads the next n messages from the queue.
	LoadQueuedMessages(ctx context.Context, n int) ([]*Message, error)
}
