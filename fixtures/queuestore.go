package fixtures

import (
	"context"

	"github.com/dogmatiq/infix/persistence/subsystem/queuestore"
)

// QueueStoreRepositoryStub is a test implementation of the
// queuestore.Repository interface.
type QueueStoreRepositoryStub struct {
	queuestore.Repository

	LoadQueueMessagesFunc func(context.Context, int) ([]*queuestore.Message, error)
}

// LoadQueueMessages loads the next n messages from the queue.
func (r *QueueStoreRepositoryStub) LoadQueueMessages(ctx context.Context, n int) ([]*queuestore.Message, error) {
	if r.LoadQueueMessagesFunc != nil {
		return r.LoadQueueMessagesFunc(ctx, n)
	}

	if r.Repository != nil {
		return r.Repository.LoadQueueMessages(ctx, n)
	}

	return nil, nil
}
