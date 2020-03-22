package sql

import (
	"context"
	"errors"

	"github.com/dogmatiq/infix/envelope"
	"github.com/dogmatiq/infix/persistence"
)

// Queue is an implementation of persistence.Queue that stores messages in an
// SQL database.
type Queue struct {
}

// Begin starts a transaction for a message on the application's message
// queue that is ready to be handled.
//
// If no messages are ready to be handled, it blocks until one becomes
// ready, ctx is canceled, or an error occurs.
func (q *Queue) Begin(ctx context.Context) (persistence.Transaction, *envelope.Envelope, error) {
	return nil, nil, errors.New("not implemented")
}

// Enqueue adds a message to the queue.
func (q *Queue) Enqueue(ctx context.Context, env *envelope.Envelope) error {
	return errors.New("not implemented")
}
