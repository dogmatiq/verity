package boltdb

import (
	"context"
	"errors"
	"time"

	"github.com/dogmatiq/infix/draftspecs/envelopespec"
	"github.com/dogmatiq/infix/persistence/subsystem/queue"
)

// EnqueueMessages adds messages to the application's message queue.
func (t *transaction) EnqueueMessages(
	ctx context.Context,
	envelopes []*envelopespec.Envelope,
) error {
	return nil
}

// DequeueMessage removes a message from the application's message queue.
//
// m.Revision must be the revision of the queued message as currently
// persisted, otherwise an optimistic concurrency conflict has occured, the
// message remains on the queue and ok is false.
func (t *transaction) DequeueMessage(
	ctx context.Context,
	m *queue.Message,
) (ok bool, err error) {
	return false, errors.New("not implemented")
}

// DelayQueuedMessage returns defers the next attempt of a queued message
// after a failure.
//
// n is the time at which the next attempt at handling the message occurs.
//
// m.Revision must be the revision of the queued message as currently
// persisted, otherwise an optimistic concurrency conflict has occured, the
// message is not delayed and ok is false.
func (t *transaction) DelayQueuedMessage(
	ctx context.Context,
	m *queue.Message,
	n time.Time,
) (ok bool, err error) {
	return false, errors.New("not implemented")
}

// queueRepository is an implementation of queue.Repository that stores queued
// messages in a BoltDB database.
type queueRepository struct {
	db     *database
	appKey []byte
}

// LoadQueuedMessages loads the next n messages from the queue.
func (r *queueRepository) LoadQueuedMessages(
	ctx context.Context,
	n int,
) ([]*queue.Message, error) {
	return nil, nil
}
