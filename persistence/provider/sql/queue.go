package sql

import (
	"context"
	"database/sql"
	"errors"

	"github.com/dogmatiq/infix/draftspecs/envelopespec"
	"github.com/dogmatiq/infix/persistence/subsystem/queue"
)

// queueDriver is the subset of the Driver interface that is concerned with the
// message queue subsystem.
type queueDriver interface {
	// InsertQueuedMessages saves messages to the queue.
	InsertQueuedMessages(
		ctx context.Context,
		tx *sql.Tx,
		ak string,
		envelopes []*envelopespec.Envelope,
	) error

	// SelectQueuedMessages selects up to n messages from the queue.
	SelectQueuedMessages(
		ctx context.Context,
		db *sql.DB,
		ak string,
		n int,
	) (*sql.Rows, error)

	// ScanQueuedMessage scans the next message from a row-set returned by
	// SelectQueuedMessages().
	ScanQueuedMessage(
		rows *sql.Rows,
		m *queue.Message,
	) error
}

// AddMessagesToQueue adds messages to the application's message queue.
func (t *transaction) AddMessagesToQueue(
	ctx context.Context,
	envelopes []*envelopespec.Envelope,
) error {
	if err := t.begin(ctx); err != nil {
		return err
	}

	return t.ds.driver.InsertQueuedMessages(
		ctx,
		t.actual,
		t.ds.appKey,
		envelopes,
	)
}

// DequeueMessage removes a message from the application's message queue.
//
// m.Revision must be the revision of the queued message as currently persisted,
// otherwise an optimistic concurrency conflict has occurred, the message
// remains on the queue and ok is false.
func (t *transaction) DequeueMessage(
	ctx context.Context,
	m *queue.Message,
) (ok bool, err error) {
	return false, errors.New("not implemented")
}

// UpdateQueuedMessage updates meta-data about a queued message.
//
// The following fields are updated:
//  - NextAttemptAt
//
// m.Revision must be the revision of the queued message as currently persisted,
// otherwise an optimistic concurrency conflict has occurred, the message is not
// updated and ok is false.
func (t *transaction) UpdateQueuedMessage(
	ctx context.Context,
	m *queue.Message,
) (ok bool, err error) {
	return false, errors.New("not implemented")
}

// queueRepository is an implementation of queue.Repository that stores queued
// messages in an SQL database.
type queueRepository struct {
	db     *sql.DB
	driver Driver
	appKey string
}

// LoadQueuedMessages loads the next n messages from the queue.
func (r *queueRepository) LoadQueuedMessages(
	ctx context.Context,
	n int,
) ([]*queue.Message, error) {
	rows, err := r.driver.SelectQueuedMessages(ctx, r.db, r.appKey, n)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := make([]*queue.Message, 0, n)

	for rows.Next() {
		m := &queue.Message{
			Envelope: &envelopespec.Envelope{
				MetaData: &envelopespec.MetaData{
					Source: &envelopespec.Source{
						Application: &envelopespec.Identity{},
						Handler:     &envelopespec.Identity{},
					},
				},
			},
		}

		if err := r.driver.ScanQueuedMessage(rows, m); err != nil {
			return nil, err
		}

		result = append(result, m)
	}

	return result, nil
}
