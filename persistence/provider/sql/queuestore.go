package sql

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"github.com/dogmatiq/infix/draftspecs/envelopespec"
	"github.com/dogmatiq/infix/persistence/subsystem/queuestore"
)

// queueDriver is the subset of the Driver interface that is concerned with the
// message queue subsystem.
type queueDriver interface {
	// InsertQueueMessage saves a message to the queue.
	InsertQueueMessage(
		ctx context.Context,
		tx *sql.Tx,
		ak string,
		env *envelopespec.Envelope,
		n time.Time,
	) error

	// SelectQueueMessages selects up to n messages from the queue.
	SelectQueueMessages(
		ctx context.Context,
		db *sql.DB,
		ak string,
		n int,
	) (*sql.Rows, error)

	// ScanQueueMessage scans the next message from a row-set returned by
	// SelectQueueMessages().
	ScanQueueMessage(
		rows *sql.Rows,
		m *queuestore.Message,
	) error
}

// SaveMessageToQueue persists a message to the application's message queue.
//
// n indicates when the next attempt at handling the message is to be made.
func (t *transaction) SaveMessageToQueue(
	ctx context.Context,
	env *envelopespec.Envelope,
	n time.Time,
) error {
	if err := t.begin(ctx); err != nil {
		return err
	}

	return t.ds.driver.InsertQueueMessage(
		ctx,
		t.actual,
		t.ds.appKey,
		env,
		n,
	)
}

// RemoveMessageFromQueue removes a specific message from the application's
// message queue.
//
// m.Revision must be the revision of the message as currently persisted,
// otherwise an optimistic concurrency conflict has occurred, the message
// remains on the queue and ok is false.
func (t *transaction) RemoveMessageFromQueue(
	ctx context.Context,
	m *queuestore.Message,
) (ok bool, err error) {
	return false, errors.New("not implemented")
}

// queueStoreRepository is an implementation of queuestore.Repository that
// stores queued messages in an SQL database.
type queueStoreRepository struct {
	db     *sql.DB
	driver Driver
	appKey string
}

// LoadQueueMessages loads the next n messages from the queue.
func (r *queueStoreRepository) LoadQueueMessages(
	ctx context.Context,
	n int,
) ([]*queuestore.Message, error) {
	rows, err := r.driver.SelectQueueMessages(ctx, r.db, r.appKey, n)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := make([]*queuestore.Message, 0, n)

	for rows.Next() {
		m := &queuestore.Message{
			Envelope: &envelopespec.Envelope{
				MetaData: &envelopespec.MetaData{
					Source: &envelopespec.Source{
						Application: &envelopespec.Identity{},
						Handler:     &envelopespec.Identity{},
					},
				},
			},
		}

		if err := r.driver.ScanQueueMessage(rows, m); err != nil {
			return nil, err
		}

		result = append(result, m)
	}

	return result, nil
}
