package sql

import (
	"context"
	"database/sql"
	"errors"

	"github.com/dogmatiq/infix/draftspecs/envelopespec"
	"github.com/dogmatiq/infix/persistence/subsystem/queuestore"
)

// queueDriver is the subset of the Driver interface that is concerned with the
// message queue subsystem.
type queueDriver interface {
	// InsertQueueMessage inserts a message in the queue.
	//
	// It returns false if the row already exists.
	InsertQueueMessage(
		ctx context.Context,
		tx *sql.Tx,
		ak string,
		m *queuestore.Message,
	) (bool, error)

	// UpdateQueueMessage updates meta-data about a message that is already on
	// the queue.
	//
	// It returns false if the row does not exists or m.Revision is not current.
	UpdateQueueMessage(
		ctx context.Context,
		tx *sql.Tx,
		ak string,
		m *queuestore.Message,
	) (bool, error)

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
// If the message is already on the queue its meta-data is updated.
//
// m.Revision must be the revision of the message as currently persisted,
// otherwise an optimistic concurrency conflict has occurred, the message
// is not saved and ErrConflict is returned.
func (t *transaction) SaveMessageToQueue(
	ctx context.Context,
	m *queuestore.Message,
) error {
	if err := t.begin(ctx); err != nil {
		return err
	}

	op := t.ds.driver.InsertQueueMessage
	if m.Revision > 0 {
		op = t.ds.driver.UpdateQueueMessage
	}

	ok, err := op(
		ctx,
		t.actual,
		t.ds.appKey,
		m,
	)
	if ok || err != nil {
		return err
	}

	return queuestore.ErrConflict
}

// RemoveMessageFromQueue removes a specific message from the application's
// message queue.
//
// m.Revision must be the revision of the message as currently persisted,
// otherwise an optimistic concurrency conflict has occurred, the message
// remains on the queue and ErrConflict is returned.
func (t *transaction) RemoveMessageFromQueue(
	ctx context.Context,
	m *queuestore.Message,
) (err error) {
	if err := t.begin(ctx); err != nil {
		return err
	}

	return errors.New("not implemented")
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
