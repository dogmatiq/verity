package sql

import (
	"context"
	"database/sql"

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
		p *queuestore.Parcel,
	) (bool, error)

	// UpdateQueueMessage updates meta-data about a message that is already on
	// the queue.
	//
	// It returns false if the row does not exists or p.Revision is not current.
	UpdateQueueMessage(
		ctx context.Context,
		tx *sql.Tx,
		ak string,
		p *queuestore.Parcel,
	) (bool, error)

	// DeleteQueueMessage deletes a message from the queue.
	//
	// It returns false if the row does not exists or p.Revision is not current.
	DeleteQueueMessage(
		ctx context.Context,
		tx *sql.Tx,
		ak string,
		p *queuestore.Parcel,
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
		p *queuestore.Parcel,
	) error
}

// SaveMessageToQueue persists a message to the application's message queue.
//
// If the message is already on the queue its meta-data is updated.
//
// p.Revision must be the revision of the message as currently persisted,
// otherwise an optimistic concurrency conflict has occurred, the message
// is not saved and ErrConflict is returned.
func (t *transaction) SaveMessageToQueue(
	ctx context.Context,
	p *queuestore.Parcel,
) error {
	if err := t.begin(ctx); err != nil {
		return err
	}

	op := t.ds.driver.InsertQueueMessage
	if p.Revision > 0 {
		op = t.ds.driver.UpdateQueueMessage
	}

	ok, err := op(
		ctx,
		t.actual,
		t.ds.appKey,
		p,
	)
	if ok || err != nil {
		return err
	}

	return queuestore.ErrConflict
}

// RemoveMessageFromQueue removes a specific message from the application's
// message queue.
//
// p.Revision must be the revision of the message as currently persisted,
// otherwise an optimistic concurrency conflict has occurred, the message
// remains on the queue and ErrConflict is returned.
func (t *transaction) RemoveMessageFromQueue(
	ctx context.Context,
	p *queuestore.Parcel,
) (err error) {
	if err := t.begin(ctx); err != nil {
		return err
	}

	ok, err := t.ds.driver.DeleteQueueMessage(
		ctx,
		t.actual,
		t.ds.appKey,
		p,
	)
	if ok || err != nil {
		return err
	}

	return queuestore.ErrConflict
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
) ([]*queuestore.Parcel, error) {
	rows, err := r.driver.SelectQueueMessages(ctx, r.db, r.appKey, n)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := make([]*queuestore.Parcel, 0, n)

	for rows.Next() {
		p := &queuestore.Parcel{
			Envelope: &envelopespec.Envelope{
				MetaData: &envelopespec.MetaData{
					Source: &envelopespec.Source{
						Application: &envelopespec.Identity{},
						Handler:     &envelopespec.Identity{},
					},
				},
			},
		}

		if err := r.driver.ScanQueueMessage(rows, p); err != nil {
			return nil, err
		}

		result = append(result, p)
	}

	return result, nil
}
