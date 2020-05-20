package sql

import (
	"context"
	"database/sql"

	"github.com/dogmatiq/infix/draftspecs/envelopespec"
	"github.com/dogmatiq/infix/persistence"
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
		i *queuestore.Item,
	) (bool, error)

	// UpdateQueueMessage updates meta-data about a message that is already on
	// the queue.
	//
	// It returns false if the row does not exist or i.Revision is not current.
	UpdateQueueMessage(
		ctx context.Context,
		tx *sql.Tx,
		ak string,
		i *queuestore.Item,
	) (bool, error)

	// DeleteQueueMessage deletes a message from the queue.
	//
	// It returns false if the row does not exist or i.Revision is not current.
	DeleteQueueMessage(
		ctx context.Context,
		tx *sql.Tx,
		ak string,
		i *queuestore.Item,
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
		i *queuestore.Item,
	) error
}

// LoadQueueMessages loads the next n messages from the queue.
func (ds *dataStore) LoadQueueMessages(
	ctx context.Context,
	n int,
) ([]*queuestore.Item, error) {
	rows, err := ds.driver.SelectQueueMessages(ctx, ds.db, ds.appKey, n)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := make([]*queuestore.Item, 0, n)

	for rows.Next() {
		i := &queuestore.Item{
			Envelope: &envelopespec.Envelope{
				MetaData: &envelopespec.MetaData{
					Source: &envelopespec.Source{
						Application: &envelopespec.Identity{},
						Handler:     &envelopespec.Identity{},
					},
				},
			},
		}

		if err := ds.driver.ScanQueueMessage(rows, i); err != nil {
			return nil, err
		}

		result = append(result, i)
	}

	return result, nil
}

// VisitSaveQueueItem applies the changes in a "SaveQueueItem" operation to the
// database.
func (c *committer) VisitSaveQueueItem(
	ctx context.Context,
	op persistence.SaveQueueItem,
) error {
	fn := c.driver.InsertQueueMessage
	if op.Item.Revision > 0 {
		fn = c.driver.UpdateQueueMessage
	}

	if ok, err := fn(
		ctx,
		c.tx,
		c.appKey,
		&op.Item,
	); ok || err != nil {
		return err
	}

	return persistence.ConflictError{
		Cause: op,
	}
}

// VisitRemoveQueueItem applies the changes in a "RemoveQueueItem" operation to
// the database.
func (c *committer) VisitRemoveQueueItem(
	ctx context.Context,
	op persistence.RemoveQueueItem,
) (err error) {
	if ok, err := c.driver.DeleteQueueMessage(
		ctx,
		c.tx,
		c.appKey,
		&op.Item,
	); ok || err != nil {
		return err
	}

	return persistence.ConflictError{
		Cause: op,
	}
}
