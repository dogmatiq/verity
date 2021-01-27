package sqlpersistence

import (
	"context"
	"database/sql"

	"github.com/dogmatiq/interopspec/envelopespec"
	"github.com/dogmatiq/verity/persistence"
)

// QueueDriver is the subset of the Driver interface that is concerned with the
// message queue subsystem.
type QueueDriver interface {
	// InsertQueueMessage inserts a message in the queue.
	//
	// It returns false if the row already exists.
	InsertQueueMessage(
		ctx context.Context,
		tx *sql.Tx,
		ak string,
		m persistence.QueueMessage,
	) (bool, error)

	// UpdateQueueMessage updates meta-data about a message that is already on
	// the queue.
	//
	// It returns false if the row does not exist or m.Revision is not current.
	UpdateQueueMessage(
		ctx context.Context,
		tx *sql.Tx,
		ak string,
		m persistence.QueueMessage,
	) (bool, error)

	// DeleteQueueMessage deletes a message from the queue.
	//
	// It returns false if the row does not exist or m.Revision is not current.
	DeleteQueueMessage(
		ctx context.Context,
		tx *sql.Tx,
		ak string,
		m persistence.QueueMessage,
	) (bool, error)

	// DeleteQueueTimeoutMessagesByProcessInstance deletes timeout messages that
	// were produced by a specific process instance.
	DeleteQueueTimeoutMessagesByProcessInstance(
		ctx context.Context,
		tx *sql.Tx,
		ak string,
		inst persistence.ProcessInstance,
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
		m *persistence.QueueMessage,
	) error
}

// LoadQueueMessages loads the next n messages from the queue.
func (ds *dataStore) LoadQueueMessages(
	ctx context.Context,
	n int,
) ([]persistence.QueueMessage, error) {
	rows, err := ds.driver.SelectQueueMessages(ctx, ds.db, ds.appKey, n)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := make([]persistence.QueueMessage, 0, n)

	for rows.Next() {
		m := persistence.QueueMessage{
			Envelope: &envelopespec.Envelope{
				SourceApplication: &envelopespec.Identity{},
				SourceHandler:     &envelopespec.Identity{},
			},
		}

		if err := ds.driver.ScanQueueMessage(rows, &m); err != nil {
			return nil, err
		}

		result = append(result, m)
	}

	return result, nil
}

// VisitSaveQueueMessage applies the changes in a "SaveQueueMessage" operation
// to the database.
func (c *committer) VisitSaveQueueMessage(
	ctx context.Context,
	op persistence.SaveQueueMessage,
) error {
	fn := c.driver.InsertQueueMessage
	if op.Message.Revision > 0 {
		fn = c.driver.UpdateQueueMessage
	}

	if ok, err := fn(
		ctx,
		c.tx,
		c.appKey,
		op.Message,
	); ok || err != nil {
		return err
	}

	return persistence.ConflictError{
		Cause: op,
	}
}

// VisitRemoveQueueMessage applies the changes in a "RemoveQueueMessage"
// operation to the database.
func (c *committer) VisitRemoveQueueMessage(
	ctx context.Context,
	op persistence.RemoveQueueMessage,
) (err error) {
	if ok, err := c.driver.DeleteQueueMessage(
		ctx,
		c.tx,
		c.appKey,
		op.Message,
	); ok || err != nil {
		return err
	}

	return persistence.ConflictError{
		Cause: op,
	}
}
