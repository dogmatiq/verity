package mysql

import (
	"context"
	"database/sql"
	"time"

	"github.com/dogmatiq/infix/internal/x/sqlx"
	"github.com/dogmatiq/infix/persistence/subsystem/queuestore"
)

// InsertQueueMessage inserts a message in the queue.
//
// It returns false if the row already exists.
func (driver) InsertQueueMessage(
	ctx context.Context,
	tx *sql.Tx,
	ak string,
	i *queuestore.Item,
) (_ bool, err error) {
	defer sqlx.Recover(&err)

	// Note: ON DUPLICATE KEY UPDATE is used because INSERT IGNORE ignores
	// more than just key conflicts.
	res := sqlx.Exec(
		ctx,
		tx,
		`INSERT INTO queue SET
				app_key = ?,
				failure_count = ?,
				next_attempt_at = ?,
				message_id = ?,
				causation_id = ?,
				correlation_id = ?,
				source_app_name = ?,
				source_app_key = ?,
				source_handler_name = ?,
				source_handler_key = ?,
				source_instance_id = ?,
				created_at = ?,
				scheduled_for = ?,
				description = ?,
				portable_name = ?,
				media_type = ?,
				data = ?
			ON DUPLICATE KEY UPDATE
				app_key = VALUES(app_key)`,
		ak,
		i.FailureCount,
		i.NextAttemptAt,
		i.Envelope.GetMetaData().GetMessageId(),
		i.Envelope.GetMetaData().GetCausationId(),
		i.Envelope.GetMetaData().GetCorrelationId(),
		i.Envelope.GetMetaData().GetSource().GetApplication().GetName(),
		i.Envelope.GetMetaData().GetSource().GetApplication().GetKey(),
		i.Envelope.GetMetaData().GetSource().GetHandler().GetName(),
		i.Envelope.GetMetaData().GetSource().GetHandler().GetKey(),
		i.Envelope.GetMetaData().GetSource().GetInstanceId(),
		i.Envelope.GetMetaData().GetCreatedAt(),
		i.Envelope.GetMetaData().GetScheduledFor(),
		i.Envelope.GetMetaData().GetDescription(),
		i.Envelope.GetPortableName(),
		i.Envelope.GetMediaType(),
		i.Envelope.GetData(),
	)

	// We use the affected count to check if the row was actually inserted.
	//
	// If the row count isn't exactly 1, our insert was ignored.
	//
	// Note that MySQL will report 2 affected rows if a single row is actually
	// changed by ON DUPLICATE KEY UPDATE, though in this case we expect the
	// failure case to return a 0 because our ON DUPLICATE KEY UPDATE clause
	// doesn't actually cause any changes.
	n, err := res.RowsAffected()
	return n == 1, err
}

// UpdateQueueMessage updates meta-data about a message that is already on
// the queue.
//
// It returns false if the row does not exists or i.Revision is not current.
func (driver) UpdateQueueMessage(
	ctx context.Context,
	tx *sql.Tx,
	ak string,
	i *queuestore.Item,
) (_ bool, err error) {
	defer sqlx.Recover(&err)

	return sqlx.TryExecRow(
		ctx,
		tx,
		`UPDATE queue SET
			revision = revision + 1,
			failure_count = ?,
			next_attempt_at = ?
		WHERE app_key = ?
		AND message_id = ?
		AND revision = ?`,
		i.FailureCount,
		i.NextAttemptAt,
		ak,
		i.Envelope.GetMetaData().GetMessageId(),
		i.Revision,
	), nil
}

// DeleteQueueMessage deletes a message from the queue.
//
// It returns false if the row does not exists or i.Revision is not current.
func (driver) DeleteQueueMessage(
	ctx context.Context,
	tx *sql.Tx,
	ak string,
	i *queuestore.Item,
) (_ bool, err error) {
	defer sqlx.Recover(&err)

	return sqlx.TryExecRow(
		ctx,
		tx,
		`DELETE FROM queue
		WHERE app_key = ?
		AND message_id = ?
		AND revision = ?`,
		ak,
		i.Envelope.GetMetaData().GetMessageId(),
		i.Revision,
	), nil
}

// SelectQueueMessages selects up to n messages from the queue.
func (driver) SelectQueueMessages(
	ctx context.Context,
	db *sql.DB,
	ak string,
	n int,
) (*sql.Rows, error) {
	return db.QueryContext(
		ctx,
		`SELECT
			q.revision,
			q.failure_count,
			q.next_attempt_at,
			q.message_id,
			q.causation_id,
			q.correlation_id,
			q.source_app_name,
			q.source_app_key,
			q.source_handler_name,
			q.source_handler_key,
			q.source_instance_id,
			q.created_at,
			q.scheduled_for,
			q.description,
			q.portable_name,
			q.media_type,
			q.data
		FROM queue AS q
		WHERE q.app_key = ?
		ORDER BY q.next_attempt_at
		LIMIT ?`,
		ak,
		n,
	)
}

// ScanQueueMessage scans the next message from a row-set returned by
// SelectQueueMessages().
func (driver) ScanQueueMessage(
	rows *sql.Rows,
	i *queuestore.Item,
) error {
	var next string

	err := rows.Scan(
		&i.Revision,
		&i.FailureCount,
		&next,
		&i.Envelope.MetaData.MessageId,
		&i.Envelope.MetaData.CausationId,
		&i.Envelope.MetaData.CorrelationId,
		&i.Envelope.MetaData.Source.Application.Name,
		&i.Envelope.MetaData.Source.Application.Key,
		&i.Envelope.MetaData.Source.Handler.Name,
		&i.Envelope.MetaData.Source.Handler.Key,
		&i.Envelope.MetaData.Source.InstanceId,
		&i.Envelope.MetaData.CreatedAt,
		&i.Envelope.MetaData.ScheduledFor,
		&i.Envelope.MetaData.Description,
		&i.Envelope.PortableName,
		&i.Envelope.MediaType,
		&i.Envelope.Data,
	)
	if err != nil {
		return err
	}

	i.NextAttemptAt, err = time.Parse(timeLayout, next)

	return err
}
