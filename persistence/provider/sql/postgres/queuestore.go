package postgres

import (
	"context"
	"database/sql"

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

	res := sqlx.Exec(
		ctx,
		tx,
		`INSERT INTO infix.queue (
				app_key,
				failure_count,
				next_attempt_at,
				message_id,
				causation_id,
				correlation_id,
				source_app_name,
				source_app_key,
				source_handler_name,
				source_handler_key,
				source_instance_id,
				created_at,
				scheduled_for,
				description,
				portable_name,
				media_type,
				data
			) VALUES (
				$1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17
			) ON CONFLICT (app_key, message_id) DO NOTHING`,
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

	n, err := res.RowsAffected()
	return n == 1, err
}

// UpdateQueueMessage updates meta-data about a message that is already on
// the queue.
//
// It returns false if the row does not exist or i.Revision is not current.
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
		`UPDATE infix.queue SET
			revision = revision + 1,
			failure_count = $1,
			next_attempt_at = $2
		WHERE app_key = $3
		AND message_id = $4
		AND revision = $5`,
		i.FailureCount,
		i.NextAttemptAt,
		ak,
		i.Envelope.GetMetaData().GetMessageId(),
		i.Revision,
	), nil
}

// DeleteQueueMessage deletes a message from the queue.
//
// It returns false if the row does not exist or i.Revision is not current.
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
		`DELETE FROM infix.queue
		WHERE app_key = $1
		AND message_id = $2
		AND revision = $3`,
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
		FROM infix.queue AS q
		WHERE q.app_key = $1
		ORDER BY q.next_attempt_at
		LIMIT $2`,
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
	return rows.Scan(
		&i.Revision,
		&i.FailureCount,
		&i.NextAttemptAt,
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
}
