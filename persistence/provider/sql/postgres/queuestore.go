package postgres

import (
	"context"
	"database/sql"

	"github.com/dogmatiq/infix/internal/x/sqlx"
	"github.com/dogmatiq/infix/persistence"
)

// InsertQueueMessage inserts a message in the queue.
//
// It returns false if the row already exists.
func (driver) InsertQueueMessage(
	ctx context.Context,
	tx *sql.Tx,
	ak string,
	m persistence.QueueMessage,
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
		m.FailureCount,
		m.NextAttemptAt,
		m.Envelope.GetMessageId(),
		m.Envelope.GetCausationId(),
		m.Envelope.GetCorrelationId(),
		m.Envelope.GetSourceApplication().GetName(),
		m.Envelope.GetSourceApplication().GetKey(),
		m.Envelope.GetSourceHandler().GetName(),
		m.Envelope.GetSourceHandler().GetKey(),
		m.Envelope.GetSourceInstanceId(),
		m.Envelope.GetCreatedAt(),
		m.Envelope.GetScheduledFor(),
		m.Envelope.GetDescription(),
		m.Envelope.GetPortableName(),
		m.Envelope.GetMediaType(),
		m.Envelope.GetData(),
	)

	n, err := res.RowsAffected()
	return n == 1, err
}

// UpdateQueueMessage updates meta-data about a message that is already on
// the queue.
//
// It returns false if the row does not exist or m.Revision is not current.
func (driver) UpdateQueueMessage(
	ctx context.Context,
	tx *sql.Tx,
	ak string,
	m persistence.QueueMessage,
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
		m.FailureCount,
		m.NextAttemptAt,
		ak,
		m.Envelope.GetMessageId(),
		m.Revision,
	), nil
}

// DeleteQueueMessage deletes a message from the queue.
//
// It returns false if the row does not exist or m.Revision is not current.
func (driver) DeleteQueueMessage(
	ctx context.Context,
	tx *sql.Tx,
	ak string,
	m persistence.QueueMessage,
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
		m.Envelope.GetMessageId(),
		m.Revision,
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
	m *persistence.QueueMessage,
) error {
	return rows.Scan(
		&m.Revision,
		&m.FailureCount,
		&m.NextAttemptAt,
		&m.Envelope.MessageId,
		&m.Envelope.CausationId,
		&m.Envelope.CorrelationId,
		&m.Envelope.SourceApplication.Name,
		&m.Envelope.SourceApplication.Key,
		&m.Envelope.SourceHandler.Name,
		&m.Envelope.SourceHandler.Key,
		&m.Envelope.SourceInstanceId,
		&m.Envelope.CreatedAt,
		&m.Envelope.ScheduledFor,
		&m.Envelope.Description,
		&m.Envelope.PortableName,
		&m.Envelope.MediaType,
		&m.Envelope.Data,
	)
}
