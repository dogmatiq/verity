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
	m *queuestore.Message,
) (_ bool, err error) {

	defer sqlx.Recover(&err)

	res := sqlx.Exec(
		ctx,
		tx,
		`INSERT INTO infix.queue (
				app_key,
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
				portable_name,
				media_type,
				data
			) VALUES (
				$1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15
			) ON CONFLICT (app_key, message_id) DO NOTHING`,
		ak,
		m.NextAttemptAt,
		m.Envelope.GetMetaData().GetMessageId(),
		m.Envelope.GetMetaData().GetCausationId(),
		m.Envelope.GetMetaData().GetCorrelationId(),
		m.Envelope.GetMetaData().GetSource().GetApplication().GetName(),
		m.Envelope.GetMetaData().GetSource().GetApplication().GetKey(),
		m.Envelope.GetMetaData().GetSource().GetHandler().GetName(),
		m.Envelope.GetMetaData().GetSource().GetHandler().GetKey(),
		m.Envelope.GetMetaData().GetSource().GetInstanceId(),
		m.Envelope.GetMetaData().GetCreatedAt(),
		m.Envelope.GetMetaData().GetScheduledFor(),
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
// It returns false if the row does not exists or m.Revision is not current.
func (driver) UpdateQueueMessage(
	ctx context.Context,
	tx *sql.Tx,
	ak string,
	m *queuestore.Message,
) (_ bool, err error) {
	defer sqlx.Recover(&err)

	return sqlx.TryUpdateRow(
		ctx,
		tx,
		`UPDATE infix.queue SET
			revision = revision + 1,
			next_attempt_at = $1
		WHERE app_key = $2
		AND message_id = $3
		AND revision = $4`,
		m.NextAttemptAt,
		ak,
		m.Envelope.GetMetaData().GetMessageId(),
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
	m *queuestore.Message,
) error {
	return rows.Scan(
		&m.Revision,
		&m.NextAttemptAt,
		&m.Envelope.MetaData.MessageId,
		&m.Envelope.MetaData.CausationId,
		&m.Envelope.MetaData.CorrelationId,
		&m.Envelope.MetaData.Source.Application.Name,
		&m.Envelope.MetaData.Source.Application.Key,
		&m.Envelope.MetaData.Source.Handler.Name,
		&m.Envelope.MetaData.Source.Handler.Key,
		&m.Envelope.MetaData.Source.InstanceId,
		&m.Envelope.MetaData.CreatedAt,
		&m.Envelope.MetaData.ScheduledFor,
		&m.Envelope.PortableName,
		&m.Envelope.MediaType,
		&m.Envelope.Data,
	)
}
