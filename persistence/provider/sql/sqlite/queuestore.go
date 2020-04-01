package sqlite

import (
	"context"
	"database/sql"
	"time"

	"github.com/dogmatiq/infix/draftspecs/envelopespec"
	"github.com/dogmatiq/infix/internal/x/sqlx"
	"github.com/dogmatiq/infix/persistence/subsystem/queuestore"
)

// InsertQueueMessage saves a messages to the queue.
func (driver) InsertQueueMessage(
	ctx context.Context,
	tx *sql.Tx,
	ak string,
	env *envelopespec.Envelope,
	n time.Time,
) (err error) {
	defer sqlx.Recover(&err)

	sqlx.Exec(
		ctx,
		tx,
		`INSERT INTO queue (
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
		n,
		env.GetMetaData().GetMessageId(),
		env.GetMetaData().GetCausationId(),
		env.GetMetaData().GetCorrelationId(),
		env.GetMetaData().GetSource().GetApplication().GetName(),
		env.GetMetaData().GetSource().GetApplication().GetKey(),
		env.GetMetaData().GetSource().GetHandler().GetName(),
		env.GetMetaData().GetSource().GetHandler().GetKey(),
		env.GetMetaData().GetSource().GetInstanceId(),
		env.GetMetaData().GetCreatedAt(),
		env.GetMetaData().GetScheduledFor(),
		env.GetPortableName(),
		env.GetMediaType(),
		env.GetData(),
	)

	return nil
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
		FROM queue AS q
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
