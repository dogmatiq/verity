package mysql

import (
	"context"
	"database/sql"
	"time"

	"github.com/dogmatiq/infix/draftspecs/envelopespec"
	"github.com/dogmatiq/infix/internal/x/sqlx"
	"github.com/dogmatiq/infix/persistence/subsystem/queue"
)

// InsertQueuedMessages saves messages to the queue.
func (driver) InsertQueuedMessages(
	ctx context.Context,
	tx *sql.Tx,
	ak string,
	envelopes []*envelopespec.Envelope,
) (err error) {
	defer sqlx.Recover(&err)

	for _, env := range envelopes {
		data := env.MetaData.ScheduledFor
		if data == "" {
			data = env.MetaData.CreatedAt
		}

		next, err := time.Parse(time.RFC3339Nano, data)
		if err != nil {
			return err
		}

		// Note: ON DUPLICATE KEY UPDATE is used because INSERT IGNORE ignores
		// more than just key conflicts.
		sqlx.Exec(
			ctx,
			tx,
			`INSERT INTO queue SET
				app_key = ?,
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
				portable_name = ?,
				media_type = ?,
				data = ?
			ON DUPLICATE KEY UPDATE
				app_key = VALUES(app_key)`,
			ak,
			next,
			env.MetaData.MessageId,
			env.MetaData.CausationId,
			env.MetaData.CorrelationId,
			env.MetaData.Source.Application.Name,
			env.MetaData.Source.Application.Key,
			env.MetaData.Source.Handler.Name,
			env.MetaData.Source.Handler.Key,
			env.MetaData.Source.InstanceId,
			env.MetaData.CreatedAt,
			env.PortableName,
			env.MediaType,
			env.Data,
		)
	}

	return nil
}

// SelectQueuedMessages selects up to n messages from the queue.
func (driver) SelectQueuedMessages(
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
			q.portable_name,
			q.media_type,
			q.data
		FROM queue AS q
		WHERE q.app_key = ?
		LIMIT ?`,
		ak,
		n,
	)
}

// ScanQueuedMessage scans the next message from a row-set returned by
// SelectQueuedMessages().
func (driver) ScanQueuedMessage(
	rows *sql.Rows,
	m *queue.Message,
) error {
	var next string

	err := rows.Scan(
		&m.Revision,
		&next,
		&m.Envelope.MetaData.MessageId,
		&m.Envelope.MetaData.CausationId,
		&m.Envelope.MetaData.CorrelationId,
		&m.Envelope.MetaData.Source.Application.Name,
		&m.Envelope.MetaData.Source.Application.Key,
		&m.Envelope.MetaData.Source.Handler.Name,
		&m.Envelope.MetaData.Source.Handler.Key,
		&m.Envelope.MetaData.Source.InstanceId,
		&m.Envelope.MetaData.CreatedAt,
		&m.Envelope.PortableName,
		&m.Envelope.MediaType,
		&m.Envelope.Data,
	)
	if err != nil {
		return err
	}

	m.NextAttemptAt, err = time.Parse(timeLayout, next)

	return nil
}