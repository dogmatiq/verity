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
	p *queuestore.Parcel,
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
		p.FailureCount,
		p.NextAttemptAt,
		p.Envelope.GetMetaData().GetMessageId(),
		p.Envelope.GetMetaData().GetCausationId(),
		p.Envelope.GetMetaData().GetCorrelationId(),
		p.Envelope.GetMetaData().GetSource().GetApplication().GetName(),
		p.Envelope.GetMetaData().GetSource().GetApplication().GetKey(),
		p.Envelope.GetMetaData().GetSource().GetHandler().GetName(),
		p.Envelope.GetMetaData().GetSource().GetHandler().GetKey(),
		p.Envelope.GetMetaData().GetSource().GetInstanceId(),
		p.Envelope.GetMetaData().GetCreatedAt(),
		p.Envelope.GetMetaData().GetScheduledFor(),
		p.Envelope.GetMetaData().GetDescription(),
		p.Envelope.GetPortableName(),
		p.Envelope.GetMediaType(),
		p.Envelope.GetData(),
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
// It returns false if the row does not exists or p.Revision is not current.
func (driver) UpdateQueueMessage(
	ctx context.Context,
	tx *sql.Tx,
	ak string,
	p *queuestore.Parcel,
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
		p.FailureCount,
		p.NextAttemptAt,
		ak,
		p.Envelope.GetMetaData().GetMessageId(),
		p.Revision,
	), nil
}

// DeleteQueueMessage deletes a message from the queue.
//
// It returns false if the row does not exists or p.Revision is not current.
func (driver) DeleteQueueMessage(
	ctx context.Context,
	tx *sql.Tx,
	ak string,
	p *queuestore.Parcel,
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
		p.Envelope.GetMetaData().GetMessageId(),
		p.Revision,
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
	p *queuestore.Parcel,
) error {
	var next string

	err := rows.Scan(
		&p.Revision,
		&p.FailureCount,
		&next,
		&p.Envelope.MetaData.MessageId,
		&p.Envelope.MetaData.CausationId,
		&p.Envelope.MetaData.CorrelationId,
		&p.Envelope.MetaData.Source.Application.Name,
		&p.Envelope.MetaData.Source.Application.Key,
		&p.Envelope.MetaData.Source.Handler.Name,
		&p.Envelope.MetaData.Source.Handler.Key,
		&p.Envelope.MetaData.Source.InstanceId,
		&p.Envelope.MetaData.CreatedAt,
		&p.Envelope.MetaData.ScheduledFor,
		&p.Envelope.MetaData.Description,
		&p.Envelope.PortableName,
		&p.Envelope.MediaType,
		&p.Envelope.Data,
	)
	if err != nil {
		return err
	}

	p.NextAttemptAt, err = time.Parse(timeLayout, next)

	return err
}
