package mysql

import (
	"context"
	"database/sql"

	"github.com/dogmatiq/infix/draftspecs/envelopespec"
	"github.com/dogmatiq/infix/internal/x/sqlx"
	"github.com/dogmatiq/infix/persistence/eventstore"
)

// UpdateNextOffset increments the eventstore offset by n and returns the
// new value.
func (driver) UpdateNextOffset(
	ctx context.Context,
	tx *sql.Tx,
	ak string,
	n eventstore.Offset,
) (_ eventstore.Offset, err error) {
	defer sqlx.Recover(&err)

	sqlx.Exec(
		ctx,
		tx,
		`INSERT INTO event_offset SET
			source_app_key = ?,
			next_offset = ?
		ON DUPLICATE KEY UPDATE
			next_offset = next_offset + VALUE(next_offset)`,
		ak,
		n,
	)

	next := sqlx.QueryInt64(
		ctx,
		tx,
		`SELECT
			next_offset
		FROM event_offset
		WHERE source_app_key = ?`,
		ak,
	)

	return eventstore.Offset(next), nil
}

// InsertEvents saves events to the eventstore, starting at a specific offset.
func (driver) InsertEvents(
	ctx context.Context,
	tx *sql.Tx,
	o eventstore.Offset,
	envelopes []*envelopespec.Envelope,
) (err error) {
	defer sqlx.Recover(&err)

	for _, env := range envelopes {
		sqlx.Exec(
			ctx,
			tx,
			`INSERT INTO event SET
				offset = ?,
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
				data = ?`,
			o,
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

		o++
	}

	return nil
}

// SelectEvents selects events from the eventstore that match the given query.
func (driver) SelectEvents(
	ctx context.Context,
	db *sql.DB,
	ak string,
	min eventstore.Offset,
) (*sql.Rows, error) {
	return db.QueryContext(
		ctx,
		`SELECT
			offset,
			message_id,
			causation_id,
			correlation_id,
			source_app_name,
			source_app_key,
			source_handler_name,
			source_handler_key,
			source_instance_id,
			created_at,
			portable_name,
			media_type,
			data
		FROM event AS e
		WHERE e.source_app_key = ?
		AND e.offset >= ?
		ORDER BY e.offset`,
		ak,
		min,
	)
}

// ScanEvent scans the next event from a row-set returned by SelectEvents().
func (driver) ScanEvent(
	rows *sql.Rows,
	ev *eventstore.Event,
) error {
	return rows.Scan(
		&ev.Offset,
		&ev.Envelope.MetaData.MessageId,
		&ev.Envelope.MetaData.CausationId,
		&ev.Envelope.MetaData.CorrelationId,
		&ev.Envelope.MetaData.Source.Application.Name,
		&ev.Envelope.MetaData.Source.Application.Key,
		&ev.Envelope.MetaData.Source.Handler.Name,
		&ev.Envelope.MetaData.Source.Handler.Key,
		&ev.Envelope.MetaData.Source.InstanceId,
		&ev.Envelope.MetaData.CreatedAt,
		&ev.Envelope.PortableName,
		&ev.Envelope.MediaType,
		&ev.Envelope.Data,
	)
}
