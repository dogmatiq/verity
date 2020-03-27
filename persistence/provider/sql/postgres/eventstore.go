package postgres

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

	o := sqlx.QueryInt64(
		ctx,
		tx,
		`INSERT INTO infix.event_offset AS o (
			source_app_key,
			next_offset
		) VALUES (
			$1, $2
		) ON CONFLICT (source_app_key) DO UPDATE SET
			next_offset = o.next_offset + excluded.next_offset
		RETURNING next_offset`,
		ak,
		n,
	)

	return eventstore.Offset(o), nil
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
			`INSERT INTO infix.event (
				"offset",
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
			) VALUES (
				$1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13
			)`,
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
			"offset",
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
		FROM infix.event AS e
		WHERE e.source_app_key = $1
		AND e.offset >= $2
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
