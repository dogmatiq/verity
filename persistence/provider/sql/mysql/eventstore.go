package mysql

import (
	"context"
	"database/sql"

	"github.com/dogmatiq/infix/draftspecs/envelopespec"
	"github.com/dogmatiq/infix/internal/x/sqlx"
	"github.com/dogmatiq/infix/persistence/provider/sql/internal/query"
	"github.com/dogmatiq/infix/persistence/subsystem/eventstore"
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

// InsertEventFilter inserts a filter that limits selected events to those
// with a portable name in the given set.
//
// It returns the filter's ID.
func (driver) InsertEventFilter(
	ctx context.Context,
	db *sql.DB,
	ak string,
	f eventstore.Filter,
) (_ int64, err error) {
	defer sqlx.Recover(&err)

	tx := sqlx.Begin(ctx, db)
	defer tx.Rollback()

	id := sqlx.Insert(
		ctx,
		tx,
		`INSERT INTO event_filter SET
			app_key = ?`,
		ak,
	)

	for n := range f {
		sqlx.Exec(
			ctx,
			tx,
			`INSERT INTO event_filter_name SET
				filter_id = ?,
				portable_name = ?`,
			id,
			n,
		)
	}

	return id, tx.Commit()
}

// DeleteEventFilter deletes an event filter.
//
// f is the filter ID, as returned by InsertEventFilter().
func (driver) DeleteEventFilter(
	ctx context.Context,
	db *sql.DB,
	f int64,
) error {
	_, err := db.ExecContext(
		ctx,
		`DELETE FROM event_filter
		WHERE id = ?`,
		f,
	)
	return err
}

// PurgeEventFilters deletes all event filters for the given application.
func (driver) PurgeEventFilters(
	ctx context.Context,
	db *sql.DB,
	ak string,
) error {
	_, err := db.ExecContext(
		ctx,
		`DELETE FROM event_filter
		WHERE app_key = ?`,
		ak,
	)
	return err
}

// SelectEvents selects events from the eventstore that match the given
// query.
//
// f is a filter ID, as returned by InsertEventFilter(). If the query does
// not use a filter, f is zero.
func (driver) SelectEvents(
	ctx context.Context,
	db *sql.DB,
	ak string,
	q eventstore.Query,
	f int64,
) (*sql.Rows, error) {
	qb := query.Builder{}

	qb.Write(
		`SELECT
			e.offset,
			e.message_id,
			e.causation_id,
			e.correlation_id,
			e.source_app_name,
			e.source_app_key,
			e.source_handler_name,
			e.source_handler_key,
			e.source_instance_id,
			e.created_at,
			e.portable_name,
			e.media_type,
			e.data
		FROM event AS e`,
	)

	if f != 0 {
		qb.Write(
			`INNER JOIN event_filter_name AS ft
			ON ft.filter_id = ?
			AND ft.portable_name = e.portable_name`,
			f,
		)
	}

	qb.Write(
		`WHERE e.source_app_key = ?
		AND e.offset >= ?`,
		ak,
		q.MinOffset,
	)

	if q.AggregateHandlerKey != "" {
		qb.Write(
			`AND e.source_handler_key = ?
			AND e.source_instance_id = ?`,
			q.AggregateHandlerKey,
			q.AggregateInstanceID,
		)
	}

	qb.Write(
		`ORDER BY e.offset`,
	)

	return db.QueryContext(
		ctx,
		qb.String(),
		qb.Parameters...,
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
