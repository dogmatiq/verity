package mysql

import (
	"context"
	"database/sql"

	"github.com/dogmatiq/infix/draftspecs/envelopespec"
	"github.com/dogmatiq/infix/internal/x/sqlx"
	"github.com/dogmatiq/infix/persistence/provider/sql/internal/query"
	"github.com/dogmatiq/infix/persistence/subsystem/eventstore"
)

// UpdateNextOffset increments the eventstore offset by 1 and returns the new
// value.
func (driver) UpdateNextOffset(
	ctx context.Context,
	tx *sql.Tx,
	ak string,
) (_ uint64, err error) {
	defer sqlx.Recover(&err)

	sqlx.Exec(
		ctx,
		tx,
		`INSERT INTO event_offset SET
			source_app_key = ?
		ON DUPLICATE KEY UPDATE
			next_offset = next_offset + 1`,
		ak,
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

	return uint64(next), nil
}

// InsertEvent saves an event to the eventstore at a specific offset.
func (driver) InsertEvent(
	ctx context.Context,
	tx *sql.Tx,
	o uint64,
	env *envelopespec.Envelope,
) error {
	_, err := tx.ExecContext(
		ctx,
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
				description = ?,
				portable_name = ?,
				media_type = ?,
				data = ?`,
		o,
		env.GetMetaData().GetMessageId(),
		env.GetMetaData().GetCausationId(),
		env.GetMetaData().GetCorrelationId(),
		env.GetMetaData().GetSource().GetApplication().GetName(),
		env.GetMetaData().GetSource().GetApplication().GetKey(),
		env.GetMetaData().GetSource().GetHandler().GetName(),
		env.GetMetaData().GetSource().GetHandler().GetKey(),
		env.GetMetaData().GetSource().GetInstanceId(),
		env.GetMetaData().GetCreatedAt(),
		env.GetMetaData().GetDescription(),
		env.GetPortableName(),
		env.GetMediaType(),
		env.GetData(),
	)

	return err
}

// InsertEventFilter inserts a filter that limits selected events to those with
// a portable name in the given set.
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

// SelectNextEventOffset selects the next "unused" offset from the event store.
func (driver) SelectNextEventOffset(
	ctx context.Context,
	db *sql.DB,
	ak string,
) (uint64, error) {
	row := db.QueryRowContext(
		ctx,
		`SELECT
			next_offset
		FROM event_offset`,
	)

	var next uint64

	err := row.Scan(&next)
	if err == sql.ErrNoRows {
		err = nil
	}

	return next, err
}

// SelectEventsByType selects events from the eventstore that match the
// given event types query.
//
// f is a filter ID, as returned by InsertEventFilter(). If the query does
// not use a filter, f is zero.
func (driver) SelectEventsByType(
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
			e.description,
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

// SelectEventsBySource selects events from the eventstore that match the
// given source, namely the source's key and id.
func (driver) SelectEventsBySource(
	ctx context.Context,
	db *sql.DB,
	ak, hk, id string,
	o uint64,
) (*sql.Rows, error) {
	return db.QueryContext(
		ctx,
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
			e.description,
			e.portable_name,
			e.media_type,
			e.data
		FROM event AS e
		WHERE e.source_app_key = ?
		AND e.source_handler_key = ?
		AND e.source_instance_id = ?
		AND e.offset >= ?
		ORDER BY e.offset`,
		ak,
		hk,
		id,
		o,
	)
}

// SelectOffsetByMessageID selects the offset of the message with the given
// ID.
func (driver) SelectOffsetByMessageID(
	ctx context.Context,
	db *sql.DB,
	id string,
) (o uint64, err error) {
	row := db.QueryRowContext(
		ctx,
		`SELECT
			e.offset
		FROM event AS e
		WHERE e.message_id = ?`,
		id,
	)

	if err = row.Scan(&o); err == sql.ErrNoRows {
		err = eventstore.UnknownMessageError{
			MessageID: id,
		}
	}

	return
}

// ScanEvent scans the next event from a row-set returned by SelectEvents().
func (driver) ScanEvent(
	rows *sql.Rows,
	i *eventstore.Item,
) error {
	return rows.Scan(
		&i.Offset,
		&i.Envelope.MetaData.MessageId,
		&i.Envelope.MetaData.CausationId,
		&i.Envelope.MetaData.CorrelationId,
		&i.Envelope.MetaData.Source.Application.Name,
		&i.Envelope.MetaData.Source.Application.Key,
		&i.Envelope.MetaData.Source.Handler.Name,
		&i.Envelope.MetaData.Source.Handler.Key,
		&i.Envelope.MetaData.Source.InstanceId,
		&i.Envelope.MetaData.CreatedAt,
		&i.Envelope.MetaData.Description,
		&i.Envelope.PortableName,
		&i.Envelope.MediaType,
		&i.Envelope.Data,
	)
}
