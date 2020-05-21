package mysql

import (
	"context"
	"database/sql"

	"github.com/dogmatiq/infix/draftspecs/envelopespec"
	"github.com/dogmatiq/infix/internal/x/sqlx"
	"github.com/dogmatiq/infix/persistence"
)

// UpdateNextOffset increments the next offset by one and returns the new value.
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

// InsertEvent saves an event at a specific offset.
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
	f map[string]struct{},
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

// SelectNextEventOffset selects the next "unused" offset.
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

// SelectEventsByType selects events that match the given type filter.
//
// f is a filter ID, as returned by InsertEventFilter(). o is the minimum offset
// to include in the results.
func (driver) SelectEventsByType(
	ctx context.Context,
	db *sql.DB,
	ak string,
	f int64,
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
		INNER JOIN event_filter_name AS ft
		ON ft.portable_name = e.portable_name
		WHERE e.source_app_key = ?
		AND e.offset >= ?
		AND ft.filter_id = ?
		ORDER BY e.offset`,
		ak,
		o,
		f,
	)
}

// SelectEventsBySource selects events that were produced by a specific handler.
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
// ID. It returns false as a second return value if the message cannot be
// found.
func (driver) SelectOffsetByMessageID(
	ctx context.Context,
	db *sql.DB,
	id string,
) (uint64, bool, error) {
	row := db.QueryRowContext(
		ctx,
		`SELECT
		e.offset
		FROM event AS e
		WHERE e.message_id = ?`,
		id,
	)

	var o uint64
	err := row.Scan(&o)
	if err == sql.ErrNoRows {
		return 0, false, nil
	}

	return o, true, err
}

// ScanEvent scans the next event from a row-set returned by SelectEvents().
func (driver) ScanEvent(
	rows *sql.Rows,
	ev *persistence.Event,
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
		&ev.Envelope.MetaData.Description,
		&ev.Envelope.PortableName,
		&ev.Envelope.MediaType,
		&ev.Envelope.Data,
	)
}
